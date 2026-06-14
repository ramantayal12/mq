// Package group implements the consumer-group coordinator (join/sync/heartbeat/
// leave) and the persisted committed-offset store. The single broker is always the
// coordinator, so group/member state lives in memory; only committed offsets are
// written to disk so restarted consumers can resume.
package group

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// offsetKey identifies a committed offset.
type offsetKey struct {
	group     string
	topic     string
	partition int32
}

type committed struct {
	offset   int64
	metadata string
}

// OffsetStore persists committed offsets under <dir>/__offsets/<group>/<topic>-<p>.
// Writes are last-write-wins and atomic (temp file + rename).
type OffsetStore struct {
	root  string // the __offsets directory
	mu    sync.RWMutex
	cache map[offsetKey]committed
}

// NewOffsetStore opens (creating if needed) the offsets directory under dataDir and
// loads existing commits into the cache.
func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	root := filepath.Join(dataDir, "__offsets")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	s := &OffsetStore{root: root, cache: map[offsetKey]committed{}}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Commit persists the offset for group/topic/partition.
func (s *OffsetStore) Commit(group, topic string, partition int32, offset int64, metadata string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Join(s.root, group)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, topic+"-"+strconv.Itoa(int(partition)))
	if err := writeAtomic(path, encodeCommit(offset, metadata)); err != nil {
		return err
	}
	s.cache[offsetKey{group, topic, partition}] = committed{offset, metadata}
	return nil
}

// Fetch returns the committed offset for group/topic/partition, if present.
func (s *OffsetStore) Fetch(group, topic string, partition int32) (offset int64, metadata string, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.cache[offsetKey{group, topic, partition}]
	if !ok {
		return -1, "", false
	}
	return c.offset, c.metadata, true
}

// CommittedOffset is a single committed position, returned by ListCommitted.
type CommittedOffset struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
}

// ListCommitted returns every committed offset currently in the store. Used by the
// metrics collector to compute per-group lag.
func (s *OffsetStore) ListCommitted() []CommittedOffset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]CommittedOffset, 0, len(s.cache))
	for k, v := range s.cache {
		out = append(out, CommittedOffset{k.group, k.topic, k.partition, v.offset})
	}
	return out
}

func encodeCommit(offset int64, metadata string) []byte {
	b := make([]byte, 12+len(metadata))
	binary.BigEndian.PutUint64(b[0:], uint64(offset))
	binary.BigEndian.PutUint32(b[8:], uint32(len(metadata)))
	copy(b[12:], metadata)
	return b
}

func decodeCommit(b []byte) (int64, string, bool) {
	if len(b) < 12 {
		return 0, "", false
	}
	offset := int64(binary.BigEndian.Uint64(b[0:]))
	mlen := int(binary.BigEndian.Uint32(b[8:]))
	if 12+mlen > len(b) {
		return offset, "", true
	}
	return offset, string(b[12 : 12+mlen]), true
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// load walks the __offsets tree into the cache.
func (s *OffsetStore) load() error {
	groups, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if !g.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(s.root, g.Name()))
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.IsDir() || strings.HasSuffix(f.Name(), ".tmp") {
				continue
			}
			i := strings.LastIndex(f.Name(), "-")
			if i < 0 {
				continue
			}
			topic := f.Name()[:i]
			p, err := strconv.Atoi(f.Name()[i+1:])
			if err != nil {
				continue
			}
			data, err := os.ReadFile(filepath.Join(s.root, g.Name(), f.Name()))
			if err != nil {
				return err
			}
			if off, meta, ok := decodeCommit(data); ok {
				s.cache[offsetKey{g.Name(), topic, int32(p)}] = committed{off, meta}
			}
		}
	}
	return nil
}
