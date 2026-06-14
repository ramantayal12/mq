package object

// Per-node write-ahead log for the object backend. Faithful to AutoMQ's Delta WAL: a single
// append-only log per node that *mixes records from all partitions*, used only for
// low-latency durability and crash recovery — the durable copy of data that has not yet been
// uploaded to object storage. Once a batch of records is uploaded into a stream-set object the
// WAL segment holding it is deleted (the WAL is trimmed).
//
// The WAL is a sequence of segment files `<dir>/<seq>.wal`. New records go to the active
// (highest-seq) segment; an upload seals the active segment, opens a fresh one, and — once the
// sealed records are durable in object storage — deletes every sealed segment.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const walMagic = 0xA7

var walCRC = crc32.MakeTable(crc32.Castagnoli)

// walRecord is one entry in the WAL: a single partition's record batch, tagged with the
// partition it belongs to so a single mixed WAL can be demultiplexed on replay.
type walRecord struct {
	Topic      string
	Partition  int32
	BaseOffset int64
	Batch      []byte
}

// wal is the per-node mixed write-ahead log. Safe for concurrent use.
type wal struct {
	dir       string
	mu        sync.Mutex
	active    *os.File
	activeSeq uint64
}

// openWAL opens (creating if needed) the WAL rooted at dir. Any pre-existing segments are
// left in place for the caller to replay(); a fresh active segment is opened for new appends
// so recovered (un-uploaded) segments and new writes stay separable until the next upload.
func openWAL(dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &wal{dir: dir}
	seqs, err := w.segmentSeqs()
	if err != nil {
		return nil, err
	}
	next := uint64(1)
	if len(seqs) > 0 {
		next = seqs[len(seqs)-1] + 1
	}
	if err := w.openActive(next); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *wal) openActive(seq uint64) error {
	f, err := os.OpenFile(w.segPath(seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.active = f
	w.activeSeq = seq
	return nil
}

// append writes rec to the active segment. The caller decides when to fsync (via sync()).
func (w *wal) append(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := encodeWALPayload(rec)
	frame := make([]byte, 1+4+4+len(payload))
	frame[0] = walMagic
	binary.BigEndian.PutUint32(frame[1:5], crc32.Checksum(payload, walCRC))
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(payload)))
	copy(frame[9:], payload)
	_, err := w.active.Write(frame)
	return err
}

// sync fsyncs the active segment (the durability point for acks).
func (w *wal) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active.Sync()
}

// rotate seals the current active segment and opens a fresh one, returning the sealed seq.
// Records written before rotate live in segments with seq <= the returned value.
func (w *wal) rotate() (sealedThrough uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.active.Sync(); err != nil {
		return 0, err
	}
	if err := w.active.Close(); err != nil {
		return 0, err
	}
	sealed := w.activeSeq
	if err := w.openActive(sealed + 1); err != nil {
		return 0, err
	}
	return sealed, nil
}

// deleteThrough removes every WAL segment with seq <= sealedThrough. Called after the records
// they hold are durable in object storage, trimming the WAL.
func (w *wal) deleteThrough(sealedThrough uint64) error {
	seqs, err := w.segmentSeqs()
	if err != nil {
		return err
	}
	for _, s := range seqs {
		if s > sealedThrough {
			continue
		}
		if err := os.Remove(w.segPath(s)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// replay reads every existing segment in seq order and calls fn for each intact record. A
// torn record at the tail of a segment (partial write from a crash) stops that segment's scan
// without error — everything before it is recovered.
func (w *wal) replay(fn func(walRecord) error) error {
	seqs, err := w.segmentSeqs()
	if err != nil {
		return err
	}
	for _, s := range seqs {
		data, err := os.ReadFile(w.segPath(s))
		if err != nil {
			return err
		}
		if err := replaySegment(data, fn); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active.Close()
}

// replaySegment decodes records from one segment's bytes, stopping cleanly at the first torn
// or corrupt record (the crash tail).
func replaySegment(data []byte, fn func(walRecord) error) error {
	for pos := 0; pos < len(data); {
		if data[pos] != walMagic {
			break
		}
		if pos+9 > len(data) {
			break // partial frame header
		}
		crc := binary.BigEndian.Uint32(data[pos+1 : pos+5])
		n := int(binary.BigEndian.Uint32(data[pos+5 : pos+9]))
		start := pos + 9
		if start+n > len(data) {
			break // partial payload
		}
		payload := data[start : start+n]
		if crc32.Checksum(payload, walCRC) != crc {
			break // corrupt record
		}
		rec, err := decodeWALPayload(payload)
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
		pos = start + n
	}
	return nil
}

// encodeWALPayload lays out: topicLen(2) | topic | partition(4) | baseOffset(8) | batch.
func encodeWALPayload(rec walRecord) []byte {
	buf := make([]byte, 2+len(rec.Topic)+4+8+len(rec.Batch))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(rec.Topic)))
	p := 2
	p += copy(buf[p:], rec.Topic)
	binary.BigEndian.PutUint32(buf[p:p+4], uint32(rec.Partition))
	p += 4
	binary.BigEndian.PutUint64(buf[p:p+8], uint64(rec.BaseOffset))
	p += 8
	copy(buf[p:], rec.Batch)
	return buf
}

func decodeWALPayload(b []byte) (walRecord, error) {
	if len(b) < 2 {
		return walRecord{}, fmt.Errorf("object: wal payload too short")
	}
	tl := int(binary.BigEndian.Uint16(b[0:2]))
	p := 2
	if p+tl+12 > len(b) {
		return walRecord{}, fmt.Errorf("object: wal payload truncated")
	}
	topic := string(b[p : p+tl])
	p += tl
	partition := int32(binary.BigEndian.Uint32(b[p : p+4]))
	p += 4
	base := int64(binary.BigEndian.Uint64(b[p : p+8]))
	p += 8
	batch := make([]byte, len(b)-p)
	copy(batch, b[p:])
	return walRecord{Topic: topic, Partition: partition, BaseOffset: base, Batch: batch}, nil
}

func (w *wal) segPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%020d.wal", seq))
}

// segmentSeqs returns the sorted seq numbers of *.wal files in the WAL dir.
func (w *wal) segmentSeqs() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		s, err := strconv.ParseUint(strings.TrimSuffix(name, ".wal"), 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}
