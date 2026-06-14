package object

// Storage is the node-level shared component of the object backend — the mq analogue of
// AutoMQ's S3Stream node storage. One Storage per broker owns the single per-node WAL, the
// in-memory pending buffer (data written but not yet uploaded), the object index, and the
// uploader. Each partition's *ObjectLog is a thin handle over this shared Storage: it keeps
// its own offset state but delegates durability and upload here. Batching many partitions into
// one upload is exactly what lets a node write few, large objects instead of one per partition.

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"mq/internal/record"
)

// StorageConfig tunes the node storage: where the WAL lives, how objects are named, and when
// the uploader flushes the pending buffer to object storage.
type StorageConfig struct {
	NodeID      string // namespaces this node's objects/WAL (objects: data/sso/<NodeID>/<seq>)
	WALDir      string // local directory for the per-node WAL segments
	UploadBytes int64  // flush pending to an object once it reaches this many bytes (0 = no size trigger)
	UploadMS    int64  // ... or this many milliseconds since the last flush (<=0 => 250ms)
}

// pendingPart is one partition's not-yet-uploaded batches, buffered in offset order.
type pendingPart struct {
	topic      string
	partition  int32
	baseOffset int64
	nextOffset int64
	batches    [][]byte
	bytes      int64
}

// Storage owns the WAL, pending buffer, index, and uploader for one node.
type Storage struct {
	store ObjectStore
	wal   *wal
	index *Index
	cfg   StorageConfig

	mu           sync.Mutex
	pending      map[string]*pendingPart
	pendingBytes int64
	uploadSeq    uint64
	recBase      map[string]int64 // earliest offset recovered from the WAL, per partition
	recNext      map[string]int64 // LEO recovered from the WAL, per partition

	uploadMu sync.Mutex // serializes upload() so the ticker and a forced flush never overlap
	notify   chan struct{}
	closed   chan struct{}
	wg       sync.WaitGroup
}

// NewStorage opens the node storage: it brings up the WAL, replays any un-uploaded records
// left by a previous run back into the pending buffer (so the next flush re-uploads them), and
// starts the background uploader.
func NewStorage(store ObjectStore, idx IndexStore, cfg StorageConfig) (*Storage, error) {
	w, err := openWAL(cfg.WALDir)
	if err != nil {
		return nil, err
	}
	s := &Storage{
		store:   store,
		wal:     w,
		index:   newIndex(idx),
		cfg:     cfg,
		pending: map[string]*pendingPart{},
		recBase: map[string]int64{},
		recNext: map[string]int64{},
		notify:  make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
	if err := s.replay(); err != nil {
		return nil, err
	}
	s.wg.Add(1)
	go s.run()
	return s, nil
}

// replay reloads un-uploaded WAL records into the pending buffer and records the recovered
// offset range per partition (used to seed each ObjectLog's LEO/earliest on open).
func (s *Storage) replay() error {
	return s.wal.replay(func(rec walRecord) error {
		h, err := record.ParseHeader(rec.Batch)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.appendPendingLocked(rec, h.LastOffset()+1)
		k := indexKey(rec.Topic, rec.Partition)
		if _, ok := s.recBase[k]; !ok {
			s.recBase[k] = rec.BaseOffset
		}
		s.recNext[k] = h.LastOffset() + 1
		s.mu.Unlock()
		return nil
	})
}

// Log returns the per-partition Backend handle, seeding its offset state from the index
// (uploaded data) and the WAL replay (un-uploaded tail).
func (s *Storage) Log(topic string, partition int32) (*ObjectLog, error) {
	if err := s.index.load(topic, partition); err != nil {
		return nil, err
	}
	k := indexKey(topic, partition)

	next := int64(0)
	if l, ok := s.index.latest(topic, partition); ok {
		next = l
	}
	s.mu.Lock()
	if r, ok := s.recNext[k]; ok && r > next {
		next = r
	}
	earliest := next
	if e, ok := s.index.earliest(topic, partition); ok {
		earliest = e
	} else if b, ok := s.recBase[k]; ok {
		earliest = b
	}
	s.mu.Unlock()

	return &ObjectLog{
		storage:       s,
		topic:         topic,
		partition:     partition,
		nextOffset:    next,
		highWatermark: next,
		earliest:      earliest,
	}, nil
}

// append durably records one partition's batch: WAL first (the durability point), then the
// in-memory pending buffer. It triggers an upload if the pending buffer crosses the size
// threshold.
func (s *Storage) append(rec walRecord, next int64) error {
	if err := s.wal.append(rec); err != nil {
		return err
	}
	s.mu.Lock()
	s.appendPendingLocked(rec, next)
	full := s.cfg.UploadBytes > 0 && s.pendingBytes >= s.cfg.UploadBytes
	s.mu.Unlock()
	if full {
		s.triggerUpload()
	}
	return nil
}

func (s *Storage) appendPendingLocked(rec walRecord, next int64) {
	k := indexKey(rec.Topic, rec.Partition)
	pp := s.pending[k]
	if pp == nil {
		pp = &pendingPart{topic: rec.Topic, partition: rec.Partition, baseOffset: rec.BaseOffset}
		s.pending[k] = pp
	}
	pp.batches = append(pp.batches, rec.Batch)
	pp.nextOffset = next
	pp.bytes += int64(len(rec.Batch))
	s.pendingBytes += int64(len(rec.Batch))
}

// sync fsyncs the WAL — the durability point ObjectLog.Flush relies on.
func (s *Storage) sync() error { return s.wal.sync() }

// triggerUpload nudges the background uploader without blocking.
func (s *Storage) triggerUpload() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// run is the background uploader: it flushes the pending buffer on a timer or when nudged.
func (s *Storage) run() {
	defer s.wg.Done()
	interval := time.Duration(s.cfg.UploadMS) * time.Millisecond
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-s.notify:
		case <-t.C:
		}
		_ = s.upload()
	}
}

// upload flushes the entire pending buffer to one stream-set object (batching every partition),
// commits each partition's offset range to the index, and trims the WAL. It is a no-op when
// there is nothing pending.
func (s *Storage) upload() error {
	s.uploadMu.Lock()
	defer s.uploadMu.Unlock()

	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return nil
	}
	snap := s.pending
	s.pending = map[string]*pendingPart{}
	s.pendingBytes = 0
	s.uploadSeq++
	seq := s.uploadSeq
	s.mu.Unlock()

	// Seal the WAL: records written before now live in segments <= sealed, and become safe to
	// delete once this object is durable.
	sealed, err := s.wal.rotate()
	if err != nil {
		s.restore(snap)
		return err
	}

	key := fmt.Sprintf("data/sso/%s/%020d", s.cfg.NodeID, seq)
	uploadedMs := time.Now().UnixMilli()
	var buf []byte
	type commit struct {
		topic     string
		partition int32
		ref       SegmentRef
	}
	var commits []commit
	for _, k := range sortedKeys(snap) {
		pp := snap[k]
		pos := int64(len(buf))
		for _, b := range pp.batches {
			buf = append(buf, b...)
		}
		commits = append(commits, commit{pp.topic, pp.partition, SegmentRef{
			Key:        key,
			BaseOffset: pp.baseOffset,
			NextOffset: pp.nextOffset,
			Position:   pos,
			Length:     int64(len(buf)) - pos,
			UploadedMs: uploadedMs,
		}})
	}

	if err := s.store.Put(context.Background(), key, buf); err != nil {
		s.restore(snap)
		return err
	}
	for _, c := range commits {
		if err := s.index.commit(c.topic, c.partition, c.ref); err != nil {
			return err
		}
	}
	return s.wal.deleteThrough(sealed)
}

// restore merges a failed upload's snapshot back into the pending buffer (its offsets are older
// than anything appended since, so it goes in front), then recomputes the byte counter.
func (s *Storage) restore(snap map[string]*pendingPart) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, pp := range snap {
		if cur := s.pending[k]; cur != nil {
			pp.batches = append(pp.batches, cur.batches...)
			pp.nextOffset = cur.nextOffset
			pp.bytes += cur.bytes
		}
		s.pending[k] = pp
	}
	s.pendingBytes = 0
	for _, pp := range s.pending {
		s.pendingBytes += pp.bytes
	}
}

// Flush forces a synchronous upload of all pending data (used by tests and the flush ticker).
func (s *Storage) Flush() error { return s.upload() }

// Close stops the uploader, flushes anything pending, and closes the WAL.
func (s *Storage) Close() error {
	close(s.closed)
	s.wg.Wait()
	if err := s.upload(); err != nil {
		return err
	}
	return s.wal.close()
}

func sortedKeys(m map[string]*pendingPart) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
