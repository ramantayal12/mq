// Package storage implements a Kafka-style segmented, append-only commit log, one
// instance per topic-partition. It knows nothing about the wire protocol; it stores
// and returns record-batch bytes verbatim, inspecting only the batch header (via
// internal/record) to assign offsets.
package storage

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mq/internal/record"
)

// ErrOffsetOutOfRange is returned by Read when the requested offset is below the
// earliest retained offset or strictly above the high watermark. A read exactly at
// the high watermark is not out of range — it returns no data (the caught-up case).
var ErrOffsetOutOfRange = errors.New("storage: offset out of range")

// Config controls a Log's segment rolling and indexing behavior.
type Config struct {
	SegmentBytes       int32 // roll to a new segment when the active log exceeds this
	IndexIntervalBytes int32 // add a sparse index entry roughly every this many bytes
}

// DefaultConfig returns sane defaults (64 MiB segments, 4 KiB index interval).
func DefaultConfig() Config {
	return Config{SegmentBytes: 64 << 20, IndexIntervalBytes: 4096}
}

// Log is the append-only commit log for a single partition. Safe for concurrent
// use: Append takes a write lock, Read takes a read lock.
//
// LEO vs. HWM. nextOffset is the log end offset (LEO) — the next offset Append will
// assign. highWatermark (HWM) is the offset up to which the data is committed (replicated
// to the in-sync replicas) and therefore readable by consumers. Without replication the
// two are identical: every append is immediately committed, so Append advances the HWM in
// lockstep with the LEO. With replication (holdHWM set), Append advances only the LEO; the
// leader advances the HWM as followers catch up (via SetHighWatermark), and a follower's
// HWM tracks the value the leader reports in fetch responses.
type Log struct {
	dir           string
	cfg           Config
	mu            sync.RWMutex
	segments      []*segment // sorted by baseOffset; last element is active
	active        *segment
	nextOffset    int64 // log end offset (LEO): next offset to assign
	highWatermark int64 // committed offset: consumers may read below it
	holdHWM       bool  // replication mode: Append advances LEO only, not HWM
	dirty         bool  // set on append, cleared on Flush (used by the flush ticker)
}

// Appender is the minimal write interface handlers depend on (DIP).
type Appender interface {
	Append(batch []byte) (baseOffset int64, err error)
}

// Reader is the minimal read interface handlers depend on (DIP).
type Reader interface {
	Read(offset int64, maxBytes int32) ([]byte, error)
	EarliestOffset() int64
	LatestOffset() int64
}

// Open opens (creating if needed) the partition log rooted at dir and recovers
// state from any existing segments.
func Open(dir string, cfg Config) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, cfg: cfg}
	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		seg, err := createSegment(dir, 0, cfg.IndexIntervalBytes)
		if err != nil {
			return nil, err
		}
		l.segments = []*segment{seg}
		l.active = seg
		l.nextOffset = 0
		l.highWatermark = 0
		return l, nil
	}
	for _, base := range bases {
		seg, err := openSegment(dir, base, cfg.IndexIntervalBytes)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
	}
	l.active = l.segments[len(l.segments)-1]
	next, err := recoverActive(l.active)
	if err != nil {
		return nil, err
	}
	l.nextOffset = next
	// Absent a persisted HWM checkpoint, treat the recovered log as fully committed on
	// open. For a replicated partition the leader/fetcher will re-establish forward
	// progress from here; for an RF=1 partition HWM == LEO is the steady state anyway.
	l.highWatermark = next
	return l, nil
}

// Append assigns the next offset to batch, patches its header + CRC, writes it to
// the active segment (rolling first if needed), and returns the assigned base offset.
func (l *Log) Append(batch []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	base := l.nextOffset
	record.SetBaseOffset(batch, base)
	record.RecomputeCRC(batch)

	if l.active.logSize > 0 && l.active.logSize+int32(len(batch)) > l.cfg.SegmentBytes {
		if err := l.roll(base); err != nil {
			return 0, err
		}
	}
	h, err := record.ParseHeader(batch)
	if err != nil {
		return 0, err
	}
	if err := l.active.append(batch, base); err != nil {
		return 0, err
	}
	l.nextOffset = base + int64(h.RecordCount)
	if !l.holdHWM {
		l.highWatermark = l.nextOffset // unreplicated: every append is immediately committed
	}
	l.dirty = true
	return base, nil
}

// AppendReplica appends a batch received from the partition leader, preserving the
// leader-assigned base offset (no re-assignment, no CRC recompute — the bytes are already
// authoritative). It is the follower's write path, used only by the replication fetcher.
// A batch already present (base below the LEO) is a harmless duplicate and is skipped; a
// gap (base above the LEO) is rejected so the fetcher can resync. It never advances the
// HWM: a follower's HWM is set from the leader's reported value via SetHighWatermark.
func (l *Log) AppendReplica(batch []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	h, err := record.ParseHeader(batch)
	if err != nil {
		return 0, err
	}
	if h.BaseOffset < l.nextOffset {
		return l.nextOffset, nil // already replicated this batch
	}
	if h.BaseOffset > l.nextOffset {
		return l.nextOffset, fmt.Errorf("storage: replica gap: batch base %d > log end %d", h.BaseOffset, l.nextOffset)
	}
	if l.active.logSize > 0 && l.active.logSize+int32(len(batch)) > l.cfg.SegmentBytes {
		if err := l.roll(h.BaseOffset); err != nil {
			return 0, err
		}
	}
	if err := l.active.append(batch, h.BaseOffset); err != nil {
		return 0, err
	}
	l.nextOffset = h.LastOffset() + 1
	l.dirty = true
	return l.nextOffset, nil
}

// HighWatermark returns the committed offset: consumers may read records below it.
func (l *Log) HighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.highWatermark
}

// SetHighWatermark advances the high watermark to hwm, clamped to never move backward
// and never exceed the LEO. Used by the leader as in-sync replicas catch up, and by a
// follower applying the leader-reported HWM from a fetch response.
func (l *Log) SetHighWatermark(hwm int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if hwm > l.nextOffset {
		hwm = l.nextOffset
	}
	if hwm > l.highWatermark {
		l.highWatermark = hwm
	}
}

// HoldHighWatermark switches the log into replication mode: subsequent appends advance
// only the LEO, leaving the HWM to be driven by SetHighWatermark. Called once when the
// broker opens a log for a partition with more than one replica. Idempotent.
func (l *Log) HoldHighWatermark() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holdHWM = true
}

// roll flushes the active segment and starts a new one based at baseOffset.
func (l *Log) roll(baseOffset int64) error {
	if err := l.active.flush(); err != nil {
		return err
	}
	seg, err := createSegment(l.dir, baseOffset, l.cfg.IndexIntervalBytes)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.active = seg
	return nil
}

// Read returns whole record batches starting at offset, up to maxBytes (always at
// least one batch if available). A read exactly at the high watermark returns nil with
// no error (the caught-up consumer should poll again). An offset below the earliest
// retained offset, or strictly above the high watermark, returns ErrOffsetOutOfRange.
func (l *Log) Read(offset int64, maxBytes int32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset == l.nextOffset {
		return nil, nil // caught up at the high watermark
	}
	if offset > l.nextOffset || offset < l.segments[0].baseOffset {
		return nil, ErrOffsetOutOfRange
	}
	seg := l.segmentFor(offset)
	return seg.readBatchesFrom(offset, maxBytes)
}

// segmentFor returns the segment whose range contains offset (largest baseOffset
// <= offset). Caller holds at least a read lock.
func (l *Log) segmentFor(offset int64) *segment {
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	})
	if i == 0 {
		return l.segments[0]
	}
	return l.segments[i-1]
}

// OffsetForTimestamp returns the base offset of the first batch whose maximum
// timestamp is >= ts. found is false when no record is that recent (the caller should
// report offset -1). It scans batch headers (61 bytes each), segment by segment in
// offset order; batches are appended in non-decreasing time, so the first qualifying
// batch is the earliest match.
func (l *Log) OffsetForTimestamp(ts int64) (offset int64, found bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	hdr := make([]byte, record.HeaderSize)
	for _, seg := range l.segments {
		var pos int32
		for pos < seg.logSize {
			if _, err := seg.logFile.ReadAt(hdr, int64(pos)); err != nil {
				return 0, false
			}
			h, err := record.ParseHeader(hdr)
			if err != nil {
				return 0, false
			}
			if h.MaxTimestamp >= ts {
				return h.BaseOffset, true
			}
			pos += int32(h.TotalSize())
		}
	}
	return 0, false
}

// EarliestOffset is the base offset of the first segment.
func (l *Log) EarliestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

// LatestOffset is the high watermark (next offset to be assigned).
func (l *Log) LatestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// Flush fsyncs the active segment if dirty.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.dirty {
		return nil
	}
	if err := l.active.flush(); err != nil {
		return err
	}
	l.dirty = false
	return nil
}

// EnforceRetention deletes whole non-active segments that are older than maxAgeMs
// or, oldest-first, to bring total size under maxBytes. A zero limit disables that
// dimension. The active segment is never deleted.
func (l *Log) EnforceRetention(maxAgeMs, maxBytes int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) <= 1 {
		return nil
	}

	totalSize := int64(0)
	for _, s := range l.segments {
		totalSize += int64(s.logSize)
	}
	cutoff := time.Now().Add(-time.Duration(maxAgeMs) * time.Millisecond)

	deleted := 0
	for deleted < len(l.segments)-1 { // never the active segment
		s := l.segments[deleted]
		expired := false
		if maxAgeMs > 0 {
			if fi, err := s.logFile.Stat(); err == nil && fi.ModTime().Before(cutoff) {
				expired = true
			}
		}
		overSize := maxBytes > 0 && totalSize > maxBytes
		if !expired && !overSize {
			break
		}
		totalSize -= int64(s.logSize)
		s.close()
		os.Remove(logPath(l.dir, s.baseOffset))
		os.Remove(indexPath(l.dir, s.baseOffset))
		deleted++
	}
	if deleted > 0 {
		l.segments = l.segments[deleted:]
	}
	return nil
}

// Close flushes and closes all segment files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, s := range l.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// listSegmentBases returns the sorted base offsets of *.log files in dir.
func listSegmentBases(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// recoverActive scans the active segment's batches to compute the next offset and
// truncates a partial trailing batch left by a crash. It also rebuilds the in-memory
// sparse index for the segment.
func recoverActive(s *segment) (int64, error) {
	hdr := make([]byte, record.HeaderSize)
	var pos int32
	var next int64
	s.index = s.index[:0]
	s.bytesSinceIdx = 0
	for pos < s.logSize {
		remaining := s.logSize - pos
		if remaining < int32(record.HeaderSize) {
			break // partial header => truncate here
		}
		if _, err := s.logFile.ReadAt(hdr, int64(pos)); err != nil {
			return 0, err
		}
		h, err := record.ParseHeader(hdr)
		if err != nil {
			break
		}
		total := int32(h.TotalSize())
		if pos+total > s.logSize {
			break // partial batch body => truncate here
		}
		// rebuild sparse index
		if len(s.index) == 0 || s.bytesSinceIdx >= s.indexInterval {
			s.index = append(s.index, indexEntry{relOffset: int32(h.BaseOffset - s.baseOffset), position: pos})
			s.bytesSinceIdx = 0
		}
		next = h.LastOffset() + 1
		pos += total
		s.bytesSinceIdx += total
	}
	if pos != s.logSize {
		if err := s.logFile.Truncate(int64(pos)); err != nil {
			return 0, err
		}
		s.logSize = pos
	}
	// rewrite the on-disk index to match the rebuilt in-memory one
	if err := rewriteIndex(s); err != nil {
		return 0, err
	}
	if next == 0 {
		next = s.baseOffset
	}
	return next, nil
}

// rewriteIndex truncates and rewrites the segment's .index file from s.index.
func rewriteIndex(s *segment) error {
	if err := s.idxFile.Truncate(0); err != nil {
		return err
	}
	if _, err := s.idxFile.Seek(0, 0); err != nil {
		return err
	}
	buf := make([]byte, 0, len(s.index)*indexEntrySize)
	for _, e := range s.index {
		buf = append(buf, encodeIndexEntry(e)...)
	}
	_, err := s.idxFile.Write(buf)
	return err
}
