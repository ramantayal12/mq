package object

// ObjectLog is the per-partition storage.Backend over the node-level Storage. It owns the
// partition's offset state (LEO/HWM, exactly as the file-based *storage.Log does) but holds no
// data of its own: Append patches and assigns the offset, then hands the batch to the shared
// Storage, which durably WALs it and later uploads it. The read, replica, and retention paths
// delegate the data work to Storage while keeping the offset bookkeeping here.

import (
	"fmt"
	"sync"

	"mq/internal/record"
	"mq/internal/storage"
)

// ObjectLog is a thin per-partition handle over Storage. Safe for concurrent use.
type ObjectLog struct {
	storage   *Storage
	topic     string
	partition int32

	mu            sync.RWMutex
	nextOffset    int64 // LEO: next offset to assign
	highWatermark int64 // committed offset
	holdHWM       bool  // replication mode: Append advances LEO only
	earliest      int64 // oldest retained offset
}

var _ storage.Backend = (*ObjectLog)(nil)

// Append assigns the next offset to batch, patches its header + CRC, durably records it via the
// node Storage (WAL + pending), and advances the LEO (and the HWM when not replicating).
func (l *ObjectLog) Append(batch []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	base := l.nextOffset
	record.SetBaseOffset(batch, base)
	record.RecomputeCRC(batch)
	h, err := record.ParseHeader(batch)
	if err != nil {
		return 0, err
	}
	next := h.LastOffset() + 1

	// Copy the batch: Storage buffers it past this call, and the caller's slice may be reused.
	b := make([]byte, len(batch))
	copy(b, batch)
	if err := l.storage.append(walRecord{Topic: l.topic, Partition: l.partition, BaseOffset: base, Batch: b}, next); err != nil {
		return 0, err
	}
	l.nextOffset = next
	if !l.holdHWM {
		l.highWatermark = next
	}
	return base, nil
}

// HighWatermark returns the committed offset.
func (l *ObjectLog) HighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.highWatermark
}

// SetHighWatermark advances the HWM, clamped to never move backward or exceed the LEO.
func (l *ObjectLog) SetHighWatermark(hwm int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if hwm > l.nextOffset {
		hwm = l.nextOffset
	}
	if hwm > l.highWatermark {
		l.highWatermark = hwm
	}
}

// HoldHighWatermark switches the log into replication mode (Append advances only the LEO).
func (l *ObjectLog) HoldHighWatermark() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holdHWM = true
}

// EarliestOffset is the oldest retained offset.
func (l *ObjectLog) EarliestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.earliest
}

// LatestOffset is the log end offset (next offset to assign).
func (l *ObjectLog) LatestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// Flush durably persists buffered writes (fsyncs the node WAL).
func (l *ObjectLog) Flush() error { return l.storage.sync() }

// Close releases the handle. The shared node Storage is closed separately (Storage.Close).
func (l *ObjectLog) Close() error { return nil }

// Read returns whole record batches starting at offset, up to maxBytes. It mirrors
// *storage.Log.Read: a read exactly at the LEO returns nil with no error (the caught-up
// consumer should poll again); an offset below the earliest retained offset or above the LEO
// returns storage.ErrOffsetOutOfRange. The data itself comes from the shared node Storage
// (pending buffer or an uploaded object).
func (l *ObjectLog) Read(offset int64, maxBytes int32) ([]byte, error) {
	l.mu.RLock()
	next, earliest := l.nextOffset, l.earliest
	l.mu.RUnlock()

	if offset == next {
		return nil, nil // caught up at the log end
	}
	if offset > next || offset < earliest {
		return nil, storage.ErrOffsetOutOfRange
	}
	return l.storage.read(l.topic, l.partition, offset, maxBytes)
}

// OffsetForTimestamp answers ListOffsets-by-time, scanning uploaded objects then the pending
// tail (delegated to the shared Storage).
func (l *ObjectLog) OffsetForTimestamp(ts int64) (int64, bool) {
	return l.storage.offsetForTimestamp(l.topic, l.partition, ts)
}

// AppendReplica writes a leader-assigned batch verbatim — the follower write path. It preserves
// the leader's base offset and CRC (the bytes are already authoritative), durably records it via
// the node Storage, and advances only the LEO (a follower's HWM is driven by the leader-reported
// value through SetHighWatermark). A batch already below the LEO is a harmless duplicate; a gap
// above it is rejected so the fetcher resyncs. Mirrors *storage.Log.AppendReplica.
func (l *ObjectLog) AppendReplica(batch []byte) (int64, error) {
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
		return l.nextOffset, fmt.Errorf("object: replica gap: batch base %d > log end %d", h.BaseOffset, l.nextOffset)
	}
	next := h.LastOffset() + 1

	// Copy the batch: Storage buffers it past this call, and the caller's slice may be reused.
	b := make([]byte, len(batch))
	copy(b, batch)
	if err := l.storage.append(walRecord{Topic: l.topic, Partition: l.partition, BaseOffset: h.BaseOffset, Batch: b}, next); err != nil {
		return 0, err
	}
	l.nextOffset = next
	return l.nextOffset, nil
}

// Size is the partition's total stored size in bytes — uploaded objects plus the pending tail.
func (l *ObjectLog) Size() int64 {
	return l.storage.size(l.topic, l.partition)
}

// EnforceRetention drops the partition's oldest uploaded objects that are older than maxAgeMs or
// that push its size past maxBytes (oldest-first), advancing the earliest offset and collecting
// any object no partition references anymore. The pending tail is never dropped (it is the
// active-segment analogue). Mirrors *storage.Log.EnforceRetention at object granularity.
func (l *ObjectLog) EnforceRetention(maxAgeMs, maxBytes int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	newEarliest, changed, err := l.storage.enforceRetention(l.topic, l.partition, maxAgeMs, maxBytes)
	if err != nil {
		return err
	}
	if changed && newEarliest > l.earliest {
		l.earliest = newEarliest
	}
	return nil
}
