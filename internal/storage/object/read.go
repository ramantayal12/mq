package object

// The object backend's read path. A partition's records live in two places: the in-memory
// pending buffer (appended but not yet uploaded — this also holds any tail recovered from the
// WAL on restart) and the uploaded stream-set objects the index points at. The two are
// contiguous: pending always begins exactly where the index leaves off, so any committed offset
// is in one place or the other. read resolves which and returns whole batches from that single
// source, mirroring *storage.Log.Read (which serves from one segment per call).

import (
	"context"

	"mq/internal/record"
)

// read returns whole record batches for a partition starting at offset, up to maxBytes (always
// at least one batch if available). offset is assumed in range [earliest, LEO) — the bounds
// checks live in ObjectLog.Read. It serves from the pending buffer when offset falls in the
// not-yet-uploaded tail, otherwise from the uploaded object the index points at (ranged GET).
func (s *Storage) read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	s.mu.Lock()
	var pendingData []byte
	if pp := s.pending[indexKey(topic, partition)]; pp != nil && len(pp.batches) > 0 && offset >= pp.baseOffset {
		for _, b := range pp.batches {
			pendingData = append(pendingData, b...)
		}
	}
	s.mu.Unlock()
	if pendingData != nil {
		return sliceBatches(pendingData, offset, maxBytes)
	}

	ref, ok := s.index.refFor(topic, partition, offset)
	if !ok {
		// A concurrent upload may have moved this offset between the bounds check and here; if
		// neither pending nor the index has it, treat as caught up rather than erroring.
		return nil, nil
	}
	data, err := s.store.Get(context.Background(), ref.Key, ref.Position, ref.Length)
	if err != nil {
		return nil, err
	}
	return sliceBatches(data, offset, maxBytes)
}

// offsetForTimestamp returns the base offset of the first batch whose maximum timestamp is >= ts,
// scanning uploaded objects in offset order and then the pending tail. Batches are appended in
// non-decreasing time, so the first qualifying batch is the earliest match. Mirrors
// *storage.Log.OffsetForTimestamp.
func (s *Storage) offsetForTimestamp(topic string, partition int32, ts int64) (int64, bool) {
	for _, ref := range s.index.refsFor(topic, partition) {
		data, err := s.store.Get(context.Background(), ref.Key, ref.Position, ref.Length)
		if err != nil {
			return 0, false
		}
		if off, ok := scanTimestamp(data, ts); ok {
			return off, true
		}
	}
	s.mu.Lock()
	var data []byte
	if pp := s.pending[indexKey(topic, partition)]; pp != nil {
		for _, b := range pp.batches {
			data = append(data, b...)
		}
	}
	s.mu.Unlock()
	return scanTimestamp(data, ts)
}

// sliceBatches returns whole batches from a contiguous run of concatenated batch bytes, starting
// at the first batch whose last offset >= wantOffset, accumulating up to maxBytes (always at
// least one batch). It is the in-memory analogue of segment.readBatchesFrom.
func sliceBatches(data []byte, wantOffset int64, maxBytes int32) ([]byte, error) {
	var out []byte
	for pos := 0; pos < len(data); {
		h, err := record.ParseHeader(data[pos:])
		if err != nil {
			return out, err
		}
		total := int(h.TotalSize())
		if pos+total > len(data) {
			break // partial trailing batch (shouldn't happen for committed data)
		}
		if h.LastOffset() < wantOffset { // batch entirely before target
			pos += total
			continue
		}
		if len(out) > 0 && int32(len(out))+int32(total) > maxBytes {
			break
		}
		out = append(out, data[pos:pos+total]...)
		pos += total
		if int32(len(out)) >= maxBytes {
			break
		}
	}
	return out, nil
}

// scanTimestamp returns the base offset of the first batch in data whose MaxTimestamp >= ts.
func scanTimestamp(data []byte, ts int64) (int64, bool) {
	for pos := 0; pos < len(data); {
		h, err := record.ParseHeader(data[pos:])
		if err != nil {
			return 0, false
		}
		if h.MaxTimestamp >= ts {
			return h.BaseOffset, true
		}
		pos += int(h.TotalSize())
	}
	return 0, false
}
