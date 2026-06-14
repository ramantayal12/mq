package object

// The object backend's retention path. A partition's data lives in uploaded stream-set objects
// (the sealed-segment analogue) plus the in-memory pending tail (the active-segment analogue).
// Retention drops whole uploaded refs oldest-first — by age or to fit a byte budget — exactly as
// the file log drops whole non-active segments; the pending tail is never dropped. Because a
// stream-set object batches many partitions, the physical object is collected only once every
// partition that shares it has pruned its slice.

import (
	"context"
	"time"
)

// size is the partition's stored size in bytes: every uploaded ref's slice plus the pending tail.
func (s *Storage) size(topic string, partition int32) int64 {
	var total int64
	for _, r := range s.index.refsFor(topic, partition) {
		total += r.Length
	}
	s.mu.Lock()
	if pp := s.pending[indexKey(topic, partition)]; pp != nil {
		total += pp.bytes
	}
	s.mu.Unlock()
	return total
}

// enforceRetention drops the partition's oldest uploaded refs that are older than maxAgeMs or
// that push its total size beyond maxBytes (a zero limit disables that dimension), deleting any
// object no partition references anymore. It returns the new earliest offset and whether anything
// was dropped. Object deletion is best-effort (like the file log's os.Remove): the logical drop
// is what matters, physical cleanup is a follow-on.
func (s *Storage) enforceRetention(topic string, partition int32, maxAgeMs, maxBytes int64) (int64, bool, error) {
	refs := s.index.refsFor(topic, partition) // sorted copy
	if len(refs) == 0 {
		return 0, false, nil
	}

	total := int64(0)
	for _, r := range refs {
		total += r.Length
	}
	s.mu.Lock()
	if pp := s.pending[indexKey(topic, partition)]; pp != nil {
		total += pp.bytes
	}
	s.mu.Unlock()

	nowMs := time.Now().UnixMilli()
	dropped := 0
	for dropped < len(refs) {
		r := refs[dropped]
		expired := maxAgeMs > 0 && nowMs-r.UploadedMs > maxAgeMs
		overSize := maxBytes > 0 && total > maxBytes
		if !expired && !overSize {
			break
		}
		total -= r.Length
		dropped++
	}
	if dropped == 0 {
		return 0, false, nil
	}

	// cutoff is the boundary the surviving data starts at — the new earliest offset. By
	// contiguity it equals the pending tail's start when every ref is dropped.
	cutoff := refs[dropped-1].NextOffset
	gone, err := s.index.prune(topic, partition, cutoff)
	if err != nil {
		return 0, false, err
	}
	seen := map[string]bool{}
	for _, r := range gone {
		if seen[r.Key] {
			continue
		}
		seen[r.Key] = true
		if !s.index.referenced(r.Key) {
			_ = s.store.Delete(context.Background(), r.Key)
		}
	}
	return cutoff, true, nil
}
