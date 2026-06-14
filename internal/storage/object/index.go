package object

// The object index is the metadata that maps each partition's offset ranges to the object
// (and byte span within it) that holds them — AutoMQ keeps exactly this in the KRaft
// controller. Here it is split in two:
//
//   - SegmentRef / IndexStore: the *durable* metadata seam. An IndexStore persists committed
//     object metadata so it survives a restart. Phase 3 ships an in-memory impl for tests; the
//     production impl, backed by the Raft FSM (faithful to AutoMQ), is wired in at broker
//     hook-up. The interface is the seam that lets the FSM own this without storage importing
//     the controller.
//   - Index: an in-memory cache over an IndexStore, keyed by (topic, partition), keeping each
//     partition's refs sorted by offset for lookup, earliest/latest, and retention.

import (
	"sort"
	"strconv"
	"sync"
)

// SegmentRef locates one partition's slice of records inside an object: the object Key, the
// half-open offset range [BaseOffset, NextOffset) the slice covers, and the byte span
// [Position, Position+Length) within the object where that slice's batches live. A stream-set
// object holds one SegmentRef per partition it batched. UploadedMs is the wall-clock time the
// object was written, the age proxy retention uses (mirroring the file log's segment ModTime).
type SegmentRef struct {
	Key        string
	BaseOffset int64
	NextOffset int64
	Position   int64
	Length     int64
	UploadedMs int64
}

// IndexStore is the durable home of object metadata. Commit records that an object now holds a
// partition's offset range; Load returns a partition's committed refs in offset order. The
// production implementation is backed by the Raft FSM (AutoMQ keeps this in the controller);
// MemIndexStore is the in-memory test double.
type IndexStore interface {
	Commit(topic string, partition int32, ref SegmentRef) error
	Load(topic string, partition int32) ([]SegmentRef, error)
	// Prune durably removes the partition's refs whose NextOffset <= cutoff (fully below the
	// retention point) and returns them. The FSM impl applies this as a replicated command.
	Prune(topic string, partition int32, cutoff int64) ([]SegmentRef, error)
	// Referenced reports whether any partition still references the object key. The FSM impl
	// answers this cluster-wide, so an object is collected only once no partition anywhere
	// (on any node) still points at it.
	Referenced(key string) (bool, error)
}

// MemIndexStore is an in-memory IndexStore for tests and single-node use.
type MemIndexStore struct {
	mu sync.Mutex
	m  map[string][]SegmentRef
}

func NewMemIndexStore() *MemIndexStore { return &MemIndexStore{m: map[string][]SegmentRef{}} }

func (s *MemIndexStore) Commit(topic string, partition int32, ref SegmentRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := indexKey(topic, partition)
	s.m[k] = append(s.m[k], ref)
	return nil
}

func (s *MemIndexStore) Load(topic string, partition int32) ([]SegmentRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := s.m[indexKey(topic, partition)]
	out := make([]SegmentRef, len(refs))
	copy(out, refs)
	return out, nil
}

func (s *MemIndexStore) Prune(topic string, partition int32, cutoff int64) ([]SegmentRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := indexKey(topic, partition)
	var kept, dropped []SegmentRef
	for _, r := range s.m[k] {
		if r.NextOffset <= cutoff {
			dropped = append(dropped, r)
		} else {
			kept = append(kept, r)
		}
	}
	s.m[k] = kept
	return dropped, nil
}

func (s *MemIndexStore) Referenced(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, refs := range s.m {
		for _, r := range refs {
			if r.Key == key {
				return true, nil
			}
		}
	}
	return false, nil
}

// Index is the in-memory, sorted view of an IndexStore.
type Index struct {
	mu    sync.RWMutex
	store IndexStore
	refs  map[string][]SegmentRef // keyed by indexKey; sorted by BaseOffset
}

func newIndex(store IndexStore) *Index {
	return &Index{store: store, refs: map[string][]SegmentRef{}}
}

// load pulls a partition's committed refs from the store into the cache (on open).
func (ix *Index) load(topic string, partition int32) error {
	refs, err := ix.store.Load(topic, partition)
	if err != nil {
		return err
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].BaseOffset < refs[j].BaseOffset })
	ix.mu.Lock()
	ix.refs[indexKey(topic, partition)] = refs
	ix.mu.Unlock()
	return nil
}

// commit durably records ref and adds it to the cache.
func (ix *Index) commit(topic string, partition int32, ref SegmentRef) error {
	if err := ix.store.Commit(topic, partition, ref); err != nil {
		return err
	}
	ix.mu.Lock()
	k := indexKey(topic, partition)
	ix.refs[k] = append(ix.refs[k], ref)
	ix.mu.Unlock()
	return nil
}

// refsFor returns a copy of a partition's refs in offset order.
func (ix *Index) refsFor(topic string, partition int32) []SegmentRef {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	refs := ix.refs[indexKey(topic, partition)]
	out := make([]SegmentRef, len(refs))
	copy(out, refs)
	return out
}

// refFor returns the committed ref whose half-open offset range [BaseOffset, NextOffset)
// contains offset, or false if no uploaded object covers it. Refs are sorted by BaseOffset, so
// a binary search for the largest BaseOffset <= offset finds the only candidate.
func (ix *Index) refFor(topic string, partition int32, offset int64) (SegmentRef, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	refs := ix.refs[indexKey(topic, partition)]
	i := sort.Search(len(refs), func(i int) bool { return refs[i].BaseOffset > offset })
	if i == 0 {
		return SegmentRef{}, false
	}
	ref := refs[i-1]
	if offset >= ref.NextOffset {
		return SegmentRef{}, false
	}
	return ref, true
}

// prune durably drops the partition's refs whose NextOffset <= cutoff and removes them from the
// cache, returning the dropped refs (so the caller can collect now-unreferenced objects).
func (ix *Index) prune(topic string, partition int32, cutoff int64) ([]SegmentRef, error) {
	dropped, err := ix.store.Prune(topic, partition, cutoff)
	if err != nil {
		return nil, err
	}
	ix.mu.Lock()
	k := indexKey(topic, partition)
	kept := ix.refs[k][:0:0]
	for _, r := range ix.refs[k] {
		if r.NextOffset > cutoff {
			kept = append(kept, r)
		}
	}
	ix.refs[k] = kept
	ix.mu.Unlock()
	return dropped, nil
}

// referenced reports whether any partition still has a ref into the object key. A stream-set
// object batches many partitions, so it is collectible only once every partition that shares it
// has pruned its slice. The durable store is authoritative — the FSM-backed store answers this
// cluster-wide. A store error is treated as "referenced" so a transient failure never collects a
// still-live object.
func (ix *Index) referenced(key string) bool {
	ok, err := ix.store.Referenced(key)
	if err != nil {
		return true
	}
	return ok
}

// earliest is the lowest offset still in the index, or false if the partition has no objects.
func (ix *Index) earliest(topic string, partition int32) (int64, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	refs := ix.refs[indexKey(topic, partition)]
	if len(refs) == 0 {
		return 0, false
	}
	return refs[0].BaseOffset, true
}

// latest is the next offset after the highest-offset object, or false if there are none.
func (ix *Index) latest(topic string, partition int32) (int64, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	refs := ix.refs[indexKey(topic, partition)]
	if len(refs) == 0 {
		return 0, false
	}
	return refs[len(refs)-1].NextOffset, true
}

func indexKey(topic string, partition int32) string {
	return topic + "\x00" + strconv.Itoa(int(partition))
}
