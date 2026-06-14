package controller

// indexStore adapts the controller into object.IndexStore: the object backend's index
// (which uploaded object holds each partition's offset range) lives in the replicated FSM,
// exactly where AutoMQ keeps it. Commit and Prune are raft commands so the index is durable
// and cluster-visible; Load and Referenced read the committed state.

import "mq/internal/storage/object"

// IndexStore returns this controller as an object.IndexStore.
func (c *Controller) IndexStore() object.IndexStore { return indexStore{c} }

type indexStore struct{ c *Controller }

var _ object.IndexStore = indexStore{}

// Commit replicates an uploaded object's slice through raft.
func (s indexStore) Commit(topic string, partition int32, ref object.SegmentRef) error {
	return s.c.Apply(Command{Type: CmdCommitSegment, Topic: topic, Partition: partition, Segment: &ref})
}

// Load returns a partition's committed refs (offset order is restored by the caller's cache).
func (s indexStore) Load(topic string, partition int32) ([]object.SegmentRef, error) {
	return s.c.FSM().Segments(topic, partition), nil
}

// Prune replicates the removal of refs fully below cutoff and returns the dropped ones. The
// cutoff defines exactly which refs go, so the dropped set is derived from the pre-prune
// committed state rather than the raft apply's response (Apply only surfaces errors, not the
// FSM return value).
func (s indexStore) Prune(topic string, partition int32, cutoff int64) ([]object.SegmentRef, error) {
	before := s.c.FSM().Segments(topic, partition)
	if err := s.c.Apply(Command{Type: CmdPruneSegments, Topic: topic, Partition: partition, Cutoff: cutoff}); err != nil {
		return nil, err
	}
	var dropped []object.SegmentRef
	for _, r := range before {
		if r.NextOffset <= cutoff {
			dropped = append(dropped, r)
		}
	}
	return dropped, nil
}

// Referenced reports whether any partition cluster-wide still references the object key.
func (s indexStore) Referenced(key string) (bool, error) {
	return s.c.FSM().SegmentReferenced(key), nil
}
