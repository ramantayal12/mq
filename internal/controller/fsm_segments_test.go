package controller

// Unit tests for the object backend's index living in the FSM: commit/prune/referenced and,
// crucially, that the index survives a snapshot/restore (the durability the file-less object
// backend depends on). Pure FSM — no raft quorum, no MinIO.

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	"mq/internal/storage/object"
)

func applyCmd(t *testing.T, f *FSM, cmd Command) {
	t.Helper()
	data, err := cmd.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if res := f.Apply(&raft.Log{Data: data}); res != nil {
		t.Fatalf("apply %d: %v", cmd.Type, res)
	}
}

func commitSeg(t *testing.T, f *FSM, topic string, p int32, ref object.SegmentRef) {
	applyCmd(t, f, Command{Type: CmdCommitSegment, Topic: topic, Partition: p, Segment: &ref})
}

func TestFSMSegmentIndex(t *testing.T) {
	f := NewFSM()
	// Object "a" batches t/0 [0,5) and t/1 [0,3); object "b" holds t/0 [5,8).
	commitSeg(t, f, "t", 0, object.SegmentRef{Key: "a", BaseOffset: 0, NextOffset: 5, Length: 100})
	commitSeg(t, f, "t", 0, object.SegmentRef{Key: "b", BaseOffset: 5, NextOffset: 8, Length: 60})
	commitSeg(t, f, "t", 1, object.SegmentRef{Key: "a", BaseOffset: 0, NextOffset: 3, Length: 40})

	if got := f.Segments("t", 0); len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("t/0 segments=%v want [a b]", got)
	}
	if got := f.Segments("t", 1); len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("t/1 segments=%v want [a]", got)
	}
	if !f.SegmentReferenced("a") || !f.SegmentReferenced("b") || f.SegmentReferenced("z") {
		t.Fatal("referenced: want a,b referenced and z not")
	}

	// Prune t/0 below 5: drops "a" from t/0, keeps "b". "a" still referenced by t/1.
	applyCmd(t, f, Command{Type: CmdPruneSegments, Topic: "t", Partition: 0, Cutoff: 5})
	if got := f.Segments("t", 0); len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("t/0 after prune=%v want [b]", got)
	}
	if !f.SegmentReferenced("a") {
		t.Fatal("a should still be referenced by t/1 after pruning t/0")
	}
	// Prune t/1 too: now nothing references "a".
	applyCmd(t, f, Command{Type: CmdPruneSegments, Topic: "t", Partition: 1, Cutoff: 3})
	if f.SegmentReferenced("a") {
		t.Fatal("a should be unreferenced after pruning t/1")
	}

	// The index must survive a snapshot/restore — it is the only durable home of which object
	// holds each partition's data, so a controller restart that lost it would orphan the data.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	data := snap.(*fsmSnapshot).data
	f2 := NewFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := f2.Segments("t", 0); len(got) != 1 || got[0].Key != "b" || got[0].NextOffset != 8 {
		t.Fatalf("restored t/0 segments=%v want [b [5,8)]", got)
	}
	if !f2.SegmentReferenced("b") || f2.SegmentReferenced("a") {
		t.Fatal("restored referenced: want b referenced, a not")
	}
}
