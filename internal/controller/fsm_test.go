package controller

import (
	"bytes"
	"io"
	"testing"
)

// memSink is an in-memory raft.SnapshotSink for exercising Persist.
type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string    { return "test" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }

func TestFSMCreateTopicPlacement(t *testing.T) {
	f := NewFSM()
	if err := f.applyLocked(Command{Type: CmdCreateTopic, Topic: "t", Replicas: [][]int32{{0, 1}, {2, 0}}}); err != nil {
		t.Fatal(err)
	}
	if !f.HasTopic("t") {
		t.Fatal("topic not created")
	}
	if l, _, ok := f.PartitionLeader("t", 0); !ok || l != 0 {
		t.Fatalf("partition 0 leader = %d ok=%v, want 0", l, ok)
	}
	if l, _, ok := f.PartitionLeader("t", 1); !ok || l != 2 {
		t.Fatalf("partition 1 leader = %d ok=%v, want 2", l, ok)
	}
	// Re-creating the same topic is rejected.
	if err := f.applyLocked(Command{Type: CmdCreateTopic, Topic: "t", Replicas: [][]int32{{0}}}); err == nil {
		t.Fatal("expected error re-creating existing topic")
	}
}

func TestFSMChangeLeaderBumpsEpoch(t *testing.T) {
	f := NewFSM()
	_ = f.applyLocked(Command{Type: CmdCreateTopic, Topic: "t", Replicas: [][]int32{{0, 1, 2}}})
	_, epoch0, _ := f.PartitionLeader("t", 0)
	if err := f.applyLocked(Command{Type: CmdChangeLeader, Topic: "t", Partition: 0, Leader: 1, ISR: []int32{1, 2}}); err != nil {
		t.Fatal(err)
	}
	l, epoch1, ok := f.PartitionLeader("t", 0)
	if !ok || l != 1 {
		t.Fatalf("leader = %d, want 1", l)
	}
	if epoch1 != epoch0+1 {
		t.Fatalf("epoch = %d, want %d (bumped)", epoch1, epoch0+1)
	}
	// Unknown partition is an error.
	if err := f.applyLocked(Command{Type: CmdChangeLeader, Topic: "t", Partition: 9}); err == nil {
		t.Fatal("expected error for unknown partition")
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	src := NewFSM()
	_ = src.applyLocked(Command{Type: CmdCreateTopic, Topic: "a", Replicas: [][]int32{{0, 1}}})
	_ = src.applyLocked(Command{Type: CmdCreateTopic, Topic: "b", Replicas: [][]int32{{1, 2}, {2, 0}}})
	_ = src.applyLocked(Command{Type: CmdChangeLeader, Topic: "b", Partition: 1, Leader: 0, ISR: []int32{0}})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if !dst.HasTopic("a") || !dst.HasTopic("b") {
		t.Fatal("restored FSM missing topics")
	}
	if l, e, ok := dst.PartitionLeader("b", 1); !ok || l != 0 || e != 1 {
		t.Fatalf("restored b/1 = leader %d epoch %d, want leader 0 epoch 1", l, e)
	}
}
