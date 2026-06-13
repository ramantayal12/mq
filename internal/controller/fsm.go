package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// partitionMeta is the authoritative placement of one partition.
type partitionMeta struct {
	Replicas    []int32 `json:"replicas"`
	Leader      int32   `json:"leader"`
	ISR         []int32 `json:"isr"`
	LeaderEpoch int32   `json:"epoch"`
}

// topicMeta is the authoritative metadata for one topic.
type topicMeta struct {
	Partitions int32           `json:"partitions"`
	Parts      []partitionMeta `json:"parts"`
}

// metaState is the whole replicated metadata. It is mutated only by FSM.Apply (the
// single writer), so application is deterministic across the quorum.
type metaState struct {
	Topics map[string]*topicMeta `json:"topics"`
}

// FSM is the raft.FSM that owns the cluster metadata. Reads take the read lock; the
// only writer is Apply, driven by the committed raft log.
type FSM struct {
	mu      sync.RWMutex
	state   metaState
	onApply func() // optional, set via SetOnApply; called after every Apply/Restore
}

// NewFSM returns an empty FSM.
func NewFSM() *FSM {
	return &FSM{state: metaState{Topics: map[string]*topicMeta{}}}
}

// SetOnApply registers a callback fired after each committed Apply and each Restore,
// always outside the FSM lock so the callback may read the FSM. The broker uses it to
// reconcile its local logs against the committed placement (open follower partitions).
func (f *FSM) SetOnApply(fn func()) {
	f.mu.Lock()
	f.onApply = fn
	f.mu.Unlock()
}

func (f *FSM) notifyApplied() {
	f.mu.RLock()
	fn := f.onApply
	f.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

// Apply decodes one committed command and mutates the state. A returned error is
// delivered to the proposer via the ApplyFuture; it does not stop the FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return fmt.Errorf("controller: undecodable command: %w", err)
	}
	f.mu.Lock()
	res := f.applyLocked(cmd)
	f.mu.Unlock()
	f.notifyApplied()
	return res
}

func (f *FSM) applyLocked(cmd Command) error {
	switch cmd.Type {
	case CmdCreateTopic:
		if _, ok := f.state.Topics[cmd.Topic]; ok {
			return fmt.Errorf("controller: topic %q already exists", cmd.Topic)
		}
		f.state.Topics[cmd.Topic] = &topicMeta{
			Partitions: int32(len(cmd.Replicas)),
			Parts:      partsFromReplicas(cmd.Replicas),
		}
		return nil
	case CmdCreatePartitions:
		t, ok := f.state.Topics[cmd.Topic]
		if !ok {
			return fmt.Errorf("controller: unknown topic %q", cmd.Topic)
		}
		t.Parts = append(t.Parts, partsFromReplicas(cmd.Replicas)...)
		t.Partitions = int32(len(t.Parts))
		return nil
	case CmdChangeLeader:
		pm, err := f.partition(cmd.Topic, cmd.Partition)
		if err != nil {
			return err
		}
		pm.Leader = cmd.Leader
		pm.ISR = append([]int32(nil), cmd.ISR...)
		pm.LeaderEpoch++
		return nil
	case CmdChangeISR:
		pm, err := f.partition(cmd.Topic, cmd.Partition)
		if err != nil {
			return err
		}
		pm.ISR = append([]int32(nil), cmd.ISR...)
		return nil
	default:
		return fmt.Errorf("controller: unknown command type %d", cmd.Type)
	}
}

// partition returns a pointer into the state for (topic, partition). Caller holds the
// lock for the duration of any mutation through the returned pointer.
func (f *FSM) partition(topic string, p int32) (*partitionMeta, error) {
	t, ok := f.state.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("controller: unknown topic %q", topic)
	}
	if p < 0 || int(p) >= len(t.Parts) {
		return nil, fmt.Errorf("controller: topic %q has no partition %d", topic, p)
	}
	return &t.Parts[p], nil
}

// partsFromReplicas builds partition metadata from per-partition replica sets: the
// first replica leads and the full set is the initial ISR. An empty replica set yields
// a leaderless partition (-1), which the controller can repair via CmdChangeLeader.
func partsFromReplicas(replicas [][]int32) []partitionMeta {
	parts := make([]partitionMeta, len(replicas))
	for i, rs := range replicas {
		pm := partitionMeta{Replicas: append([]int32(nil), rs...), ISR: append([]int32(nil), rs...), Leader: -1}
		if len(rs) > 0 {
			pm.Leader = rs[0]
		}
		parts[i] = pm
	}
	return parts
}

// HasTopic reports whether the topic exists in the committed metadata.
func (f *FSM) HasTopic(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.state.Topics[name]
	return ok
}

// PartitionLeader returns the committed leader and leader-epoch for a partition.
func (f *FSM) PartitionLeader(topic string, p int32) (leader, epoch int32, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, exists := f.state.Topics[topic]
	if !exists || p < 0 || int(p) >= len(t.Parts) {
		return 0, 0, false
	}
	return t.Parts[p].Leader, t.Parts[p].LeaderEpoch, true
}

// PartitionView is a read-only copy of one partition's committed placement.
type PartitionView struct {
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

// TopicView is a read-only copy of one topic's committed placement.
type TopicView struct {
	Name       string
	Partitions int32
	Parts      []PartitionView
}

// Partition returns a copy of the committed placement for (topic, partition).
func (f *FSM) Partition(topic string, p int32) (PartitionView, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.state.Topics[topic]
	if !ok || p < 0 || int(p) >= len(t.Parts) {
		return PartitionView{}, false
	}
	return viewPart(t.Parts[p]), true
}

// Partitions returns a topic's committed partition count.
func (f *FSM) Partitions(topic string) (int32, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.state.Topics[topic]
	if !ok {
		return 0, false
	}
	return t.Partitions, true
}

// Topics returns a copy of every committed topic, sorted by name (stable ordering for
// metadata responses and reconciliation).
func (f *FSM) Topics() []TopicView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]TopicView, 0, len(f.state.Topics))
	for name, t := range f.state.Topics {
		tv := TopicView{Name: name, Partitions: t.Partitions, Parts: make([]PartitionView, len(t.Parts))}
		for i, pm := range t.Parts {
			tv.Parts[i] = viewPart(pm)
		}
		out = append(out, tv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func viewPart(pm partitionMeta) PartitionView {
	return PartitionView{
		Leader:      pm.Leader,
		LeaderEpoch: pm.LeaderEpoch,
		Replicas:    append([]int32(nil), pm.Replicas...),
		ISR:         append([]int32(nil), pm.ISR...),
	}
}

// Snapshot captures the whole state as JSON. The state is tiny, so a full snapshot per
// call is cheap and there is no need for incremental snapshots.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the state from a snapshot produced by Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var st metaState
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return err
	}
	if st.Topics == nil {
		st.Topics = map[string]*topicMeta{}
	}
	f.mu.Lock()
	f.state = st
	f.mu.Unlock()
	f.notifyApplied()
	return nil
}

// fsmSnapshot is an immutable JSON encoding of the state at snapshot time.
type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
