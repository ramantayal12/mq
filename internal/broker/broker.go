// Package broker holds this node's topic catalog and the storage logs for the
// partitions it leads. Partition placement is owned by internal/cluster: a broker
// stores a partition's log only when the cluster says it leads that partition
// (replication factor 1). It knows nothing about the wire protocol.
package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/controller"
	"mq/internal/replication"
	"mq/internal/storage"
)

// Errors returned by LocalLog so handlers can map them to Kafka error codes.
var (
	ErrNotLeader        = errors.New("broker: not leader for partition")
	ErrUnknownPartition = errors.New("broker: unknown partition")
)

// TopicInfo is a topic name and its partition count.
type TopicInfo struct {
	Name       string
	Partitions int32
}

// Broker tracks the topic catalog (topic -> partition count) and holds storage logs
// for partitions this node replicates.
//
// Placement has two modes. Without a controller (single-node, or the static-membership
// cluster) the pure-function cluster view decides leadership (replication factor 1).
// With a controller wired in (SetController), the Raft FSM is authoritative: topic
// creation is proposed through the controller, and this node opens a log for every
// partition it replicates (leader or follower) by reconciling against the FSM.
type Broker struct {
	cfg     config.Config
	logCfg  storage.Config
	cluster *cluster.Cluster
	ctrl    *controller.Controller // nil unless cluster mode wires in the Raft controller
	repl    *replication.Manager   // nil unless a controller is wired in; follower fetchers
	mu      sync.RWMutex
	catalog map[string]int32                  // topic -> partition count (cluster-wide)
	logs    map[string]map[int32]*storage.Log // topic -> partition -> local log

	// Leader-side replication state (controller mode only), guarded by replMu.
	replMu    sync.Mutex
	followers map[tpKey]map[int32]followerProgress // (topic,partition) -> replica id -> progress
	leadSince map[tpKey]time.Time                  // when this node started leading a partition
	maintStop chan struct{}                        // closed to stop the ISR/HWM maintenance loop
	maintDone chan struct{}                        // closed when that loop has exited
}

// tpKey identifies a topic-partition for the leader-side replication maps.
type tpKey struct {
	topic string
	p     int32
}

// followerProgress is the leader's view of one follower: the offset it has replicated up
// to (its next fetch offset) and when it was last heard from.
type followerProgress struct {
	endOffset int64
	lastSeen  time.Time
}

const (
	// replicaLagTimeout is how long a follower may go without fetching before the leader
	// shrinks it out of the ISR (so a dead follower stops holding back the high watermark).
	replicaLagTimeout = 10 * time.Second
	// replicaMaintInterval is how often the leader re-evaluates ISR membership and the HWM.
	replicaMaintInterval = time.Second
)

// New constructs the broker over the given cluster view and loads topics already on
// disk (the led-partition directories under the data dir).
func New(cfg config.Config, cl *cluster.Cluster) (*Broker, error) {
	b := &Broker{
		cfg:       cfg,
		logCfg:    storage.Config{SegmentBytes: cfg.SegmentBytes, IndexIntervalBytes: cfg.IndexIntervalBytes},
		cluster:   cl,
		catalog:   map[string]int32{},
		logs:      map[string]map[int32]*storage.Log{},
		followers: map[tpKey]map[int32]followerProgress{},
		leadSince: map[tpKey]time.Time{},
	}
	if err := os.MkdirAll(cfg.LogDirs, 0o755); err != nil {
		return nil, err
	}
	if err := b.loadExisting(); err != nil {
		return nil, err
	}
	return b, nil
}

// Cluster returns the membership/placement view.
func (b *Broker) Cluster() *cluster.Cluster { return b.cluster }

// NodeID is this broker's id.
func (b *Broker) NodeID() int32 { return b.cluster.Self() }

// topicApplyWait bounds how long a create waits for the local FSM to reflect a commit
// (a follower applies asynchronously after the leader commits).
const topicApplyWait = 3 * time.Second

// SetController switches the broker to FSM-backed placement: subsequent topic creation
// is proposed through the controller, and the broker reconciles its local logs against
// the committed placement on every apply. Called once at startup, before serving.
func (b *Broker) SetController(ctrl *controller.Controller) {
	b.mu.Lock()
	b.ctrl = ctrl
	b.repl = replication.NewManager(b.cluster.Self(), b.leaderAddr, b.replicaLog)
	b.mu.Unlock()
	ctrl.FSM().SetOnApply(b.reconcileFromFSM)
	b.reconcileFromFSM() // adopt any state replayed/restored before the hook was registered
	b.startReplicaMaintenance()
}

// leaderAddr resolves the current leader's node id and Kafka address for a partition from
// the FSM placement; used by follower fetchers to target the leader. ok is false when no
// leader is committed yet.
func (b *Broker) leaderAddr(topic string, p int32) (int32, string, bool) {
	pv, ok := b.ctrl.FSM().Partition(topic, p)
	if !ok || pv.Leader < 0 {
		return 0, "", false
	}
	bi, ok := b.cluster.Broker(pv.Leader)
	if !ok {
		return 0, "", false
	}
	return pv.Leader, fmt.Sprintf("%s:%d", bi.Host, bi.Port), true
}

// replicaLog opens (if needed) and returns this node's local log for a followed partition.
func (b *Broker) replicaLog(topic string, p int32) (*storage.Log, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.openLocked(topic, p)
}

// RecordReplicaFetch notes that a follower (replica id) fetched from the given offset,
// meaning it holds every record below that offset. Called by the fetch handler when a
// fetch carries a ReplicaID >= 0. It refreshes the follower's progress and re-derives the
// partition's high watermark. No-op without a controller.
func (b *Broker) RecordReplicaFetch(topic string, p, replicaID int32, fetchOffset int64) {
	if b.ctrl == nil {
		return
	}
	key := tpKey{topic, p}
	b.replMu.Lock()
	if b.followers[key] == nil {
		b.followers[key] = map[int32]followerProgress{}
	}
	b.followers[key][replicaID] = followerProgress{endOffset: fetchOffset, lastSeen: time.Now()}
	b.replMu.Unlock()
	b.advanceHighWatermark(topic, p)
}

// advanceHighWatermark sets the leader's HWM for a partition to the smallest replicated
// offset across its in-sync replicas (the leader itself contributes its LEO). A no-op
// unless this node leads the partition. An in-sync follower with no progress yet pins the
// HWM at 0 until its first fetch, which is correct: nothing is committed until replicated.
func (b *Broker) advanceHighWatermark(topic string, p int32) {
	pv, ok := b.ctrl.FSM().Partition(topic, p)
	if !ok || pv.Leader != b.cluster.Self() {
		return
	}
	l := b.getLog(topic, p)
	if l == nil {
		return
	}
	hwm := l.LatestOffset()
	b.replMu.Lock()
	prog := b.followers[tpKey{topic, p}]
	for _, id := range pv.ISR {
		if id == b.cluster.Self() {
			continue
		}
		off := int64(0)
		if pr, ok := prog[id]; ok {
			off = pr.endOffset
		}
		if off < hwm {
			hwm = off
		}
	}
	b.replMu.Unlock()
	l.SetHighWatermark(hwm)
}

// getLog returns this node's already-open log for a partition, or nil.
func (b *Broker) getLog(topic string, p int32) *storage.Log {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if pm, ok := b.logs[topic]; ok {
		return pm[p]
	}
	return nil
}

// noteLeadSince records (once) when this node started leading a partition, used as the
// grace window before an unheard-from follower is shrunk out of the ISR at startup.
func (b *Broker) noteLeadSince(topic string, p int32) {
	key := tpKey{topic, p}
	b.replMu.Lock()
	if _, ok := b.leadSince[key]; !ok {
		b.leadSince[key] = time.Now()
	}
	b.replMu.Unlock()
}

// startReplicaMaintenance launches the leader-side loop that shrinks/expands the ISR and
// refreshes high watermarks for the partitions this node leads.
func (b *Broker) startReplicaMaintenance() {
	b.replMu.Lock()
	if b.maintStop != nil {
		b.replMu.Unlock()
		return
	}
	b.maintStop = make(chan struct{})
	b.maintDone = make(chan struct{})
	stop, done := b.maintStop, b.maintDone
	b.replMu.Unlock()

	go func() {
		defer close(done)
		t := time.NewTicker(replicaMaintInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				b.maintainReplicas()
			}
		}
	}()
}

// maintainReplicas re-evaluates ISR membership and the HWM for every RF>1 partition this
// node leads.
func (b *Broker) maintainReplicas() {
	self := b.cluster.Self()
	for _, tv := range b.ctrl.FSM().Topics() {
		for p := range tv.Parts {
			pv := tv.Parts[p]
			if pv.Leader != self || len(pv.Replicas) <= 1 {
				continue
			}
			b.maintainPartitionISR(tv.Name, int32(p), pv)
			b.advanceHighWatermark(tv.Name, int32(p))
		}
	}
}

// maintainPartitionISR computes the desired ISR for a led partition (this node plus every
// replica heard from within replicaLagTimeout, with a startup grace for replicas not yet
// seen) and, when it differs from the committed ISR, proposes the change through the
// controller. ISR changes are durable metadata, so they go through Raft, not local state.
func (b *Broker) maintainPartitionISR(topic string, p int32, pv controller.PartitionView) {
	self := b.cluster.Self()
	now := time.Now()
	key := tpKey{topic, p}

	b.replMu.Lock()
	prog := b.followers[key]
	leadSince := b.leadSince[key]
	desired := make([]int32, 0, len(pv.Replicas))
	for _, r := range pv.Replicas {
		if r == self {
			desired = append(desired, self)
			continue
		}
		inSync := false
		if pr, seen := prog[r]; seen {
			inSync = now.Sub(pr.lastSeen) <= replicaLagTimeout
		} else if !leadSince.IsZero() {
			inSync = now.Sub(leadSince) <= replicaLagTimeout // not heard from yet, still in grace
		}
		if inSync {
			desired = append(desired, r)
		}
	}
	b.replMu.Unlock()

	if sameNodeSet(desired, pv.ISR) {
		return
	}
	if err := b.ctrl.Apply(controller.Command{Type: controller.CmdChangeISR, Topic: topic, Partition: p, ISR: desired}); err != nil {
		// Transient leadership churn; the next tick retries.
		return
	}
}

// sameNodeSet reports whether a and b contain the same node ids (order-insensitive).
func sameNodeSet(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[int32]struct{}, len(a))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := seen[x]; !ok {
			return false
		}
	}
	return true
}

// replicationFactor is the configured RF clamped to [1, quorum size].
func (b *Broker) replicationFactor() int32 {
	rf := b.cfg.ReplicationFactor
	if rf < 1 {
		rf = 1
	}
	if n := int32(b.cluster.Size()); rf > n {
		rf = n
	}
	return rf
}

// reconcileFromFSM opens a local log for every partition this node replicates per the
// committed FSM placement. It is the follower's path to a present-but-unwritten .log
// (Phase 3's fetcher fills follower logs; produce only ever writes the leader's). Fired
// after each FSM apply/restore and once at SetController. Best-effort: a transient open
// failure is retried on the next apply.
func (b *Broker) reconcileFromFSM() {
	if b.ctrl == nil {
		return
	}
	self := b.cluster.Self()
	topics := b.ctrl.FSM().Topics()

	// follow records the fetcher action for a replicated (RF>1) partition: follow it when
	// another node leads, stop following (we lead) otherwise. Collected under the log lock
	// but applied after release so manager calls don't nest inside b.mu.
	type follow struct {
		topic string
		p     int32
		lead  bool
	}
	var actions []follow

	b.mu.Lock()
	for _, tv := range topics {
		for p := range tv.Parts {
			pv := tv.Parts[p]
			if !containsNode(pv.Replicas, self) {
				continue
			}
			l, err := b.openLocked(tv.Name, int32(p))
			if err != nil {
				continue
			}
			if len(pv.Replicas) > 1 {
				l.HoldHighWatermark() // committed offset is driven by replication, not append
				actions = append(actions, follow{tv.Name, int32(p), pv.Leader == self})
			}
		}
	}
	b.mu.Unlock()

	if b.repl == nil {
		return
	}
	for _, a := range actions {
		if a.lead {
			b.repl.StopFollowing(a.topic, a.p)
			b.noteLeadSince(a.topic, a.p)
		} else {
			b.repl.EnsureFollowing(a.topic, a.p)
		}
	}
}

// PartitionMeta returns the placement a Metadata response advertises for one partition:
// leader, leader-epoch, replica set and ISR. With a controller it reads the committed
// FSM state (falling back to the placement seed before the topic is committed locally);
// without one it reports the single-replica cluster placement.
func (b *Broker) PartitionMeta(topic string, p int32) (leader, epoch int32, replicas, isr []int32) {
	if b.ctrl != nil {
		if pv, ok := b.ctrl.FSM().Partition(topic, p); ok {
			return pv.Leader, pv.LeaderEpoch, pv.Replicas, pv.ISR
		}
	}
	leader = b.cluster.LeaderFor(topic, p)
	return leader, 0, []int32{leader}, []int32{leader}
}

// leads reports whether this node is the serving leader for (topic, partition): the FSM
// leader in controller mode, else the pure-function placement.
func (b *Broker) leads(topic string, p int32) bool {
	if b.ctrl != nil {
		pv, ok := b.ctrl.FSM().Partition(topic, p)
		return ok && pv.Leader == b.cluster.Self()
	}
	return b.cluster.IsLeader(topic, p)
}

// proposeCreate commits a CmdCreateTopic with np partitions (RF replica sets seeded from
// the cluster view) through the controller, then waits for the local FSM to reflect it and
// opens any logs this node now replicates. A non-positive np falls back to the configured
// default. An "already exists" race is treated as success.
func (b *Broker) proposeCreate(name string, np int32) error {
	if np <= 0 {
		np = b.cfg.NumPartitions
	}
	if np <= 0 {
		np = 1
	}
	rf := b.replicationFactor()
	replicas := make([][]int32, np)
	for p := int32(0); p < np; p++ {
		replicas[p] = b.cluster.ReplicasFor(name, p, rf)
	}
	err := b.ctrl.Apply(controller.Command{Type: controller.CmdCreateTopic, Topic: name, Replicas: replicas})
	if err != nil && !b.ctrl.FSM().HasTopic(name) {
		return err
	}
	b.waitTopic(name)
	b.reconcileFromFSM()
	return nil
}

// waitTopic blocks (bounded) until the local FSM reflects the named topic.
func (b *Broker) waitTopic(name string) {
	deadline := time.Now().Add(topicApplyWait)
	for time.Now().Before(deadline) {
		if b.ctrl.FSM().HasTopic(name) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// CreatePartitions grows an existing topic to newCount total partitions (partitions can
// only grow, matching Kafka). In cluster mode the added partitions' replica sets are seeded
// from the cluster view and committed through the controller (CmdCreatePartitions), so every
// broker converges and opens the new logs via reconcileFromFSM; the controller-free single
// node extends its catalog directly.
func (b *Broker) CreatePartitions(name string, newCount int32) error {
	if b.ctrl != nil {
		cur, ok := b.ctrl.FSM().Partitions(name)
		if !ok {
			return fmt.Errorf("topic %q does not exist", name)
		}
		if newCount <= cur {
			return fmt.Errorf("topic %q already has %d partitions", name, cur)
		}
		rf := b.replicationFactor()
		added := make([][]int32, 0, newCount-cur)
		for p := cur; p < newCount; p++ {
			added = append(added, b.cluster.ReplicasFor(name, p, rf))
		}
		if err := b.ctrl.Apply(controller.Command{Type: controller.CmdCreatePartitions, Topic: name, Replicas: added}); err != nil {
			return err
		}
		b.waitPartitions(name, newCount)
		b.reconcileFromFSM()
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, ok := b.catalog[name]
	if !ok {
		return fmt.Errorf("topic %q does not exist", name)
	}
	if newCount <= cur {
		return fmt.Errorf("topic %q already has %d partitions", name, cur)
	}
	return b.registerLocked(name, newCount)
}

// waitPartitions blocks (bounded) until the local FSM reflects at least want partitions for
// the topic (a follower applies asynchronously after the leader commits).
func (b *Broker) waitPartitions(name string, want int32) {
	deadline := time.Now().Add(topicApplyWait)
	for time.Now().Before(deadline) {
		if n, ok := b.ctrl.FSM().Partitions(name); ok && n >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func containsNode(ids []int32, id int32) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// AdvertisedHost/Port expose how clients should reconnect to this broker.
func (b *Broker) AdvertisedHost() string { return b.cfg.AdvertisedHost }
func (b *Broker) AdvertisedPort() int32  { return b.cfg.AdvertisedPort }

// SetAdvertised overrides the advertised host/port (used when binding an ephemeral
// port, e.g. in tests).
func (b *Broker) SetAdvertised(host string, port int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.AdvertisedHost = host
	b.cfg.AdvertisedPort = port
}

func (b *Broker) AutoCreate() bool { return b.cfg.AutoCreateTopics }
func (b *Broker) DataDir() string  { return b.cfg.LogDirs }

// NumPartitions returns the partition count for a topic. Known topics use their
// recorded count; unknown topics default to the cluster-wide configured count (which
// must be identical on every broker so placement stays consistent).
func (b *Broker) NumPartitions(topic string) int32 {
	if b.ctrl != nil {
		if n, ok := b.ctrl.FSM().Partitions(topic); ok {
			return n
		}
		return b.cfg.NumPartitions
	}
	b.mu.RLock()
	n, ok := b.catalog[topic]
	b.mu.RUnlock()
	if ok {
		return n
	}
	return b.cfg.NumPartitions
}

// KnownTopics returns the topics this broker is aware of, sorted by name. In a
// cluster this is each broker's local view (a broker learns a topic when it leads one
// of its partitions or serves a metadata request for it).
func (b *Broker) KnownTopics() []TopicInfo {
	if b.ctrl != nil {
		tvs := b.ctrl.FSM().Topics() // already sorted by name
		out := make([]TopicInfo, 0, len(tvs))
		for _, tv := range tvs {
			out = append(out, TopicInfo{Name: tv.Name, Partitions: tv.Partitions})
		}
		return out
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TopicInfo, 0, len(b.catalog))
	for name, n := range b.catalog {
		out = append(out, TopicInfo{Name: name, Partitions: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Knows reports whether the topic is in this broker's catalog.
func (b *Broker) Knows(topic string) bool {
	if b.ctrl != nil {
		return b.ctrl.FSM().HasTopic(topic)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.catalog[topic]
	return ok
}

// CreateTopic registers a topic with the given partition count, erroring if it already
// exists. In cluster mode the controller commits the count cluster-wide so every broker
// agrees; the controller-free single-node path honors the requested count directly. A
// non-positive count defaults to the configured partition count.
func (b *Broker) CreateTopic(name string, partitions int32) error {
	if b.ctrl != nil {
		if b.ctrl.FSM().HasTopic(name) {
			return fmt.Errorf("topic %q already exists", name)
		}
		return b.proposeCreate(name, partitions)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.catalog[name]; ok {
		return fmt.Errorf("topic %q already exists", name)
	}
	return b.registerLocked(name, partitions)
}

// EnsureTopic registers the topic if absent (used for auto-create), returning its
// partition count.
func (b *Broker) EnsureTopic(name string) (int32, error) {
	if b.ctrl != nil {
		if n, ok := b.ctrl.FSM().Partitions(name); ok {
			return n, nil
		}
		if err := b.proposeCreate(name, b.cfg.NumPartitions); err != nil {
			return 0, err
		}
		if n, ok := b.ctrl.FSM().Partitions(name); ok {
			return n, nil
		}
		return b.cfg.NumPartitions, nil
	}
	b.mu.RLock()
	n, ok := b.catalog[name]
	b.mu.RUnlock()
	if ok {
		return n, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n, ok := b.catalog[name]; ok {
		return n, nil
	}
	n = b.cfg.NumPartitions
	if err := b.registerLocked(name, n); err != nil {
		return 0, err
	}
	return n, nil
}

// LocalLog returns the storage log for (topic, partition), creating it on demand.
// It returns ErrNotLeader if this broker does not lead the partition, or
// ErrUnknownPartition if the index is out of range.
func (b *Broker) LocalLog(topic string, partition int32) (*storage.Log, error) {
	np := b.NumPartitions(topic)
	if partition < 0 || partition >= np {
		return nil, ErrUnknownPartition
	}
	if !b.leads(topic, partition) {
		return nil, ErrNotLeader
	}
	b.mu.RLock()
	if pm, ok := b.logs[topic]; ok {
		if l, ok := pm[partition]; ok {
			b.mu.RUnlock()
			return l, nil
		}
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	// In controller mode the topic is already committed in the FSM (leads() proved it),
	// so the log just needs opening; topics are never invented locally. Without a
	// controller, register the topic on first touch as before.
	if b.ctrl == nil {
		if _, ok := b.catalog[topic]; !ok {
			if err := b.registerLocked(topic, np); err != nil {
				return nil, err
			}
		}
	}
	return b.openLocked(topic, partition)
}

// FlushAll fsyncs every dirty local log.
func (b *Broker) FlushAll() error {
	for _, l := range b.snapshotLogs() {
		if err := l.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// EnforceRetention applies configured retention to every local log.
func (b *Broker) EnforceRetention() {
	if b.cfg.RetentionMs <= 0 && b.cfg.RetentionBytes <= 0 {
		return
	}
	for _, l := range b.snapshotLogs() {
		_ = l.EnforceRetention(b.cfg.RetentionMs, b.cfg.RetentionBytes)
	}
}

// Close stops replication, flushes, and closes every local log.
func (b *Broker) Close() error {
	b.replMu.Lock()
	stop, done := b.maintStop, b.maintDone
	b.maintStop = nil
	b.replMu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
	if b.repl != nil {
		b.repl.Close()
	}

	var firstErr error
	for _, l := range b.snapshotLogs() {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *Broker) snapshotLogs() []*storage.Log {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []*storage.Log
	for _, pm := range b.logs {
		for _, l := range pm {
			out = append(out, l)
		}
	}
	return out
}

// registerLocked records a topic in the catalog and opens logs for the partitions
// this broker leads. Caller holds the write lock.
func (b *Broker) registerLocked(name string, partitions int32) error {
	if partitions <= 0 {
		partitions = b.cfg.NumPartitions
	}
	b.catalog[name] = partitions
	for p := int32(0); p < partitions; p++ {
		if b.cluster.IsLeader(name, p) {
			if _, err := b.openLocked(name, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// openLocked opens (creating files if needed) the log for a led partition. Caller
// holds the write lock.
func (b *Broker) openLocked(topic string, partition int32) (*storage.Log, error) {
	if pm, ok := b.logs[topic]; ok {
		if l, ok := pm[partition]; ok {
			return l, nil
		}
	}
	l, err := storage.Open(b.partitionDir(topic, partition), b.logCfg)
	if err != nil {
		return nil, err
	}
	if b.logs[topic] == nil {
		b.logs[topic] = map[int32]*storage.Log{}
	}
	b.logs[topic][partition] = l
	return l, nil
}

// partitionDir is "<data-dir>/<topic>-<partition>".
func (b *Broker) partitionDir(topic string, p int32) string {
	return filepath.Join(b.cfg.LogDirs, fmt.Sprintf("%s-%d", topic, p))
}

// loadExisting scans the data dir for "<topic>-<partition>" directories (this node's
// led partitions) and reopens their logs, recovering each topic's partition count from
// the highest partition index on disk. This catalog is only consulted on the
// controller-free single-node path; in cluster mode the committed FSM count supersedes it.
func (b *Broker) loadExisting() error {
	entries, err := os.ReadDir(b.cfg.LogDirs)
	if err != nil {
		return err
	}
	maxPart := map[string]int32{}
	dirs := map[string][]int32{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "__") { // skip internal dirs like __offsets
			continue
		}
		i := strings.LastIndex(name, "-")
		if i < 0 {
			continue
		}
		pid, err := strconv.Atoi(name[i+1:])
		if err != nil {
			continue
		}
		topic := name[:i]
		dirs[topic] = append(dirs[topic], int32(pid))
		if int32(pid) > maxPart[topic] {
			maxPart[topic] = int32(pid)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, pids := range dirs {
		b.catalog[topic] = maxPart[topic] + 1
		for _, pid := range pids {
			if _, err := b.openLocked(topic, pid); err != nil {
				return err
			}
		}
	}
	return nil
}
