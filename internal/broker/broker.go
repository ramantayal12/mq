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
	mu      sync.RWMutex
	catalog map[string]int32                  // topic -> partition count (cluster-wide)
	logs    map[string]map[int32]*storage.Log // topic -> partition -> local log
}

// New constructs the broker over the given cluster view and loads topics already on
// disk (the led-partition directories under the data dir).
func New(cfg config.Config, cl *cluster.Cluster) (*Broker, error) {
	b := &Broker{
		cfg:     cfg,
		logCfg:  storage.Config{SegmentBytes: cfg.SegmentBytes, IndexIntervalBytes: cfg.IndexIntervalBytes},
		cluster: cl,
		catalog: map[string]int32{},
		logs:    map[string]map[int32]*storage.Log{},
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
	b.mu.Unlock()
	ctrl.FSM().SetOnApply(b.reconcileFromFSM)
	b.reconcileFromFSM() // adopt any state replayed/restored before the hook was registered
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
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, tv := range topics {
		for p := range tv.Parts {
			if containsNode(tv.Parts[p].Replicas, self) {
				_, _ = b.openLocked(tv.Name, int32(p))
			}
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

// proposeCreate commits a CmdCreateTopic (with RF replica sets seeded from the cluster
// view) through the controller, then waits for the local FSM to reflect it and opens any
// logs this node now replicates. An "already exists" race is treated as success.
func (b *Broker) proposeCreate(name string) error {
	np := b.cfg.NumPartitions
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

// CreateTopic registers a topic with the given partition count, erroring if it
// already exists. In a multi-broker cluster the count is forced to the cluster
// default so every broker agrees without a controller (custom counts need consensus,
// which is out of scope); a single-node cluster honors the requested count.
func (b *Broker) CreateTopic(name string, partitions int32) error {
	if b.ctrl != nil {
		if b.ctrl.FSM().HasTopic(name) {
			return fmt.Errorf("topic %q already exists", name)
		}
		return b.proposeCreate(name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.catalog[name]; ok {
		return fmt.Errorf("topic %q already exists", name)
	}
	return b.registerLocked(name, b.resolveCount(partitions))
}

// EnsureTopic registers the topic if absent (used for auto-create), returning its
// partition count.
func (b *Broker) EnsureTopic(name string) (int32, error) {
	if b.ctrl != nil {
		if n, ok := b.ctrl.FSM().Partitions(name); ok {
			return n, nil
		}
		if err := b.proposeCreate(name); err != nil {
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

// Close flushes and closes every local log.
func (b *Broker) Close() error {
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

// resolveCount picks the partition count to register: requested count only in a
// single-node cluster, otherwise the cluster default.
func (b *Broker) resolveCount(requested int32) int32 {
	if b.cluster.Size() == 1 && requested > 0 {
		return requested
	}
	return b.cfg.NumPartitions
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
// led partitions) and reopens their logs. In a single-node cluster the partition
// count is recovered from the highest partition index; in a multi-node cluster it is
// the cluster default (custom counts are not supported across brokers).
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
		count := b.cfg.NumPartitions
		if b.cluster.Size() == 1 {
			count = maxPart[topic] + 1
		}
		b.catalog[topic] = count
		for _, pid := range pids {
			if _, err := b.openLocked(topic, pid); err != nil {
				return err
			}
		}
	}
	return nil
}
