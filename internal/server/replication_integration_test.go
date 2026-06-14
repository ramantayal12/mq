//go:build integration

package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/controller"
	"mq/internal/group"
)

// freeTCPPort grabs an ephemeral port and releases it for raft/rpc to rebind.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// replCluster is an n-node cluster whose placement is owned by a Raft controller quorum.
type replCluster struct {
	seed     string
	addrs    []string                 // per-node Kafka listener address
	dirs     []string                 // per-node data directory (partition logs live directly under it)
	ctrls    []*controller.Controller // per-node controller (for reading committed placement)
	stopNode func(i int)              // stop a single node (idempotent)
	stop     func()                   // stop every node
}

// startReplicatedCluster boots n brokers, each paired with a Raft controller, all wired
// into one quorum, with the given replication factor. Raft and forwarding-RPC use
// independent ephemeral ports; the Kafka listeners carry the cluster membership.
func startReplicatedCluster(t *testing.T, n int, rf int32, numPartitions int32) replCluster {
	t.Helper()

	// Bind all Kafka listeners first so every broker is told the full membership.
	lns := make([]net.Listener, n)
	specs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		_, portStr, _ := net.SplitHostPort(ln.Addr().String())
		specs[i] = fmt.Sprintf("%d@127.0.0.1:%s", i, portStr)
	}
	spec := strings.Join(specs, ",")

	// Raft + forwarding-RPC addresses are independent of the Kafka ports in tests.
	peers := make([]controller.Peer, n)
	for i := range peers {
		peers[i] = controller.Peer{
			NodeID:   int32(i),
			RaftAddr: fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t)),
			RPCAddr:  fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t)),
		}
	}

	dirs := make([]string, n)
	addrs := make([]string, n)
	ctrls := make([]*controller.Controller, n)
	brokers := make([]*broker.Broker, n)
	stops := make([]func(), n)

	for i := 0; i < n; i++ {
		cfg := config.Default()
		cfg.NodeID = int32(i)
		cfg.Brokers = spec
		cfg.LogDirs = t.TempDir()
		cfg.NumPartitions = numPartitions
		cfg.ReplicationFactor = rf
		dirs[i] = cfg.LogDirs

		cl, err := cluster.Parse(cfg.NodeID, cfg.Brokers, "127.0.0.1", 0)
		if err != nil {
			t.Fatal(err)
		}
		ctrl, err := controller.New(controller.Config{
			NodeID:    int32(i),
			RaftBind:  peers[i].RaftAddr,
			Advertise: peers[i].RaftAddr,
			RPCBind:   peers[i].RPCAddr,
			RaftDir:   filepath.Join(cfg.LogDirs, "raft"),
			Bootstrap: i == 0,
			Peers:     peers,
			LogOutput: io.Discard,
		}, controller.NewFSM())
		if err != nil {
			t.Fatal(err)
		}
		b, err := broker.New(cfg, cl)
		if err != nil {
			t.Fatal(err)
		}
		store, err := group.NewOffsetStore(cfg.LogDirs)
		if err != nil {
			t.Fatal(err)
		}
		coord := group.NewCoordinator(store)
		srv := NewFromListener(lns[i], b, coord)
		go srv.Serve()

		ctrls[i], brokers[i] = ctrl, b
		_, portStr, _ := net.SplitHostPort(lns[i].Addr().String())
		addrs[i] = "127.0.0.1:" + portStr
		var once sync.Once
		stops[i] = func() { once.Do(func() { srv.Close(); coord.Close(); b.Close(); ctrl.Close() }) }
	}

	// Wait for the quorum to elect a leader, then point every broker at its FSM.
	if !waitAnyLeader(ctrls, 15*time.Second) {
		for _, s := range stops {
			s()
		}
		t.Fatal("no raft leader elected")
	}
	for i := range brokers {
		brokers[i].SetController(ctrls[i])
	}

	return replCluster{
		seed:     addrs[0],
		addrs:    addrs,
		dirs:     dirs,
		ctrls:    ctrls,
		stopNode: func(i int) { stops[i]() },
		stop: func() {
			for _, s := range stops {
				s()
			}
		},
	}
}

func waitAnyLeader(ctrls []*controller.Controller, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range ctrls {
			if c.IsLeader() {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// TestReplicationPlacementRF3 proves Phase 2's goal: with RF=3 across 3 brokers the
// controller assigns every broker as a replica of every partition, and each broker opens
// the partition's log locally — so the same partition's directory exists on all three —
// while the leader path still serves a clean produce/consume round-trip.
func TestReplicationPlacementRF3(t *testing.T) {
	const n = 3
	const np = int32(3)
	rc := startReplicatedCluster(t, n, 3, np)
	defer rc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Producing auto-creates "rep" through the controller (the metadata handler proposes
	// the topic, forwarding to the leader if the request lands on a follower).
	prod := newClient(t, rc.seed, kgo.DefaultProduceTopic("rep"))
	const total = 60
	for i := 0; i < total; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("v%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	// RF=3 over 3 brokers ⇒ every node replicates every partition ⇒ each partition's log
	// directory must appear on all three brokers (followers via reconcile, eventually).
	for _, dir := range rc.dirs {
		for p := int32(0); p < np; p++ {
			want := filepath.Join(dir, fmt.Sprintf("rep-%d", p))
			if !eventuallyTrue(5*time.Second, func() bool { return dirExists(want) }) {
				t.Fatalf("replica log %s never opened", want)
			}
		}
	}

	// The leader path still serves: consume everything back.
	cons := newClient(t, rc.seed,
		kgo.ConsumeTopics("rep"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	got := 0
	for got < total && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	if got != total {
		t.Fatalf("consumed %d/%d from the RF=3 cluster", got, total)
	}
}

// TestCustomAndExpandablePartitionsRF3 proves Phase 6's goal in cluster mode: CreateTopics
// honors a custom partition count (≠ the configured default), CreatePartitions (API 37)
// grows the topic, and every broker in the quorum converges on the new count — opening a
// log for each partition it now replicates.
func TestCustomAndExpandablePartitionsRF3(t *testing.T) {
	const n = 3
	const def = int32(1) // configured default; the topic is created with a different count
	rc := startReplicatedCluster(t, n, 3, def)
	defer rc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := newClient(t, rc.seed)
	defer admin.Close()

	// Create "grow" with 4 partitions — different from the cluster default of 1.
	ctReq := kmsg.NewCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = "grow"
	ct.NumPartitions = 4
	ct.ReplicationFactor = 3
	ctReq.Topics = append(ctReq.Topics, ct)
	ctReq.TimeoutMillis = 5000
	ctResp, err := ctReq.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	if ec := ctResp.Topics[0].ErrorCode; ec != 0 {
		t.Fatalf("CreateTopics error code %d", ec)
	}

	// Every controller's FSM must converge on the custom count, and every broker must open
	// a log for each of the 4 partitions (RF=3 ⇒ all nodes replicate all partitions).
	assertConverged(t, rc, "grow", 4)

	// Grow "grow" to 6 partitions via CreatePartitions (API 37).
	cpReq := kmsg.NewCreatePartitionsRequest()
	cp := kmsg.NewCreatePartitionsRequestTopic()
	cp.Topic = "grow"
	cp.Count = 6
	cpReq.Topics = append(cpReq.Topics, cp)
	cpReq.TimeoutMillis = 5000
	cpResp, err := cpReq.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}
	if ec := cpResp.Topics[0].ErrorCode; ec != 0 {
		t.Fatalf("CreatePartitions error code %d", ec)
	}

	assertConverged(t, rc, "grow", 6)

	// The grown topic must serve: produce across all 6 partitions and read them back.
	prod := newClient(t, rc.seed)
	const total = 60
	for i := 0; i < total; i++ {
		prod.Produce(ctx, &kgo.Record{Topic: "grow", Value: []byte(fmt.Sprintf("v%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	cons := newClient(t, rc.seed, kgo.ConsumeTopics("grow"))
	defer cons.Close()
	got := 0
	for got < total && ctx.Err() == nil {
		cons.PollFetches(ctx).EachRecord(func(*kgo.Record) { got++ })
	}
	if got != total {
		t.Fatalf("consumed %d/%d from the grown topic", got, total)
	}
}

// assertConverged waits until every node's controller FSM reports want partitions for the
// topic and every node has opened a log directory for each partition.
func assertConverged(t *testing.T, rc replCluster, topic string, want int32) {
	t.Helper()
	for i := range rc.ctrls {
		ci := i
		if !eventuallyTrue(10*time.Second, func() bool {
			np, ok := rc.ctrls[ci].FSM().Partitions(topic)
			return ok && np == want
		}) {
			np, _ := rc.ctrls[ci].FSM().Partitions(topic)
			t.Fatalf("node %d FSM partition count = %d, want %d", ci, np, want)
		}
		for p := int32(0); p < want; p++ {
			dir := filepath.Join(rc.dirs[ci], fmt.Sprintf("%s-%d", topic, p))
			if !eventuallyTrue(10*time.Second, func() bool { return dirExists(dir) }) {
				t.Fatalf("node %d never opened partition log %s", ci, dir)
			}
		}
	}
}

func eventuallyTrue(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// partitionLogBytes sums the sizes of all .log segment files in a partition directory.
func partitionLogBytes(dir, topic string, p int32) int64 {
	pdir := filepath.Join(dir, fmt.Sprintf("%s-%d", topic, p))
	entries, err := os.ReadDir(pdir)
	if err != nil {
		return -1
	}
	var total int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			if fi, err := e.Info(); err == nil {
				total += fi.Size()
			}
		}
	}
	return total
}

// TestFollowerLogsConvergeRF3 is Phase 3's core: with RF=3, records produced to the leader
// are pulled by each follower's fetcher, so every broker's on-disk partition log converges
// to the same non-empty byte length (Phase 2 only opened empty follower dirs). Consuming
// then returns everything, proving the high watermark advanced as the ISR caught up.
func TestFollowerLogsConvergeRF3(t *testing.T) {
	const n = 3
	const np = int32(1)
	rc := startReplicatedCluster(t, n, 3, np)
	defer rc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prod := newClient(t, rc.seed, kgo.DefaultProduceTopic("conv"))
	const total = 50
	for i := 0; i < total; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("value-%03d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	// Every broker's log for partition 0 must converge to the same non-zero size.
	converged := eventuallyTrue(15*time.Second, func() bool {
		first := partitionLogBytes(rc.dirs[0], "conv", 0)
		if first <= 0 {
			return false
		}
		for _, dir := range rc.dirs[1:] {
			if partitionLogBytes(dir, "conv", 0) != first {
				return false
			}
		}
		return true
	})
	if !converged {
		var sizes []int64
		for _, dir := range rc.dirs {
			sizes = append(sizes, partitionLogBytes(dir, "conv", 0))
		}
		t.Fatalf("follower logs never converged; per-broker .log bytes = %v", sizes)
	}

	// The committed data is fully consumable (HWM reached the LEO once followers caught up).
	cons := newClient(t, rc.seed,
		kgo.ConsumeTopics("conv"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	got := 0
	for got < total && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	if got != total {
		t.Fatalf("consumed %d/%d after convergence", got, total)
	}
}

// TestAcksAllCommitsBeforeAckRF3 is the Phase 4 guarantee: with acks=all (franz-go's
// default), a successful produce response means the record is committed — its HWM has
// advanced past it. So immediately after Flush returns, every record must be consumable
// with NO convergence wait, since consumers are clamped to the HWM.
func TestAcksAllCommitsBeforeAckRF3(t *testing.T) {
	const n = 3
	const np = int32(1)
	rc := startReplicatedCluster(t, n, 3, np)
	defer rc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// AllISRAcks is franz-go's default; set it explicitly to make the intent of the test clear.
	prod := newClient(t, rc.seed,
		kgo.DefaultProduceTopic("acksall"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	const total = 50
	for i := 0; i < total; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("value-%03d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	// No eventuallyTrue/convergence wait here — acks=all already committed the data, so a
	// fresh consumer reading from the start must immediately see all of it.
	cons := newClient(t, rc.seed,
		kgo.ConsumeTopics("acksall"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	got := 0
	for got < total && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	if got != total {
		t.Fatalf("consumed %d/%d; acks=all did not commit before acking", got, total)
	}
}

// TestLeaderFailoverRF3 is Phase 5's guarantee: when the broker leading a partition is
// killed, the controller elects a surviving in-sync replica, clients re-route on the
// NOT_LEADER path, and no committed (acks=all) data is lost. We commit 50 records, kill the
// leader, wait for the FSM to name a new leader, then produce 25 more and consume all 75.
func TestLeaderFailoverRF3(t *testing.T) {
	const n = 3
	const np = int32(1)
	rc := startReplicatedCluster(t, n, 3, np)
	defer rc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Phase 1: commit 50 records with acks=all, so all three replicas are in-sync.
	prod := newClient(t, rc.seed,
		kgo.DefaultProduceTopic("fail"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	const first = 50
	for i := 0; i < first; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("a-%03d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	// Identify and kill the broker leading fail-0.
	pv, ok := rc.ctrls[0].FSM().Partition("fail", 0)
	if !ok || pv.Leader < 0 {
		t.Fatalf("no committed leader for fail-0")
	}
	oldLeader := pv.Leader
	survivor := (int(oldLeader) + 1) % n // stays up; used to read committed placement
	rc.stopNode(int(oldLeader))

	// The controller fails the partition over to a surviving ISR member.
	var newLeader int32
	if !eventuallyTrue(30*time.Second, func() bool {
		v, ok := rc.ctrls[survivor].FSM().Partition("fail", 0)
		if ok && v.Leader >= 0 && v.Leader != oldLeader {
			newLeader = v.Leader
			return true
		}
		return false
	}) {
		t.Fatalf("partition never failed over from dead leader %d", oldLeader)
	}

	// Surviving seed addresses (exclude the killed broker) so clients can still bootstrap.
	var seeds []string
	for i, a := range rc.addrs {
		if i != int(oldLeader) {
			seeds = append(seeds, a)
		}
	}

	// Phase 2: clients recover — produce 25 more to the new leader, still acks=all.
	prod2, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)},
		kgo.DefaultProduceTopic("fail"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	const second = 25
	for i := 0; i < second; i++ {
		prod2.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("b-%03d", i))}, nil)
	}
	if err := prod2.Flush(ctx); err != nil {
		t.Fatalf("flush after failover to leader %d: %v", newLeader, err)
	}
	prod2.Close()

	// Every distinct record — the 50 committed before the kill and the 25 after — must
	// survive. We assert on the distinct set, not the count: without an idempotent producer
	// (mq advertises no InitProducerID) franz-go may re-send a batch on the NOT_LEADER
	// re-route, so duplicates are expected at-least-once behavior; data *loss* is not.
	want := map[string]bool{}
	for i := 0; i < first; i++ {
		want[fmt.Sprintf("a-%03d", i)] = true
	}
	for i := 0; i < second; i++ {
		want[fmt.Sprintf("b-%03d", i)] = true
	}
	cons, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)},
		kgo.ConsumeTopics("fail"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	seen := map[string]bool{}
	for len(seen) < len(want) && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) { seen[string(r.Value)] = true })
	}
	if len(seen) != len(want) {
		t.Fatalf("consumed %d/%d distinct records after failover to leader %d (data lost)", len(seen), len(want), newLeader)
	}
}
