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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

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
	seed string
	dirs []string // per-node data directory (partition logs live directly under it)
	stop func()
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
	ctrls := make([]*controller.Controller, n)
	brokers := make([]*broker.Broker, n)
	var stops []func()

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
		stops = append(stops, func() { srv.Close(); coord.Close(); b.Close(); ctrl.Close() })
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

	_, seedPort, _ := net.SplitHostPort(lns[0].Addr().String())
	return replCluster{
		seed: "127.0.0.1:" + seedPort,
		dirs: dirs,
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
