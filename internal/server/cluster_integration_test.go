//go:build integration

package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/group"
)

// startCluster boots an n-node cluster of in-process brokers on ephemeral ports,
// wired with the same static membership, and returns one seed address.
func startCluster(t *testing.T, n int) (seed string, stop func()) {
	t.Helper()

	// Bind all listeners first so every broker can be told the full membership.
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

	var stops []func()
	for i := 0; i < n; i++ {
		cfg := config.Default()
		cfg.NodeID = int32(i)
		cfg.Brokers = spec
		cfg.LogDirs = t.TempDir()
		cfg.NumPartitions = int32(n) // spread partitions one-per-broker for the default topic

		cl, err := cluster.Parse(cfg.NodeID, cfg.Brokers, "127.0.0.1", 0)
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
		stops = append(stops, func() { srv.Close(); coord.Close(); b.Close() })
	}
	_, seedPort, _ := net.SplitHostPort(lns[0].Addr().String())
	return "127.0.0.1:" + seedPort, func() {
		for _, s := range stops {
			s()
		}
	}
}

func TestClusterProduceConsumeAcrossBrokers(t *testing.T) {
	seed, stop := startCluster(t, 3)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Produce across all partitions; each partition is led by a different broker, so
	// the client (seeded with one broker) must route writes to the right leaders
	// using the Metadata it discovers.
	prod := newClient(t, seed, kgo.DefaultProduceTopic("spread"))
	const n = 60
	for i := 0; i < n; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("v%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	cons := newClient(t, seed,
		kgo.ConsumeTopics("spread"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	total := 0
	for total < n && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { total++ })
	}
	if total != n {
		t.Fatalf("consumed %d/%d across the cluster", total, n)
	}
}

func TestClusterConsumerGroup(t *testing.T) {
	seed, stop := startCluster(t, 3)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	prod := newClient(t, seed, kgo.DefaultProduceTopic("cgtopic"))
	const n = 30
	for i := 0; i < n; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("m%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	// A consumer group spans partitions led by different brokers; FindCoordinator
	// must route all group calls to a single coordinator broker (chosen by hash),
	// while fetches still hit the per-partition leaders.
	cons := newClient(t, seed,
		kgo.ConsumeTopics("cgtopic"),
		kgo.ConsumerGroup("cg"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	got := 0
	for got < n && ctx.Err() == nil {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	if got != n {
		t.Fatalf("group consumed %d/%d", got, n)
	}
	if err := cons.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
