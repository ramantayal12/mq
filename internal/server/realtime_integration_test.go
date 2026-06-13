//go:build integration

package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/group"
)

// startServer boots a single in-process broker and returns the *Server (so tests can
// read its request counters) plus its address.
func startServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	cfg := config.Default()
	cfg.LogDirs = t.TempDir()
	cfg.NumPartitions = 3
	cl := cluster.Single(0, "127.0.0.1", int32(port))
	b, err := broker.New(cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	store, err := group.NewOffsetStore(cfg.LogDirs)
	if err != nil {
		t.Fatal(err)
	}
	coord := group.NewCoordinator(store)
	srv := NewFromListener(ln, b, coord)
	go srv.Serve()
	return srv, ln.Addr().String(), func() { srv.Close(); coord.Close(); b.Close() }
}

// Probe: how hard does an idle consumer hammer the broker? Real Kafka long-polls a
// fetch up to max.wait.ms; mq returns immediately, so an idle consumer spins.
func TestRT_IdleFetchBusyPoll(t *testing.T) {
	srv, addr, stop := startServer(t)
	defer stop()

	// Seed one record so the topic exists, then let the consumer sit idle at the end.
	p := newClient(t, addr, kgo.DefaultProduceTopic("idle"))
	p.Produce(context.Background(), &kgo.Record{Value: []byte("x")}, nil)
	p.Flush(context.Background())
	p.Close()

	cons := newClient(t, addr, kgo.ConsumeTopics("idle"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cons.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cons.PollFetches(ctx) // drain the one record; now caught up / idle

	before := srv.Requests(1) // Fetch = api key 1
	idle := 2 * time.Second
	time.Sleep(idle)
	after := srv.Requests(1)

	rate := float64(after-before) / idle.Seconds()
	t.Logf("idle Fetch requests over %v: %d  (%.0f fetches/sec while no data)", idle, after-before, rate)
	if rate > 50 {
		t.Logf("GAP CONFIRMED: no long-poll — idle consumer busy-polls at %.0f req/s (Kafka would issue ~1 per max.wait.ms)", rate)
	}
}

// warmupSentinel is the value of the single record produced to pre-create a topic's
// partitions before consumers join. It is NOT part of the evt-0..evt-(n-1) stream and
// must be excluded from consumer tallies, otherwise it occupies one of the n target
// slots and the driver loop can stop one real message short (spurious MESSAGE LOSS).
const warmupSentinel = "evt--1"

// streamProduce pushes n keyed records ("evt-<i>") into topic at a steady rate to
// simulate a real-time stream. Values are unique so consumers can detect loss/dups.
func streamProduce(t *testing.T, seed, topic string, n int, over time.Duration) {
	t.Helper()
	cl := newClient(t, seed, kgo.DefaultProduceTopic(topic))
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), over+10*time.Second)
	defer cancel()
	gap := over / time.Duration(n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("k%d", i%50)),
			Value: []byte(fmt.Sprintf("evt-%d", i)),
		}, func(_ *kgo.Record, err error) {
			wg.Done()
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
		if gap > 0 {
			time.Sleep(gap)
		}
	}
	wg.Wait()
}

// groupConsumer runs one consumer instance in a group, recording every value it sees
// into counts (guarded by mu) and its own tally into perConsumer[id].
func groupConsumer(ctx context.Context, t *testing.T, seed, topic, group string, id int,
	mu *sync.Mutex, counts map[string]int, perConsumer map[int]int, total *int64, target int64) {
	cl := newClient(t, seed,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cl.Close()
	for atomic.LoadInt64(total) < target && ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded && e.Err != context.Canceled {
					t.Logf("consumer %d poll error: %v", id, e.Err)
				}
			}
		}
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == warmupSentinel { // topic-creation warmup, not part of the stream
				return
			}
			mu.Lock()
			if counts[string(r.Value)] == 0 {
				atomic.AddInt64(total, 1)
			}
			counts[string(r.Value)]++
			perConsumer[id]++
			mu.Unlock()
		})
	}
}

// Point 1+2+3+4: multi-partition topic on a multi-broker cluster, one real-time
// producer, and a consumer GROUP with several concurrent consumer instances. Asserts
// no message loss and reports the partition split (consumer concurrency) and dups.
func TestRT_GroupConcurrencyNoLoss(t *testing.T) {
	seed, stop := startCluster(t, 3) // 3 brokers, NumPartitions=3 by default
	defer stop()

	const topic = "rt-stream"
	const n = 600
	const consumers = 3

	// Pre-create so all partitions exist before consumers join.
	admin := newClient(t, seed, kgo.DefaultProduceTopic(topic))
	admin.Produce(context.Background(), &kgo.Record{Value: []byte(warmupSentinel)}, nil) // warmup; excluded from tallies in groupConsumer
	admin.Flush(context.Background())
	admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	mu := &sync.Mutex{}
	counts := map[string]int{}
	perConsumer := map[int]int{}
	var total int64

	var wg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			groupConsumer(ctx, t, seed, topic, "rt-group", id, mu, counts, perConsumer, &total, n)
		}(c)
	}

	// Stream the data while consumers are live.
	streamProduce(t, seed, topic, n, 2*time.Second)

	// Wait for all to be consumed (or timeout).
	deadline := time.Now().Add(25 * time.Second)
	for atomic.LoadInt64(&total) < n && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	missing, dups := 0, 0
	for i := 0; i < n; i++ {
		c := counts[fmt.Sprintf("evt-%d", i)]
		if c == 0 {
			missing++
		} else if c > 1 {
			dups++
		}
	}
	t.Logf("consumed unique=%d/%d, missing=%d, dups=%d", total, n, missing, dups)
	t.Logf("per-consumer tallies (concurrency split): %v", perConsumer)
	active := 0
	for _, v := range perConsumer {
		if v > 0 {
			active++
		}
	}
	t.Logf("consumer instances that received partitions: %d/%d", active, consumers)
	if missing > 0 {
		t.Fatalf("MESSAGE LOSS: %d of %d messages never consumed", missing, n)
	}
}

// Point 4 (fan-out sense): two independent groups each receive the full stream.
func TestRT_FanoutIndependentGroups(t *testing.T) {
	seed, stop := startCluster(t, 3)
	defer stop()
	const topic = "rt-fanout"
	const n = 300

	streamProduce(t, seed, topic, n, 0) // burst

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consumeAll := func(group string) int64 {
		mu := &sync.Mutex{}
		counts := map[string]int{}
		per := map[int]int{}
		var total int64
		gctx, gcancel := context.WithTimeout(ctx, 15*time.Second)
		defer gcancel()
		groupConsumer(gctx, t, seed, topic, group, 0, mu, counts, per, &total, n)
		return total
	}

	a := consumeAll("group-A")
	b := consumeAll("group-B")
	t.Logf("group-A got %d, group-B got %d (each should get %d)", a, b, n)
	if a != n || b != n {
		t.Fatalf("fan-out failed: A=%d B=%d want %d each", a, b, n)
	}
}

// Point 2 (rebalance): a consumer dies mid-stream; the survivor must take over its
// partitions and the group must still consume everything.
func TestRT_RebalanceOnConsumerDeath(t *testing.T) {
	seed, stop := startCluster(t, 3)
	defer stop()
	const topic = "rt-rebal"
	const n = 400

	streamProduce(t, seed, topic, n, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	mu := &sync.Mutex{}
	counts := map[string]int{}
	per := map[int]int{}
	var total int64

	// Consumer 0 runs to completion.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); groupConsumer(ctx, t, seed, topic, "rt-rg", 0, mu, counts, per, &total, n) }()

	// Consumer 1 runs briefly then dies.
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	wg.Add(1)
	go func() { defer wg.Done(); groupConsumer(dctx, t, seed, topic, "rt-rg", 1, mu, counts, per, &total, n) }()
	_ = dcancel

	deadline := time.Now().Add(35 * time.Second)
	for atomic.LoadInt64(&total) < n && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	missing := 0
	for i := 0; i < n; i++ {
		if counts[fmt.Sprintf("evt-%d", i)] == 0 {
			missing++
		}
	}
	t.Logf("after consumer death: consumed=%d/%d missing=%d per=%v", total, n, missing, per)
	if missing > 0 {
		t.Fatalf("REBALANCE LOSS: %d messages lost after a consumer died", missing)
	}
}
