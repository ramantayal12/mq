//go:build integration

// Package server integration tests drive the broker with the real franz-go Kafka
// client to prove wire-protocol compatibility. Run with: go test -tags integration ./...
package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/group"
)

// startBroker boots a broker on an ephemeral port and returns its address.
func startBroker(t *testing.T) (addr string, stop func()) {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.AdvertisedHost = "127.0.0.1"
	cfg.LogDirs = t.TempDir()
	cfg.NumPartitions = 3
	cfg.SegmentBytes = 4096 // force rolls during the test

	// Bind first so we can advertise the real ephemeral port (single-node cluster).
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		t.Fatal(err)
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	cfg.AdvertisedPort = int32(port)

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
	tcpAddr := ln.Addr().String()

	go srv.Serve()
	return tcpAddr, func() {
		srv.Close()
		coord.Close()
		b.Close()
	}
}

func newClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{kgo.SeedBrokers(addr)}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestProduceFetchRoundTrip(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()

	cl := newClient(t, addr, kgo.DefaultProduceTopic("events"))
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
	}
	wg.Wait()

	// Consume them back from the beginning.
	cons := newClient(t, addr,
		kgo.ConsumeTopics("events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()

	got := map[string]string{}
	for len(got) < n {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { got[string(r.Key)] = string(r.Value) })
		if ctx.Err() != nil {
			t.Fatalf("timed out, got %d/%d", len(got), n)
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if got[k] != fmt.Sprintf("value-%d", i) {
			t.Fatalf("key %s = %q", k, got[k])
		}
	}
}

func TestConsumerGroupCommitResume(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Produce 20 records.
	prod := newClient(t, addr, kgo.DefaultProduceTopic("orders"))
	for i := 0; i < 20; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("o%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	// First consumer in group "g1" reads 10 and commits.
	c1 := newClient(t, addr,
		kgo.ConsumeTopics("orders"),
		kgo.ConsumerGroup("g1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	read := 0
	for read < 10 {
		fs := c1.PollRecords(ctx, 10-read)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("c1 poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { read++ })
	}
	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	c1.Close()

	// Second consumer in the same group resumes after the committed offset.
	c2 := newClient(t, addr,
		kgo.ConsumeTopics("orders"),
		kgo.ConsumerGroup("g1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer c2.Close()
	remaining := 0
	deadline := time.Now().Add(10 * time.Second)
	for remaining < 10 && time.Now().Before(deadline) {
		fs := c2.PollRecords(ctx, 10)
		fs.EachRecord(func(*kgo.Record) { remaining++ })
	}
	if remaining != 10 {
		t.Fatalf("resume read %d records, want 10", remaining)
	}
}

func TestListAndDescribeGroups(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Produce a few records, then run a consumer group so there's a Stable group.
	prod := newClient(t, addr, kgo.DefaultProduceTopic("ev"))
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte("v")}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	cons := newClient(t, addr,
		kgo.ConsumeTopics("ev"),
		kgo.ConsumerGroup("grp"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	got := 0
	for got < 5 {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
		if ctx.Err() != nil {
			t.Fatal("timed out joining group")
		}
	}

	// ListGroups must include the group.
	lreq := kmsg.NewListGroupsRequest()
	lresp, err := lreq.RequestWith(ctx, cons)
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	found := false
	for _, g := range lresp.Groups {
		if g.Group == "grp" {
			found = true
		}
	}
	if !found {
		t.Fatalf("group grp not listed: %+v", lresp.Groups)
	}

	// DescribeGroups must report it Stable with one member.
	dreq := kmsg.NewDescribeGroupsRequest()
	dreq.Groups = []string{"grp"}
	dresp, err := dreq.RequestWith(ctx, cons)
	if err != nil {
		t.Fatalf("describe groups: %v", err)
	}
	if len(dresp.Groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(dresp.Groups))
	}
	g := dresp.Groups[0]
	if g.Group != "grp" {
		t.Fatalf("group id = %q", g.Group)
	}
	if g.State != "Stable" {
		t.Fatalf("state = %q, want Stable", g.State)
	}
	if len(g.Members) != 1 {
		t.Fatalf("want 1 member, got %d", len(g.Members))
	}
}

func TestListOffsets(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl := newClient(t, addr, kgo.DefaultProduceTopic("t1"))
	defer cl.Close()
	for i := 0; i < 5; i++ {
		cl.Produce(ctx, &kgo.Record{Value: []byte("x"), Partition: 0}, nil)
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// A fresh start-offset consumer should see all 5.
	cons := newClient(t, addr,
		kgo.ConsumeTopics("t1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()
	total := 0
	deadline := time.Now().Add(8 * time.Second)
	for total < 5 && time.Now().Before(deadline) {
		fs := cons.PollFetches(ctx)
		fs.EachRecord(func(*kgo.Record) { total++ })
	}
	if total != 5 {
		t.Fatalf("read %d want 5", total)
	}
}

func TestListOffsetsByTimestamp(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	cl := newClient(t, addr, kgo.DefaultProduceTopic("ts"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	defer cl.Close()
	// Flush per record so each lands in its own batch (timestamp seek is per-batch):
	// offsets 0,1,2 at base+0s, base+10s, base+20s.
	for i := 0; i < 3; i++ {
		cl.Produce(ctx, &kgo.Record{
			Value:     []byte("x"),
			Partition: 0,
			Timestamp: base.Add(time.Duration(i*10) * time.Second),
		}, nil)
		if err := cl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Seek to base+15s -> first record at/after it is offset 2 (base+20s).
	req := kmsg.NewListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = "ts"
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = 0
	rp.Timestamp = base.Add(15 * time.Second).UnixMilli()
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	got := resp.Topics[0].Partitions[0]
	if got.ErrorCode != 0 {
		t.Fatalf("error code %d", got.ErrorCode)
	}
	if got.Offset != 2 {
		t.Fatalf("offset=%d want 2", got.Offset)
	}
}
