//go:build functional

// Package failover spins up a real 3-node, RF=3 mqbroker cluster, kills a broker
// mid-flight, and proves the cluster keeps serving: previously committed data is still
// readable and new writes succeed after the surviving nodes elect new partition leaders.
//
// These tests spawn their own cluster and ignore MQ_FUNC_ADDR.
package failover

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mq/test/harness"
)

// TestLeaderFailover produces a baseline, kills a broker, then verifies continuity:
// all baseline records remain readable and fresh writes are accepted post-failover.
func TestLeaderFailover(t *testing.T) {
	const (
		nodes      = 3
		rf         = 3
		partitions = 6
		topic      = "failover-topic"
		baseline   = 300
	)

	cl, err := harness.StartCluster(nodes, rf, partitions)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	defer cl.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Producer with acks=all (franz-go default) and generous retries so it rides through
	// the leader change instead of failing.
	prod, err := cl.Client(
		kgo.DefaultProduceTopic(topic),
		kgo.RecordRetries(20),
		kgo.ProduceRequestTimeout(10*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	// Baseline: produce and confirm committed before we kill anything.
	for i := 0; i < baseline; i++ {
		prod.Produce(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("baseline flush: %v", err)
	}

	// Kill a non-bootstrap broker to force partition-leadership failover for the
	// partitions it led, while the raft quorum (2 of 3) survives.
	cl.Kill(2)
	t.Log("killed broker 2; waiting for failover")

	// Give the controller's liveness loop time to detect the death and re-elect leaders
	// for the affected partitions (heartbeat timeout + grace).
	time.Sleep(8 * time.Second)

	// New writes must succeed against the surviving cluster.
	const postKill = 200
	var produceErrs int
	for i := baseline; i < baseline+postKill; i++ {
		err := prod.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte(fmt.Sprintf("v%d", i)),
		}).FirstErr()
		if err != nil {
			produceErrs++
		}
	}
	if produceErrs > 0 {
		t.Fatalf("%d/%d post-failover produces failed", produceErrs, postKill)
	}

	// Every record (baseline + post-kill) must be readable from the survivors. We track
	// distinct keys so at-least-once delivery (possible around failover) doesn't trip us.
	cons, err := cl.Client(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cons.Close()

	want := baseline + postKill
	seen := make(map[string]struct{}, want)
	deadline := time.Now().Add(45 * time.Second)
	for len(seen) < want && time.Now().Before(deadline) {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			// Transient metadata errors are expected right after a kill; keep polling.
			t.Logf("poll errors (continuing): %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { seen[string(r.Key)] = struct{}{} })
	}
	if len(seen) < want {
		t.Fatalf("after failover read %d/%d distinct keys", len(seen), want)
	}
}
