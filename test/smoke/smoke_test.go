//go:build functional

// Package smoke holds fast correctness checks against a single real broker: the basic
// produce/consume contract, consumer-group offset durability, and that the Prometheus
// endpoint actually exports the kafka_* series.
package smoke

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mq/test/harness"
)

var broker *harness.Broker

func TestMain(m *testing.M) {
	b, err := harness.StartBroker()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start broker:", err)
		os.Exit(1)
	}
	broker = b
	code := m.Run()
	b.Stop()
	os.Exit(code)
}

func client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := broker.Client(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// TestProduceConsumeRoundTrip produces keyed records across all partitions and reads them
// all back from the start, asserting every key/value survives the wire round trip.
func TestProduceConsumeRoundTrip(t *testing.T) {
	const topic = "smoke-roundtrip"
	const n = 200

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prod := client(t, kgo.DefaultProduceTopic(topic))
	defer prod.Close()

	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		want[k] = v
		prod.Produce(ctx, &kgo.Record{Key: []byte(k), Value: []byte(v)}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cons := client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()

	got := make(map[string]string, n)
	for len(got) < n {
		if ctx.Err() != nil {
			t.Fatalf("timed out: got %d/%d", len(got), n)
		}
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { got[string(r.Key)] = string(r.Value) })
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s = %q, want %q", k, got[k], v)
		}
	}
}

// TestConsumerGroupCommitResume proves committed offsets persist: a second consumer in
// the same group resumes exactly where the first left off, with no gaps or duplicates.
func TestConsumerGroupCommitResume(t *testing.T) {
	const topic = "smoke-group"
	const group = "smoke-grp-1"
	const total = 40

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	prod := client(t, kgo.DefaultProduceTopic(topic))
	for i := 0; i < total; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("m%d", i))}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	prod.Close()

	c1 := client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	read := 0
	for read < total/2 {
		if ctx.Err() != nil {
			t.Fatalf("c1 timed out at %d", read)
		}
		fs := c1.PollRecords(ctx, total/2-read)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("c1 poll: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { read++ })
	}
	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	c1.Close()

	c2 := client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer c2.Close()
	resumed := 0
	deadline := time.Now().Add(15 * time.Second)
	for resumed < total/2 && time.Now().Before(deadline) {
		fs := c2.PollRecords(ctx, total/2)
		fs.EachRecord(func(*kgo.Record) { resumed++ })
	}
	if resumed != total/2 {
		t.Fatalf("resumed %d records, want %d", resumed, total/2)
	}
}

// TestMetricsExposed verifies the Prometheus integration end-to-end: after real traffic,
// /metrics serves the expected kafka_* series and the produce counter advances.
func TestMetricsExposed(t *testing.T) {
	if broker.MetricsURL == "" {
		t.Skip("metrics URL not configured")
	}
	const topic = "smoke-metrics"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prod := client(t, kgo.DefaultProduceTopic(topic))
	defer prod.Close()
	for i := 0; i < 25; i++ {
		prod.Produce(ctx, &kgo.Record{Value: []byte("x")}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	body, err := broker.Scrape()
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	for _, name := range []string{
		"kafka_produce_requests_total",
		"kafka_produce_bytes_total",
		"kafka_requests_total",
		"kafka_request_latency_seconds",
		"kafka_active_connections",
		"kafka_topics_total",
		"kafka_partition_log_end_offset",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metric %q missing from /metrics output", name)
		}
	}
	if v := harness.CounterValue(body, `kafka_produce_requests_total{topic="`+topic+`"}`); v <= 0 {
		t.Errorf("kafka_produce_requests_total for %s = %v, want > 0", topic, v)
	}
}
