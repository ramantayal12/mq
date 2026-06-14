//go:build functional

// Package load drives sustained, real-time-shaped traffic at a broker. Its main purpose
// is to make the Grafana dashboards move: run it against a `docker compose up` broker
// (MQ_FUNC_ADDR=localhost:9092) and watch http://localhost:3005 while it runs.
//
//	MQ_FUNC_ADDR=localhost:9092 \
//	MQ_FUNC_METRICS_URL=http://localhost:7080/metrics \
//	LOAD_DURATION=2m \
//	  go test -tags functional -run TestSustainedLoad -v ./test/load/
package load

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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

// loadDuration is short by default so the suite stays fast; override with LOAD_DURATION
// (e.g. "2m") when you want to watch Grafana.
func loadDuration() time.Duration {
	if v := os.Getenv("LOAD_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 20 * time.Second
}

// TestSustainedLoad runs multiple producers and consumer groups against multiple topics
// with periodic bursts, then asserts meaningful traffic flowed and (when metrics are
// available) that the broker exported per-topic and consumer-group series.
func TestSustainedLoad(t *testing.T) {
	cfg := harness.DefaultLoad(loadDuration())
	t.Logf("running load: %d producers, %d groups, ~%d rec/s across %v for %v",
		cfg.Producers, cfg.Groups, cfg.RatePerSec, cfg.Topics, cfg.Duration)

	stats, err := harness.RunLoad(broker, cfg)
	if err != nil {
		t.Fatalf("run load: %v", err)
	}
	t.Logf("produced=%d consumed=%d errors=%d", stats.Produced, stats.Consumed, stats.Errors)

	if stats.Produced == 0 {
		t.Fatal("no records produced")
	}
	if stats.Consumed == 0 {
		t.Fatal("no records consumed")
	}
	if stats.Errors > stats.Produced/10 {
		t.Fatalf("too many errors: %d of %d produced", stats.Errors, stats.Produced)
	}

	if broker.MetricsURL == "" {
		t.Skip("metrics URL not configured; skipping metric assertions")
	}
	body, err := broker.Scrape()
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// Per-topic produce counters and the consumer-group lag series (wired in metrics)
	// must be present after a real workload.
	for _, topic := range cfg.Topics {
		if v := harness.CounterValue(body, fmt.Sprintf(`mq_produce_requests_total{topic="%s"}`, topic)); v <= 0 {
			t.Errorf("no produce metric for topic %s", topic)
		}
	}
	if !strings.Contains(body, "mq_consumer_group_lag") {
		t.Error("mq_consumer_group_lag series missing after consumer-group load")
	}
}
