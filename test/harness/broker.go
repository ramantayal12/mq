//go:build functional

package harness

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Broker is a running mqbroker the tests can talk to: either a process this harness
// spawned, or an external one selected via MQ_FUNC_ADDR.
type Broker struct {
	Addr       string // Kafka bootstrap address
	MetricsURL string // Prometheus scrape URL ("" if unknown)

	cmd     *exec.Cmd // nil for an external broker
	dataDir string
}

// StartBroker returns a single-node broker. If MQ_FUNC_ADDR is set it targets that
// already-running broker (e.g. `docker compose up`); otherwise it builds and spawns one
// on free ephemeral ports with a throwaway data dir.
func StartBroker() (*Broker, error) {
	if addr := os.Getenv("MQ_FUNC_ADDR"); addr != "" {
		b := &Broker{Addr: addr, MetricsURL: os.Getenv("MQ_FUNC_METRICS_URL")}
		if !b.waitReady(30 * time.Second) {
			return nil, fmt.Errorf("external broker not ready at %s", addr)
		}
		return b, nil
	}

	bin, err := brokerBinary()
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "mq-node")
	if err != nil {
		return nil, err
	}
	kPort, mPort := freePort(), freePort()
	addr := fmt.Sprintf("127.0.0.1:%d", kPort)
	metrics := fmt.Sprintf("127.0.0.1:%d", mPort)

	cmd := exec.Command(bin)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"MQ_LISTENERS="+addr,
		"MQ_ADVERTISED_LISTENERS="+addr,
		"MQ_METRICS_ADDR="+metrics,
		"MQ_LOG_DIRS="+filepath.Join(dir, "data"),
		"MQ_NUM_PARTITIONS=6",
		"MQ_AUTO_CREATE_TOPICS=true",
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start broker: %w", err)
	}

	b := &Broker{
		Addr:       addr,
		MetricsURL: "http://" + metrics + "/metrics",
		cmd:        cmd,
		dataDir:    dir,
	}
	if !b.waitReady(30 * time.Second) {
		b.Stop()
		return nil, fmt.Errorf("broker not ready at %s", addr)
	}
	return b, nil
}

// Stop signals the broker to shut down and removes its data dir. No-op for an external
// broker.
func (b *Broker) Stop() {
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = b.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = b.cmd.Process.Kill()
		}
	}
	if b.dataDir != "" {
		os.RemoveAll(b.dataDir)
	}
}

// Client opens a franz-go client pointed at this broker.
func (b *Broker) Client(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(b.Addr)}, opts...)...)
}

func (b *Broker) waitReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.Addr))
	if err != nil {
		return false
	}
	defer cl.Close()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr := cl.Ping(ctx)
		cancel()
		if pingErr == nil && metricsReady(b.MetricsURL) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func metricsReady(url string) bool {
	if url == "" {
		return true // metrics check disabled
	}
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
