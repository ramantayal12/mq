//go:build functional

package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Cluster is a set of real mqbroker processes wired into one Raft-controlled quorum with
// a chosen replication factor. Used by the failover tests.
type Cluster struct {
	Addrs   []string // per-node Kafka addresses
	cmds    []*exec.Cmd
	dataDir string
}

// StartCluster builds the broker binary and spawns n nodes sharing one membership spec,
// each with rf replicas per partition. Node 0 bootstraps the Raft quorum. It blocks until
// the cluster serves a produce (controller elected, metadata ready).
func StartCluster(n int, rf, partitions int) (*Cluster, error) {
	bin, err := brokerBinary()
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "mq-cluster")
	if err != nil {
		return nil, err
	}

	// Allocate one non-overlapping Kafka/raft/rpc port triple per node, then build the
	// shared membership.
	bases := freeTriples(n)
	specs := make([]string, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", bases[i])
		specs[i] = fmt.Sprintf("%d@%s", i, addrs[i])
	}
	spec := strings.Join(specs, ",")

	c := &Cluster{Addrs: addrs, cmds: make([]*exec.Cmd, n), dataDir: root}
	for i := 0; i < n; i++ {
		// Disable the metrics endpoint so the three nodes don't fight over the default
		// :7080 (failover doesn't assert on metrics).
		cmd := exec.Command(bin, "--metrics-addr=")
		cmd.Dir = root
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("KAFKA_NODE_ID=%d", i),
			"KAFKA_BROKERS="+spec,
			"KAFKA_LISTENERS="+addrs[i],
			"KAFKA_ADVERTISED_LISTENERS="+addrs[i],
			fmt.Sprintf("KAFKA_REPLICATION_FACTOR=%d", rf),
			fmt.Sprintf("KAFKA_NUM_PARTITIONS=%d", partitions),
			fmt.Sprintf("KAFKA_RAFT_BOOTSTRAP=%t", i == 0),
			"KAFKA_AUTO_CREATE_TOPICS=true",
			"KAFKA_LOG_DIRS="+filepath.Join(root, fmt.Sprintf("node-%d", i)),
		)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			c.Stop()
			return nil, fmt.Errorf("start node %d: %w", i, err)
		}
		c.cmds[i] = cmd
	}

	if !c.waitServes(45 * time.Second) {
		c.Stop()
		return nil, fmt.Errorf("cluster did not become writable")
	}
	return c, nil
}

// Kill terminates node i (SIGKILL, no graceful flush) to simulate a hard broker failure.
func (c *Cluster) Kill(i int) {
	if i < len(c.cmds) && c.cmds[i] != nil && c.cmds[i].Process != nil {
		_ = c.cmds[i].Process.Kill()
		_, _ = c.cmds[i].Process.Wait()
		c.cmds[i] = nil
	}
}

// Stop terminates every still-running node and removes the data dirs.
func (c *Cluster) Stop() {
	for i := range c.cmds {
		if c.cmds[i] != nil && c.cmds[i].Process != nil {
			_ = c.cmds[i].Process.Signal(os.Interrupt)
		}
	}
	for i := range c.cmds {
		if c.cmds[i] != nil && c.cmds[i].Process != nil {
			done := make(chan struct{})
			go func(j int) { _ = c.cmds[j].Wait(); close(done) }(i)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = c.cmds[i].Process.Kill()
			}
		}
	}
	os.RemoveAll(c.dataDir)
}

// Client opens a client seeded with every surviving broker so it can route around a dead
// node even if that node was the original seed.
func (c *Cluster) Client(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(c.Addrs...)}, opts...)...)
}

// waitServes blocks until a probe produce succeeds, proving the controller elected a
// leader and metadata/placement are ready.
func (c *Cluster) waitServes(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	cl, err := c.Client(kgo.DefaultProduceTopic("__harness_probe"))
	if err != nil {
		return false
	}
	defer cl.Close()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := cl.ProduceSync(ctx, &kgo.Record{Value: []byte("ping")}).FirstErr()
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}
