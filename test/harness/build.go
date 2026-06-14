//go:build functional

// Package harness provides the shared building blocks for the functional test suite:
// compiling and spawning real mqbroker processes (single-node and clustered), generating
// sustained real-time load, and scraping Prometheus metrics. It deliberately avoids the
// testing package so it can be reused from any test package under test/.
package harness

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
)

// brokerBinary compiles ./cmd/mqbroker once per test run and returns the binary path.
func brokerBinary() (string, error) {
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mq-bin")
		if err != nil {
			buildErr = err
			return
		}
		bin := filepath.Join(dir, "mqbroker")
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/mqbroker")
		cmd.Dir = repoRoot()
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("build mqbroker: %w", err)
			return
		}
		builtBin = bin
	})
	return builtBin, buildErr
}

// repoRoot returns the module root, derived from this source file's location
// (test/harness/build.go -> ../..).
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

// freePort returns an ephemeral TCP port that was free at call time.
func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// freeTriples returns n base ports, each a P where P, P+1 and P+2 are free — and where no
// port in any node's {P, P+1, P+2} window collides with another node's. A cluster node uses
// P for Kafka, P+1 for raft and P+2 for the forwarding RPC (the broker derives the latter
// two from the Kafka port), so the windows must not overlap. Every reserved port is held
// open until all n are found, then released for the brokers to rebind.
func freeTriples(n int) []int {
	var held []net.Listener
	defer func() {
		for _, l := range held {
			l.Close()
		}
	}()
	occupied := map[int]bool{}
	bases := make([]int, 0, n)
	for len(bases) < n {
		l0, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		base := l0.Addr().(*net.TCPAddr).Port
		if occupied[base] || occupied[base+1] || occupied[base+2] {
			l0.Close()
			continue
		}
		l1, e1 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+1))
		l2, e2 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+2))
		if e1 != nil || e2 != nil {
			l0.Close()
			if e1 == nil {
				l1.Close()
			}
			if e2 == nil {
				l2.Close()
			}
			continue
		}
		held = append(held, l0, l1, l2)
		occupied[base], occupied[base+1], occupied[base+2] = true, true, true
		bases = append(bases, base)
	}
	return bases
}
