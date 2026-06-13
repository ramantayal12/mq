//go:build integration

package controller

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// freePort grabs an ephemeral TCP port and releases it for the raft transport to reuse.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitLeader(ctrls []*Controller, timeout time.Duration) *Controller {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range ctrls {
			if c != nil && c.IsLeader() {
				return c
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// TestControllerFailover proves Phase 1's goal: a 3-node raft quorum elects a
// controller, a metadata mutation committed on the leader replicates to every node, and
// after the leader dies a new controller is elected that still holds the committed
// metadata.
func TestControllerFailover(t *testing.T) {
	const n = 3
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{NodeID: int32(i), RaftAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
	}

	ctrls := make([]*Controller, n)
	for i := 0; i < n; i++ {
		c, err := New(Config{
			NodeID:    int32(i),
			RaftBind:  peers[i].RaftAddr,
			Advertise: peers[i].RaftAddr,
			RaftDir:   t.TempDir(),
			Bootstrap: i == 0, // only one broker forms the initial quorum
			Peers:     peers,
			LogOutput: io.Discard,
		}, NewFSM())
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		ctrls[i] = c
	}
	defer func() {
		for _, c := range ctrls {
			if c != nil {
				_ = c.Close()
			}
		}
	}()

	leader := waitLeader(ctrls, 10*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// A mutation committed on the leader must replicate to every node.
	if err := leader.Apply(Command{Type: CmdCreateTopic, Topic: "t", Replicas: [][]int32{{0, 1, 2}}}); err != nil {
		t.Fatalf("apply on leader: %v", err)
	}
	for i, c := range ctrls {
		if !eventually(3*time.Second, func() bool { return c.FSM().HasTopic("t") }) {
			t.Fatalf("node %d never replicated the committed topic", i)
		}
	}

	// Kill the leader; a survivor must take over and still hold the metadata.
	leaderID, _ := leader.LeaderID()
	if err := leader.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	for i := range ctrls {
		if ctrls[i] == leader {
			ctrls[i] = nil
		}
	}

	newLeader := waitLeader(ctrls, 15*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after controller death")
	}
	if newID, _ := newLeader.LeaderID(); newID == leaderID {
		t.Fatalf("new leader id %d equals the dead leader %d", newID, leaderID)
	}
	if !newLeader.FSM().HasTopic("t") {
		t.Fatal("committed metadata lost after controller failover")
	}
	t.Logf("failover ok: leader %d died, node %d took over with metadata intact", leaderID, mustLeaderID(t, newLeader))
}

func mustLeaderID(t *testing.T, c *Controller) int32 {
	t.Helper()
	id, ok := c.LeaderID()
	if !ok {
		t.Fatal("leader id not available")
	}
	return id
}
