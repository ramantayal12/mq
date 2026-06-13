//go:build integration

package controller

import (
	"fmt"
	"io"
	"testing"
	"time"
)

// newRPCPeers builds n peers with both a raft and a forwarding-RPC address.
func newRPCPeers(t *testing.T, n int) []Peer {
	t.Helper()
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{
			NodeID:   int32(i),
			RaftAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			RPCAddr:  fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		}
	}
	return peers
}

// TestForwardFromFollower proves Phase 2's leader-forwarding RPC: a command applied on a
// follower is forwarded to the raft leader, committed there, and replicated to the quorum
// — even though only the leader may propose to the raft log.
func TestForwardFromFollower(t *testing.T) {
	const n = 3
	peers := newRPCPeers(t, n)

	ctrls := make([]*Controller, n)
	for i := 0; i < n; i++ {
		c, err := New(Config{
			NodeID:    int32(i),
			RaftBind:  peers[i].RaftAddr,
			Advertise: peers[i].RaftAddr,
			RPCBind:   peers[i].RPCAddr,
			RaftDir:   t.TempDir(),
			Bootstrap: i == 0,
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
			_ = c.Close()
		}
	}()

	leader := waitLeader(ctrls, 10*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Pick a non-leader and apply through it; Apply must forward to the leader.
	var follower *Controller
	for _, c := range ctrls {
		if !c.IsLeader() {
			follower = c
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	if err := follower.Apply(Command{Type: CmdCreateTopic, Topic: "fwd", Replicas: [][]int32{{0, 1, 2}}}); err != nil {
		t.Fatalf("forwarded apply failed: %v", err)
	}
	for i, c := range ctrls {
		if !eventually(3*time.Second, func() bool { return c.FSM().HasTopic("fwd") }) {
			t.Fatalf("node %d never saw the forwarded+committed topic", i)
		}
	}

	// The FSM's application error must propagate back through the forward path: a second
	// create of the same topic is rejected, and the follower receives that rejection.
	if err := follower.Apply(Command{Type: CmdCreateTopic, Topic: "fwd", Replicas: [][]int32{{0}}}); err == nil {
		t.Fatal("expected duplicate-topic error to propagate back through the forward path")
	}
}
