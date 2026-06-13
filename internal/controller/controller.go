package controller

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// applyTimeout bounds a single leader-local raft.Apply.
const applyTimeout = 10 * time.Second

// errNotLeader is returned when a proposal reaches a node that is not the raft leader
// (e.g. leadership changed between resolving the leader and the forwarded RPC arriving).
// The caller may retry against the new leader.
var errNotLeader = errors.New("controller: not leader")

// errForwardUnavailable wraps the transient leadership/transport failures the forwarding
// path may hit (no leader known yet, dial refused, target no longer leader). Apply retries
// while it sees this; a genuine FSM rejection (e.g. duplicate topic) is returned as-is.
var errForwardUnavailable = errors.New("controller: leader unavailable")

// Peer is one broker's identity in the raft quorum.
type Peer struct {
	NodeID   int32
	RaftAddr string // host:port for the raft transport (client port + 1)
	RPCAddr  string // host:port for the leader-forwarding RPC (client port + 2)
}

// Config configures a controller. Port math (client port +1/+2) is resolved by the
// caller so this package stays decoupled from the Kafka listener.
type Config struct {
	NodeID    int32
	RaftBind  string    // bind address for the raft TCP transport
	Advertise string    // advertised raft address (defaults to RaftBind)
	RPCBind   string    // bind address for the leader-forwarding RPC; empty disables it
	RaftDir   string    // raft data directory (created if absent), e.g. <log-dirs>/raft
	Bootstrap bool      // form a fresh quorum from Peers (only when no existing state)
	Peers     []Peer    // every broker's raft server; used only when bootstrapping
	LogOutput io.Writer // raft + transport log sink; defaults to os.Stderr
}

// Controller wraps a *raft.Raft and the metadata FSM it replicates.
type Controller struct {
	nodeID  int32
	raft    *raft.Raft
	fsm     *FSM
	trans   *raft.NetworkTransport
	store   *raftboltdb.BoltStore
	rpcLn   net.Listener     // leader-forwarding RPC listener (nil when RPCBind is empty)
	rpcAddr map[int32]string // node id -> forwarding RPC address (from Peers)
	rpcWG   sync.WaitGroup
}

// New constructs and starts the controller. When cfg.Bootstrap is set and no prior raft
// state exists on disk, it bootstraps a new quorum from cfg.Peers; otherwise it starts
// and rejoins the existing quorum recorded in its store.
func New(cfg Config, fsm *FSM) (*Controller, error) {
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.Advertise == "" {
		cfg.Advertise = cfg.RaftBind
	}
	if err := os.MkdirAll(cfg.RaftDir, 0o755); err != nil {
		return nil, fmt.Errorf("controller: mkdir raft dir: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "store.db"))
	if err != nil {
		return nil, fmt.Errorf("controller: open bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.RaftDir, 2, cfg.LogOutput)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("controller: snapshot store: %w", err)
	}
	advAddr, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("controller: resolve advertise addr %q: %w", cfg.Advertise, err)
	}
	trans, err := raft.NewTCPTransport(cfg.RaftBind, advAddr, 3, 10*time.Second, cfg.LogOutput)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("controller: raft transport: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(itoa(cfg.NodeID))
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Output: cfg.LogOutput, Level: hclog.Warn})

	hadState, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		trans.Close()
		store.Close()
		return nil, fmt.Errorf("controller: probe existing state: %w", err)
	}

	r, err := raft.NewRaft(rc, fsm, store, store, snaps, trans)
	if err != nil {
		trans.Close()
		store.Close()
		return nil, fmt.Errorf("controller: new raft: %w", err)
	}

	if cfg.Bootstrap && !hadState {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(itoa(p.NodeID)),
				Address:  raft.ServerAddress(p.RaftAddr),
			})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			_ = r.Shutdown().Error()
			trans.Close()
			store.Close()
			return nil, fmt.Errorf("controller: bootstrap: %w", err)
		}
	}

	c := &Controller{nodeID: cfg.NodeID, raft: r, fsm: fsm, trans: trans, store: store, rpcAddr: map[int32]string{}}
	for _, p := range cfg.Peers {
		if p.RPCAddr != "" {
			c.rpcAddr[p.NodeID] = p.RPCAddr
		}
	}
	if cfg.RPCBind != "" {
		if err := c.startRPC(cfg.RPCBind); err != nil {
			_ = r.Shutdown().Error()
			trans.Close()
			store.Close()
			return nil, fmt.Errorf("controller: start forwarding rpc: %w", err)
		}
	}
	return c, nil
}

// FSM returns the replicated metadata state machine.
func (c *Controller) FSM() *FSM { return c.fsm }

// IsLeader reports whether this controller is the current raft leader.
func (c *Controller) IsLeader() bool { return c.raft.State() == raft.Leader }

// LeaderID returns the node id of the current raft leader, if one is known.
func (c *Controller) LeaderID() (int32, bool) {
	_, id := c.raft.LeaderWithID()
	if id == "" {
		return 0, false
	}
	n, err := strconv.Atoi(string(id))
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// WaitForLeader blocks until the quorum has a leader or the timeout elapses, reporting
// whether a leader emerged.
func (c *Controller) WaitForLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.raft.Leader() != "" {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c.raft.Leader() != ""
}

// Apply commits one metadata mutation through raft. On the leader it proposes directly;
// on a follower it forwards the command to the current leader's forwarding RPC. Transient
// leadership churn (no leader yet, leader just moved) is retried until applyTimeout, so a
// request that arrives during an election still lands; a genuine FSM rejection returns at
// once. Either way the returned error reflects the FSM's application result.
func (c *Controller) Apply(cmd Command) error {
	deadline := time.Now().Add(applyTimeout)
	for {
		if c.raft.State() == raft.Leader {
			return c.proposeLocal(cmd)
		}
		err := c.forwardToLeader(cmd)
		if err == nil || !errors.Is(err, errForwardUnavailable) {
			return err
		}
		if !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// proposeLocal applies a command through raft on this node, which must be the leader.
// It is the single funnel both Apply (leader branch) and the forwarding RPC server use,
// so a forwarded command never re-forwards (no proposal loops).
func (c *Controller) proposeLocal(cmd Command) error {
	if c.raft.State() != raft.Leader {
		return errNotLeader
	}
	data, err := cmd.encode()
	if err != nil {
		return err
	}
	f := c.raft.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp, ok := f.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

// Close shuts the controller down: it stops the forwarding RPC, then raft, then closes
// the transport and store.
func (c *Controller) Close() error {
	var firstErr error
	if c.rpcLn != nil {
		_ = c.rpcLn.Close()
		c.rpcWG.Wait()
	}
	if err := c.raft.Shutdown().Error(); err != nil {
		firstErr = err
	}
	if err := c.trans.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := c.store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func itoa(n int32) string { return strconv.FormatInt(int64(n), 10) }
