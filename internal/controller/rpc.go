package controller

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// The forwarding RPC is a tiny length-prefixed JSON frame: a Command in, an rpcResult
// out. Mutations can originate at any broker, but only the raft leader may propose, so a
// follower forwards the command here to the leader's RPC address (client port + 2). This
// keeps private controller traffic off the Kafka port and out of the raft log itself.
//
// Liveness heartbeats (the other half of GAPS_PLAN §1d) ride this same RPC as a
// CmdHeartbeat frame: every broker beats the leader, which records lastSeen and fails over
// partitions led by a node it stops hearing from (see liveness.go). Heartbeats are soft
// state — recorded directly, never proposed to the raft log.

const (
	rpcMaxFrame = 1 << 20 // a metadata command is tiny; cap defensively
	rpcDialWait = 3 * time.Second
	rpcCallWait = applyTimeout + 2*time.Second // allow the leader's raft.Apply to finish
)

// rpcResult is the forwarding RPC reply: an empty Error means the command committed.
type rpcResult struct {
	Error string `json:"error,omitempty"`
}

// startRPC binds the forwarding RPC listener and serves it until Close.
func (c *Controller) startRPC(bind string) error {
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	c.rpcLn = ln
	c.rpcWG.Add(1)
	go func() {
		defer c.rpcWG.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed in Close
			}
			c.rpcWG.Add(1)
			go func() {
				defer c.rpcWG.Done()
				c.serveRPCConn(conn)
			}()
		}
	}()
	return nil
}

// serveRPCConn handles one forwarded command: read it, propose it locally (this node is
// expected to be the leader), and write back the application result.
func (c *Controller) serveRPCConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(rpcCallWait))
	frame, err := readFrame(conn)
	if err != nil {
		return
	}
	cmd, err := decodeCommand(frame)
	var res rpcResult
	switch {
	case err != nil:
		res.Error = fmt.Sprintf("controller: undecodable forwarded command: %v", err)
	case cmd.Type == CmdHeartbeat:
		c.recordHeartbeat(cmd.From) // soft liveness state, never proposed to raft
	default:
		if applyErr := c.proposeLocal(cmd); applyErr != nil {
			res.Error = applyErr.Error()
		}
	}
	out, _ := json.Marshal(res)
	_ = conn.SetWriteDeadline(time.Now().Add(rpcCallWait))
	_ = writeFrame(conn, out)
}

// forwardToLeader sends cmd to the current leader's forwarding RPC and returns its result.
// Leadership/transport problems are wrapped in errForwardUnavailable so Apply retries them;
// the leader's own FSM application error is returned verbatim (not retried).
func (c *Controller) forwardToLeader(cmd Command) error {
	id, ok := c.LeaderID()
	if !ok {
		return fmt.Errorf("%w: no leader known yet", errForwardUnavailable)
	}
	addr, ok := c.rpcAddr[id]
	if !ok {
		return fmt.Errorf("controller: no forwarding address for leader node %d", id)
	}
	conn, err := net.DialTimeout("tcp", addr, rpcDialWait)
	if err != nil {
		return fmt.Errorf("%w: dial leader %d: %v", errForwardUnavailable, id, err)
	}
	defer conn.Close()

	data, err := cmd.encode()
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(rpcCallWait))
	if err := writeFrame(conn, data); err != nil {
		return fmt.Errorf("%w: forward to leader %d: %v", errForwardUnavailable, id, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(rpcCallWait))
	respFrame, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("%w: read leader %d reply: %v", errForwardUnavailable, id, err)
	}
	var res rpcResult
	if err := json.Unmarshal(respFrame, &res); err != nil {
		return fmt.Errorf("%w: decode leader %d reply: %v", errForwardUnavailable, id, err)
	}
	switch {
	case res.Error == "":
		return nil
	case res.Error == errNotLeader.Error():
		// The target lost leadership between our resolve and its apply; retry.
		return fmt.Errorf("%w: target node %d no longer leader", errForwardUnavailable, id)
	default:
		return errors.New(res.Error)
	}
}

// writeFrame writes a 4-byte big-endian length prefix followed by payload.
func writeFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads a length-prefixed payload written by writeFrame.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > rpcMaxFrame {
		return nil, fmt.Errorf("controller: bad rpc frame size %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
