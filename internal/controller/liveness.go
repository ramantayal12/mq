package controller

import (
	"net"
	"time"
)

const (
	// heartbeatInterval is how often every broker pings the controller leader.
	heartbeatInterval = time.Second
	// livenessTimeout is how long the leader goes without a heartbeat before treating a
	// node as dead (~3 missed beats). It is also the grace a freshly elected controller
	// leader gives every node to heartbeat before it will fail anything over.
	livenessTimeout = 3 * time.Second
)

// startLiveness launches the per-node liveness loop. Started only in cluster mode (the
// forwarding RPC is enabled), since heartbeats and failover both ride that RPC.
func (c *Controller) startLiveness() {
	c.liveStop = make(chan struct{})
	c.liveDone = make(chan struct{})
	go c.runLiveness()
}

// runLiveness heartbeats the current controller leader every interval and, while this node
// is the leader, fails over partitions whose leader it has stopped hearing from.
func (c *Controller) runLiveness() {
	defer close(c.liveDone)
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	wasLeader := false
	for {
		select {
		case <-c.liveStop:
			return
		case <-t.C:
			isLeader := c.IsLeader()
			if isLeader && !wasLeader {
				c.onBecomeLeader()
			}
			wasLeader = isLeader
			c.sendHeartbeat()
			if isLeader {
				c.checkFailover()
			}
		}
	}
}

// onBecomeLeader resets liveness state when this node acquires controller leadership: the
// previous leader's lastSeen map means nothing here, so it starts empty, and a fresh grace
// window (leaderSince) prevents an immediate spurious failover before live nodes can beat.
func (c *Controller) onBecomeLeader() {
	c.liveMu.Lock()
	c.lastSeen = map[int32]time.Time{}
	c.leaderSince = time.Now()
	c.liveMu.Unlock()
}

// recordHeartbeat notes that node id was alive just now. Called on the leader when a
// forwarded heartbeat arrives.
func (c *Controller) recordHeartbeat(id int32) {
	c.liveMu.Lock()
	c.lastSeen[id] = time.Now()
	c.liveMu.Unlock()
}

// alive reports whether node id has heartbeated within livenessTimeout. This node is always
// considered alive (it would not be running this check otherwise).
func (c *Controller) alive(id int32, now time.Time) bool {
	if id == c.nodeID {
		return true
	}
	c.liveMu.Lock()
	ls, ok := c.lastSeen[id]
	c.liveMu.Unlock()
	return ok && now.Sub(ls) <= livenessTimeout
}

// sendHeartbeat pings the current controller leader so it knows this node is alive. The
// leader counts itself, so it sends to no one. Best-effort: a dropped beat is retried next
// tick, and enough misses are exactly what trigger this node's failover elsewhere.
func (c *Controller) sendHeartbeat() {
	id, ok := c.LeaderID()
	if !ok || id == c.nodeID {
		return
	}
	addr, ok := c.rpcAddr[id]
	if !ok {
		return
	}
	conn, err := net.DialTimeout("tcp", addr, rpcDialWait)
	if err != nil {
		return
	}
	defer conn.Close()
	data, err := Command{Type: CmdHeartbeat, From: c.nodeID}.encode()
	if err != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(rpcDialWait))
	if writeFrame(conn, data) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(rpcCallWait))
	_, _ = readFrame(conn) // ack carries no payload of interest
}

// checkFailover runs on the controller leader: for every partition whose leader is no
// longer alive, it elects the first surviving in-sync replica and commits a CmdChangeLeader
// (which bumps the leader epoch, fencing the stale leader). A partition with no surviving
// ISR member is left alone — electing an out-of-sync replica would lose committed data.
func (c *Controller) checkFailover() {
	now := time.Now()
	c.liveMu.Lock()
	inGrace := now.Sub(c.leaderSince) < livenessTimeout
	c.liveMu.Unlock()
	if inGrace {
		return
	}
	for _, tv := range c.fsm.Topics() {
		for p := range tv.Parts {
			pv := tv.Parts[p]
			if pv.Leader >= 0 && c.alive(pv.Leader, now) {
				continue
			}
			survivors := make([]int32, 0, len(pv.ISR))
			for _, id := range pv.ISR {
				if c.alive(id, now) {
					survivors = append(survivors, id)
				}
			}
			if len(survivors) == 0 || survivors[0] == pv.Leader {
				continue
			}
			// Best-effort: transient churn (lost leadership mid-scan) is retried next tick.
			_ = c.Apply(Command{
				Type:      CmdChangeLeader,
				Topic:     tv.Name,
				Partition: int32(p),
				Leader:    survivors[0],
				ISR:       survivors,
			})
		}
	}
}
