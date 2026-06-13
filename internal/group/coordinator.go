package group

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// Error codes returned to handlers (mirror Kafka codes; handlers pass them through).
const (
	ErrNone                int16 = 0
	ErrIllegalGeneration   int16 = 22
	ErrUnknownMemberID     int16 = 25
	ErrRebalanceInProgress int16 = 27
)

// syncWaitTimeout bounds how long a follower SyncGroup blocks for the leader.
const syncWaitTimeout = 30 * time.Second

type groupState int

const (
	stateEmpty groupState = iota
	stateAwaitingSync
	stateStable
)

// String maps a group state to its Kafka name (reported by DescribeGroups).
// stateAwaitingSync corresponds to Kafka's CompletingRebalance (members joined,
// awaiting the leader's assignment via SyncGroup).
func (s groupState) String() string {
	switch s {
	case stateEmpty:
		return "Empty"
	case stateAwaitingSync:
		return "CompletingRebalance"
	case stateStable:
		return "Stable"
	default:
		return "Dead"
	}
}

type member struct {
	id             string
	protocols      []Protocol
	assignment     []byte
	lastHeartbeat  time.Time
	sessionTimeout time.Duration
	synced         bool
}

type group struct {
	id           string
	protocolType string
	mu           sync.Mutex
	cond         *sync.Cond
	generation   int32
	leaderID     string
	protocolName string
	state        groupState
	members      map[string]*member
}

// Coordinator manages all consumer groups plus the persisted offset store.
type Coordinator struct {
	mu     sync.Mutex
	groups map[string]*group
	store  *OffsetStore
	quit   chan struct{}
}

// Protocol is one candidate assignment protocol a member supports.
type Protocol struct {
	Name     string
	Metadata []byte
}

// Member is a group member echoed to the leader (id + chosen-protocol metadata).
type Member struct {
	ID       string
	Metadata []byte
}

// NewCoordinator builds a coordinator over the given offset store and starts the
// session-timeout reaper.
func NewCoordinator(store *OffsetStore) *Coordinator {
	c := &Coordinator{groups: map[string]*group{}, store: store, quit: make(chan struct{})}
	go c.reap()
	return c
}

// Close stops the background reaper.
func (c *Coordinator) Close() { close(c.quit) }

// Store exposes the offset store for commit/fetch handlers.
func (c *Coordinator) Store() *OffsetStore { return c.store }

// GroupOverview summarizes a group for ListGroups.
type GroupOverview struct {
	GroupID      string
	ProtocolType string
	State        string
}

// ListGroups returns a summary of every known group, sorted by id.
func (c *Coordinator) ListGroups() []GroupOverview {
	c.mu.Lock()
	groups := make([]*group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.Unlock()

	out := make([]GroupOverview, 0, len(groups))
	for _, g := range groups {
		g.mu.Lock()
		out = append(out, GroupOverview{GroupID: g.id, ProtocolType: g.protocolType, State: g.state.String()})
		g.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out
}

// MemberDescription describes one group member for DescribeGroups. ClientID/Host are
// not retained by mq (see docs/GAPS_PLAN.md), so handlers leave them empty.
type MemberDescription struct {
	MemberID   string
	Metadata   []byte
	Assignment []byte
}

// GroupDescription is a full description of one group for DescribeGroups.
type GroupDescription struct {
	State        string
	ProtocolType string
	Protocol     string
	Members      []MemberDescription
}

// DescribeGroup returns a full description of one group, if it exists.
func (c *Coordinator) DescribeGroup(id string) (GroupDescription, bool) {
	g := c.lookup(id)
	if g == nil {
		return GroupDescription{}, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	d := GroupDescription{
		State:        g.state.String(),
		ProtocolType: g.protocolType,
		Protocol:     g.protocolName,
	}
	for _, m := range g.members {
		d.Members = append(d.Members, MemberDescription{
			MemberID:   m.id,
			Metadata:   metadataFor(m, g.protocolName),
			Assignment: m.assignment,
		})
	}
	return d, true
}

func (c *Coordinator) getOrCreate(id, protocolType string) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[id]
	if !ok {
		g = &group{id: id, protocolType: protocolType, state: stateEmpty, members: map[string]*member{}}
		g.cond = sync.NewCond(&g.mu)
		c.groups[id] = g
	}
	return g
}

// --- JoinGroup ---

// JoinArgs are the inputs to a join.
type JoinArgs struct {
	GroupID            string
	MemberID           string
	ClientID           string
	ProtocolType       string
	Protocols          []Protocol
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
}

// JoinResult is the outcome of a join.
type JoinResult struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	LeaderID     string
	MemberID     string
	Members      []Member // populated only for the leader
}

// Join adds/refreshes a member and (re)starts a rebalance round when membership
// changes. Convergence across multiple consumers is driven by Heartbeat returning
// REBALANCE_IN_PROGRESS to stale-generation members, prompting them to rejoin.
func (c *Coordinator) Join(args JoinArgs) JoinResult {
	g := c.getOrCreate(args.GroupID, args.ProtocolType)
	g.mu.Lock()
	defer g.mu.Unlock()

	memberID := args.MemberID
	isNew := false
	if memberID == "" {
		memberID = args.ClientID + "-" + randomID()
		isNew = true
	}
	m, exists := g.members[memberID]
	if !exists {
		m = &member{id: memberID}
		g.members[memberID] = m
		isNew = true
	}
	m.protocols = args.Protocols
	m.lastHeartbeat = time.Now()
	m.sessionTimeout = time.Duration(args.SessionTimeoutMs) * time.Millisecond
	if m.sessionTimeout <= 0 {
		m.sessionTimeout = 30 * time.Second
	}

	// A membership change (or first member) opens a new generation.
	if isNew || g.state == stateEmpty {
		g.generation++
		g.state = stateAwaitingSync
		for _, mm := range g.members {
			mm.synced = false
		}
		g.cond.Broadcast()
	}
	if _, ok := g.members[g.leaderID]; !ok || g.leaderID == "" {
		g.leaderID = memberID // first/anyone becomes leader if the old one is gone
	}
	g.protocolName = chooseProtocol(g)

	res := JoinResult{
		GenerationID: g.generation,
		ProtocolName: g.protocolName,
		LeaderID:     g.leaderID,
		MemberID:     memberID,
	}
	if memberID == g.leaderID {
		for id, mm := range g.members {
			res.Members = append(res.Members, Member{ID: id, Metadata: metadataFor(mm, g.protocolName)})
		}
	}
	return res
}

// --- SyncGroup ---

// SyncResult is the outcome of a sync.
type SyncResult struct {
	ErrorCode  int16
	Assignment []byte
}

// Sync stores the leader's assignments and unblocks followers; followers block until
// the leader has synced (or a timeout) and then receive their assignment.
func (c *Coordinator) Sync(groupID, memberID string, generation int32, assignments map[string][]byte) SyncResult {
	g := c.lookup(groupID)
	if g == nil {
		return SyncResult{ErrorCode: ErrUnknownMemberID}
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.members[memberID]
	if !ok {
		return SyncResult{ErrorCode: ErrUnknownMemberID}
	}
	if generation != g.generation {
		return SyncResult{ErrorCode: ErrIllegalGeneration}
	}

	if memberID == g.leaderID {
		for id, a := range assignments {
			if mm, ok := g.members[id]; ok {
				mm.assignment = a
			}
		}
		g.state = stateStable
		g.cond.Broadcast()
	} else {
		deadline := time.Now().Add(syncWaitTimeout)
		for g.state != stateStable && generation == g.generation && time.Now().Before(deadline) {
			waitUntil(g.cond, deadline)
		}
		if generation != g.generation {
			return SyncResult{ErrorCode: ErrRebalanceInProgress}
		}
	}
	m.synced = true
	m.lastHeartbeat = time.Now()
	return SyncResult{Assignment: m.assignment}
}

// --- Heartbeat ---

// Heartbeat refreshes a member's liveness; a stale generation triggers a rejoin.
func (c *Coordinator) Heartbeat(groupID, memberID string, generation int32) int16 {
	g := c.lookup(groupID)
	if g == nil {
		return ErrUnknownMemberID
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[memberID]
	if !ok {
		return ErrUnknownMemberID
	}
	if generation < g.generation {
		return ErrRebalanceInProgress
	}
	if generation > g.generation {
		return ErrIllegalGeneration
	}
	m.lastHeartbeat = time.Now()
	return ErrNone
}

// --- LeaveGroup ---

// Leave removes a member and opens a new generation for the remaining members.
func (c *Coordinator) Leave(groupID, memberID string) int16 {
	g := c.lookup(groupID)
	if g == nil {
		return ErrUnknownMemberID
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[memberID]; !ok {
		return ErrUnknownMemberID
	}
	delete(g.members, memberID)
	c.rebalanceLocked(g)
	return ErrNone
}

// rebalanceLocked opens a new generation. Caller holds g.mu.
func (c *Coordinator) rebalanceLocked(g *group) {
	g.generation++
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.leaderID = ""
	} else {
		g.state = stateAwaitingSync
		if _, ok := g.members[g.leaderID]; !ok {
			for id := range g.members {
				g.leaderID = id
				break
			}
		}
		for _, mm := range g.members {
			mm.synced = false
		}
	}
	g.cond.Broadcast()
}

func (c *Coordinator) lookup(id string) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups[id]
}

// reap evicts members whose session has expired and rebalances their groups.
func (c *Coordinator) reap() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
			c.mu.Lock()
			groups := make([]*group, 0, len(c.groups))
			for _, g := range c.groups {
				groups = append(groups, g)
			}
			c.mu.Unlock()
			now := time.Now()
			for _, g := range groups {
				g.mu.Lock()
				var dead []string
				for id, m := range g.members {
					if now.Sub(m.lastHeartbeat) > m.sessionTimeout {
						dead = append(dead, id)
					}
				}
				for _, id := range dead {
					delete(g.members, id)
				}
				if len(dead) > 0 {
					c.rebalanceLocked(g)
				}
				g.mu.Unlock()
			}
		}
	}
}

// chooseProtocol picks an assignment protocol common to the group (the first one
// proposed by the leader, falling back to any member). Caller holds g.mu.
func chooseProtocol(g *group) string {
	if m, ok := g.members[g.leaderID]; ok && len(m.protocols) > 0 {
		return m.protocols[0].Name
	}
	for _, m := range g.members {
		if len(m.protocols) > 0 {
			return m.protocols[0].Name
		}
	}
	return ""
}

// metadataFor returns a member's metadata for the chosen protocol name.
func metadataFor(m *member, name string) []byte {
	for _, p := range m.protocols {
		if p.Name == name {
			return p.Metadata
		}
	}
	if len(m.protocols) > 0 {
		return m.protocols[0].Metadata
	}
	return nil
}

// waitUntil blocks on cond until broadcast or the deadline elapses. Caller holds
// cond.L; a timer broadcasts at the deadline so the wait loop re-checks.
func waitUntil(cond *sync.Cond, deadline time.Time) {
	timer := time.AfterFunc(time.Until(deadline), func() {
		cond.L.Lock()
		cond.Broadcast()
		cond.L.Unlock()
	})
	cond.Wait()
	timer.Stop()
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
