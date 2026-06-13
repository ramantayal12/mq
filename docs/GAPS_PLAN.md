# Gaps Plan — design for the six basic-functionality gaps

Finalized design for closing the gaps found by the real-time streaming harness
(`realtime_integration_test.go`). **Everything here is pre-decided.** No open
questions remain; implementation follows this document.

Read [HLD.md](HLD.md) for the system overview and [LLD.md](LLD.md) for byte-level
detail. File references below are clickable into the current tree.

## 1. Scope

Six gaps, grouped into three independent quick wins (A–C) and one sequential epic (D):

| # | Gap | Workstream |
|---|-----|------------|
| 2 | No consumer-group admin/introspection (ListGroups, DescribeGroups) | **A** |
| 4 | `OFFSET_OUT_OF_RANGE` never returned | **B** |
| 3 | `ListOffsets` ignores timestamps | **C** |
| 1 | No replication / failover | **D** (Phases 1–5) |
| 6 | `acks=all` behaves like `acks=1` | **D** (Phase 4) |
| 5 | Cluster topics use a fixed partition count | **D** (Phase 6) |

A, B, C touch none of the replication surface and may proceed immediately and in
parallel. D is strictly sequential and gated on Phase 1.

## 2. Decisions register (locked)

| Topic | Decision | Rationale |
|-------|----------|-----------|
| Consensus mechanism | **Embed `hashicorp/raft` + `raft-boltdb`** | Large code savings over a hand-rolled Raft. Accepted break from HLD §1 "no external broker deps"; record in HLD §8. |
| Raft quorum membership | **Every broker is a Raft voter** | Small clusters; mirrors a co-located KRaft quorum. Implies majority-alive for failover ⇒ prefer odd cluster sizes (3, 5). |
| Epic ordering | **Controller-first** — Phase 1 is a prerequisite; replication ships only with automatic failover | User decision: no intermediate "replicated-but-no-failover" state. |
| Follower replication transport | **Reuse the Kafka `Fetch` API with `ReplicaID = self`** | Broker already speaks the wire protocol; `ReplicaID` field already exists. No new internal RPC. |
| ISR change authority | **Committed through the controller (Raft)** | Faithful to Kafka's AlterPartition flow; not a leader-local decision. |
| `cluster` package role after Phase 1 | **Demoted to bootstrap membership + initial placement seed**; live leadership/ISR lives in the Raft FSM | `LeaderFor` survives only as the first-assignment seed. |
| Timestamp seek implementation | **Scan-based `OffsetForTimestamp`** (no `.timeindex` file) | Correct and surgical; no on-disk format/recovery change. `.timeindex` is a future optimization only. |
| DescribeGroups `client.id`/`client.host` | **Skip for now — return empty strings** | The lag table (`--describe`) does not need them; real values require plumbing header client-id + conn remote-addr into the coordinator. Out of scope for this pass. |
| Version caps for new APIs | **ListGroups ≤2, DescribeGroups ≤2, CreatePartitions ≤1** | Conservative, like the codebase's existing caps (Heartbeat is ≤2 though it flexes at v4). DescribeGroups v3 adds `authorized_operations` (no authz in mq) and v4 adds `group_instance_id` (no static membership) — capping at v2 avoids both and stays on fixed (non-compact) encoding. |
| Controller command encoding | **JSON, one flat `Command` struct with a `Type` discriminator** | Metadata volume is tiny, so size/speed are irrelevant; JSON is human-inspectable in the raft log and snapshots. |
| Broker liveness | **Leader-local soft state via a dedicated controller RPC**; only the resulting `BrokerDown`/`ChangeLeader` are raft-committed | Avoids raft write-amplification from per-second heartbeats; only durable leadership changes go through the log. |
| Controller ports | Raft transport on **client port + 1**; controller forwarding RPC on **client port + 2** | Keeps the Kafka port clean; no private API keys injected into the Kafka protocol. |

---

## Workstream A — Consumer-group introspection (#2)

**Success criteria:** `kafka-consumer-groups.sh --bootstrap-server … --list` returns
running groups; `--describe --group X` returns members and the existing lag table
(OffsetFetch + ListOffsets already work). Verified by a franz-go integration test
(`AdminClient.ListGroups` / `DescribeGroups`) and a manual CLI run.

The coordinator already holds everything needed (members with `id`/`protocols`/
`assignment`, plus `generation`/`leaderID`/`protocolName`/`state`); it only lacks a
read accessor. No storage or wire-format novelty.

### Changes

- [internal/protocol/apikeys.go](../internal/protocol/apikeys.go): add
  `APIDescribeGroups int16 = 15`, `APIListGroups int16 = 16`; add two
  `SupportedVersions` rows — `{APIListGroups, 0, 2}`, `{APIDescribeGroups, 0, 2}`.
- New `internal/protocol/listgroups.go` and `internal/protocol/describegroups.go`:
  request/response structs + per-version encode/decode, in the style of
  `group.go`/`offsets.go`. DescribeGroups returns member `assignment` bytes
  **verbatim** (client decodes them — same opaque-blob discipline as record batches).
- [internal/group/coordinator.go](../internal/group/coordinator.go): add read-only
  accessors that take the existing locks:
  - `ListGroups() []GroupOverview` where `GroupOverview = {GroupID, ProtocolType, State string}`.
  - `DescribeGroup(id string) (GroupDescription, bool)` with state, protocolType,
    protocolName, leader, and members `{MemberID, Assignment, Metadata []byte}`.
  - `func (s groupState) String() string` → Kafka state names: `Empty`,
    `PreparingRebalance` (for `stateAwaitingSync`), `Stable`, `Dead`.
- [internal/server/handlers.go](../internal/server/handlers.go): two handlers + two
  `Dispatch` cases. Cluster semantics (free, and matching Kafka):
  - **ListGroups** returns only groups this broker coordinates (clients query every
    broker and merge).
  - **DescribeGroups** for a group not coordinated here returns `NOT_COORDINATOR`
    (reuse the existing `notCoordinator` helper); clients reach the coordinator via
    FindCoordinator first.

---

## Workstream B — `OFFSET_OUT_OF_RANGE` (#4)

**Success criteria:** a fetch below the earliest retained offset returns error code 1
(triggering the client's `auto.offset.reset`); a fetch above the high-watermark
returns error 1; a fetch *exactly at* the HWM still returns empty (normal caught-up
case — must not regress the long-poll). Verified by a unit test on
`storage.Log.Read` plus an integration test: produce, let retention delete a segment,
fetch the deleted offset.

`protocol.ErrOffsetOutOfRange` (code 1) already exists in
[apikeys.go](../internal/protocol/apikeys.go).

### The three cases `Read` must distinguish

| offset vs. log | today ([log.go:134](../internal/storage/log.go#L134)) | correct |
|---|---|---|
| `< EarliestOffset` (retention deleted it) | silently clamps up to earliest | **error 1** |
| `> nextOffset` (HWM) | `nil, nil` | **error 1** |
| `== nextOffset` (caught up) | `nil, nil` | `nil, nil` (unchanged) |

### Changes

- [internal/storage/log.go](../internal/storage/log.go): add
  `var ErrOffsetOutOfRange = errors.New("storage: offset out of range")`. In `Read`:
  return it for `offset < l.segments[0].baseOffset` and for `offset > l.nextOffset`;
  keep `offset == l.nextOffset → nil, nil`. Remove the clamp at
  [log.go:142-144](../internal/storage/log.go#L142).
- [internal/server/handlers.go](../internal/server/handlers.go) `buildFetch`: map
  `errors.Is(err, storage.ErrOffsetOutOfRange)` → `pr.ErrorCode = protocol.ErrOffsetOutOfRange`.
  **Long-poll interaction:** an out-of-range partition contributes 0 bytes, so the
  current loop ([handlers.go:216-227](../internal/server/handlers.go#L216)) would
  block until the deadline. `buildFetch` must signal a terminal partition error back
  to `fetch`, which then returns immediately instead of long-polling.
- `logHandle` interface is unaffected (`Read` signature unchanged).

---

## Workstream C — `ListOffsets` timestamp seek (#3)

**Success criteria:** `ListOffsets(timestamp=T)` returns the offset of the first
record with timestamp ≥ T (franz-go `OffsetForTimestamp`); a `T` past the end returns
offset `-1` ("no offset for timestamp"); EARLIEST/LATEST keep working. Verified by a
unit test (produce batches with known timestamps, query midpoints) and an integration
test.

RecordBatch v2 carries `firstTimestamp` at byte offset 27 and `maxTimestamp` at 35 —
both currently unparsed.

### Changes

- [internal/record/batch.go](../internal/record/batch.go): add field offset consts
  `offFirstTimestamp = 27`, `offMaxTimestamp = 35`; add `FirstTimestamp`/
  `MaxTimestamp` to `BatchHeader` and populate in `ParseHeader`. Pure addition; no
  on-disk format change.
- [internal/storage/log.go](../internal/storage/log.go): add
  `OffsetForTimestamp(ts int64) (offset int64, found bool)` — walk batch headers
  (using the existing sparse offset index to seek to each segment start) and return
  the base offset of the first batch whose `MaxTimestamp >= ts`; `found=false` if none
  qualify. Scan-based by decision; `.timeindex` is a future optimization only.
- [internal/server/handlers.go](../internal/server/handlers.go) `listOffsets`:
  replace the fallthrough at [handlers.go:278-280](../internal/server/handlers.go#L278) —
  for a timestamp that is neither EARLIEST nor LATEST, call `OffsetForTimestamp`;
  return `-1` when not found (not `LatestOffset`).

---

## Workstream D — Replication, failover, and consequences (#1, #6, #5)

**Controller-first.** Nothing in D ships until Phase 1 is in place. The deepest and
riskiest change is the HWM/LEO split in Phase 3.

### What `cluster` becomes

Today [cluster.go](../internal/cluster/cluster.go) is a pure-function stand-in for
KRaft. After Phase 1 it shrinks to **bootstrap membership** (seeding the Raft peer set
from `MQ_BROKERS`) plus the **initial placement seed** (`ReplicasFor`). Live
leadership/ISR moves into the Raft-replicated FSM. `IsLeader`/`IsCoordinator` read FSM
state, not the pure function.

### Phase 1 — Raft metadata controller (prerequisite)

**Goal:** an elected controller owns cluster metadata via a Raft FSM; broker liveness
is tracked; metadata survives controller death. **Verify:** 3-broker quorum, kill the
controller, a new one is elected and serves identical Metadata.

Dependencies (added to `go.mod`): `github.com/hashicorp/raft`,
`github.com/hashicorp/raft-boltdb/v2`.

#### 1a. Package layout

- `internal/controller/controller.go` — wraps `*raft.Raft`, owns the `*FSM`, exposes
  the API the handlers/broker call. Lifecycle: `New(cfg, fsm) (*Controller, error)`,
  `Close() error`.
- `internal/controller/fsm.go` — the `raft.FSM` (`Apply`/`Snapshot`/`Restore`) + the
  in-memory `metaState` it guards.
- `internal/controller/command.go` — the `Command` type + JSON (un)marshal helpers.
- `internal/controller/rpc.go` — the leader-forwarding RPC server/client (port +2).

#### 1b. hashicorp/raft wiring (locked)

Under `<log-dirs>/raft/`:
- log + stable store: a single `raftboltdb.NewBoltStore(<raft>/store.db)` (satisfies
  both `LogStore` and `StableStore`).
- snapshots: `raft.NewFileSnapshotStore(<raft>, 2, os.Stderr)` (retain 2).
- transport: `raft.NewTCPTransport(raftBind, advertise, 3, 10s, os.Stderr)` on
  **client port + 1**.
- config: `raft.DefaultConfig()` with `LocalID = raft.ServerID(itoa(NodeID))`.
- bootstrap: when `MQ_RAFT_BOOTSTRAP=true` *and* no existing state,
  `BootstrapCluster` with one `raft.Server` per `MQ_BROKERS` entry (ServerID = node id,
  Address = `host:(port+1)`). Otherwise `NewRaft` and rejoin the existing quorum.

#### 1c. FSM state & command set (locked)

```go
type metaState struct {
    Topics map[string]*topicMeta // topic -> meta
}
type topicMeta struct {
    Partitions int32
    Parts      []partitionMeta   // index -> meta
}
type partitionMeta struct {
    Replicas    []int32
    Leader      int32
    ISR         []int32
    LeaderEpoch int32
}
```

Broker liveness is **not** in the FSM — it is leader-local soft state (1d). One flat
command struct, JSON-encoded, discriminated by `Type`:

```go
type CmdType uint8
const (
    CmdCreateTopic CmdType = iota + 1
    CmdCreatePartitions
    CmdChangeLeader   // {Topic, Partition, Leader, ISR, bumps LeaderEpoch}
    CmdChangeISR      // {Topic, Partition, ISR}
)
type Command struct {
    Type       CmdType  `json:"t"`
    Topic      string   `json:"topic,omitempty"`
    Partition  int32    `json:"p,omitempty"`
    Partitions int32    `json:"np,omitempty"`   // CreateTopic / CreatePartitions
    Replicas   [][]int32`json:"replicas,omitempty"` // per-partition replica sets (CreateTopic/CreatePartitions)
    Leader     int32    `json:"leader,omitempty"`
    ISR        []int32  `json:"isr,omitempty"`
}
```

`FSM.Apply(*raft.Log)` JSON-decodes one `Command` and mutates `metaState` under its
write lock; it is the only writer, so application is deterministic. `Snapshot` returns
a value that JSON-encodes the whole `metaState`; `Restore` decodes it. (`RegisterBroker`
and heartbeats are intentionally absent — they are soft state, see 1d.)

#### 1d. Leader forwarding & liveness (locked)

Mutations can arrive at any broker, but only the raft leader may propose. `Controller`
exposes `Apply(cmd Command) error`:
- if `raft.State()==Leader`: `raft.Apply(json(cmd), 10s)` and return its error;
- else: forward `cmd` to the leader's controller RPC (port +2 of the leader's
  advertised host, resolved via `raft.LeaderWithID()` → node id → `cluster.Broker`).

The RPC (`rpc.go`) is a tiny length-prefixed JSON frame: `Command` in, `{error string}`
out. **Liveness**: every broker sends a heartbeat over this same RPC each second; the
leader records `lastSeen[nodeID]` in memory. When a broker misses `livenessTimeout`
(≈3 heartbeats), the leader proposes `CmdChangeLeader` for every partition that node
led, electing the first surviving ISR member (Phase 5). Heartbeats never touch the raft
log; only the resulting leadership change is committed.

#### 1e. Config & cluster changes

- [internal/config/config.go](../internal/config/config.go): add `RaftBootstrap bool`
  (`MQ_RAFT_BOOTSTRAP`), and derive raft/controller ports as advertised-port +1/+2
  (no new port knobs needed). Raft data dir is `<LogDirs>/raft`.
- [cluster.go](../internal/cluster/cluster.go): demoted to bootstrap membership +
  `LeaderFor`/`ReplicasFor` as the *initial* seed only. `IsLeader`/`IsCoordinator` read
  FSM state via the controller once Phase 2 lands.
- `Metadata` is served from FSM state once Phase 2 lands; in Phase 1 it still reads the
  static `cluster` view, so Phase 1 ships without changing client-visible behavior — it
  only stands up the controller and proves failover-of-the-controller in isolation.

### Phase 2 — Replica placement from the FSM (RF > 1)

**Goal:** partitions have RF replicas assigned by the controller; each broker opens
logs for partitions it replicates. **Verify:** RF=3 across 3 brokers, the same
partition's `.log` exists on all three.

- [config](../internal/config/config.go): `ReplicationFactor` (`MQ_REPLICATION_FACTOR`,
  clamped to quorum size).
- Controller assigns the initial replica set on topic creation, seeded by
  `ReplicasFor(topic,p) = brokers[(hash(topic)+p+i) % N]` for `i ∈ [0,RF)`, then stores
  it in the FSM so it is stable across reassignment.
- [broker.go](../internal/broker/broker.go): open logs for any partition where this
  node is in the FSM replica set (leader **or** follower), tagging role. Follower logs
  are written only by the Phase 3 fetcher, never by produce.
- [handlers.go](../internal/server/handlers.go) `metadata`: source
  `Leader`/`Replicas`/`Isr`/leader-epoch from FSM state instead of `[]int32{leader}`
  ([handlers.go:146-151](../internal/server/handlers.go#L146)).

### Phase 3 — Follower fetch + HWM/LEO split + ISR maintenance

**Goal:** followers replicate from the leader; HWM advances only as ISR catches up;
consumers never read past HWM. **Verify:** produce to leader, follower logs converge;
RF=1 suite unchanged.

- **New `internal/replication/`**: per-follower-partition fetcher acting as a Kafka
  client issuing `Fetch` with `ReplicaID = self` against the current leader; re-targets
  when the FSM leader changes.
- [storage/log.go](../internal/storage/log.go): **split HWM from LEO.** Add
  `highWatermark int64` + `HighWatermark()`; `LatestOffset()` stays LEO. *Riskiest
  change* — RF=1 ⇒ ISR={leader} ⇒ HWM==LEO ⇒ no behavior change; the entire existing
  RF=1 test suite must pass before any later phase is layered on.
- Leader side: `Fetch` with `ReplicaID >= 0` records that follower's fetch offset +
  timestamp; in-sync = caught up within `replica.lag.time.max.ms`. ISR shrink/expand is
  committed through the controller (Raft), not locally.
- [handlers.go](../internal/server/handlers.go) `buildFetch`: consumer fetches
  (`ReplicaID < 0`) clamp returned records and `HighWatermark` to HWM
  ([handlers.go:251](../internal/server/handlers.go#L251)); `ListOffsets(LATEST)`
  returns HWM.

### Phase 4 — `acks=all` (resolves #6)

**Goal:** `acks=-1` blocks until ISR replicates; RF=1 returns immediately. **Verify:**
RF=3 acks=all observably waits for followers; RF=1 latency unchanged.

- [handlers.go](../internal/server/handlers.go) `produce`: today acks is ignored except
  `acks==0` ([handlers.go:189](../internal/server/handlers.go#L189)). For `acks==-1`,
  after append, wait (bounded, like the fetch long-poll) until `HWM >= base+count`,
  then respond. At RF=1 this is instant — the old behavior was already correct; this
  only makes it *actually wait* once followers exist.

### Phase 5 — Failover (the payoff of #1)

**Goal:** a dead leader's partitions fail over to an in-sync replica; clients recover
automatically. **Verify:** RF=3, produce continuously, kill the leader broker →
survivor elected, zero data loss within ISR, consumers resume.

- The controller detects the lost heartbeat (Phase 1) → commits a new leader chosen
  from that partition's ISR, bumps `leaderEpoch`, updates the FSM → propagated
  `Metadata` shows the new leader → clients re-route on the existing
  `NOT_LEADER_OR_FOLLOWER` path. The leader-epoch bump fences stale leaders.

### Phase 6 — Custom & expandable partitions (resolves #5)

**Goal:** `CreateTopics` honors custom counts; `CreatePartitions` (API 37) grows a
topic; all brokers converge. **Verify:** create a topic with N≠default partitions in
cluster mode, add partitions, confirm convergence.

- Now possible because the controller agrees counts cluster-wide. Remove the
  `resolveCount` clamp ([broker.go:230](../internal/broker/broker.go#L230)) and the
  `loadExisting` count-from-disk derivation
  ([broker.go:282](../internal/broker/broker.go#L282)), sourcing counts from FSM. Add
  the `CreatePartitions` protocol (API 37, cap ≤1) + handler.

---

## 3. Sequencing & milestones

1. **A — Group introspection** (#2) — independent, high value.
2. **B — OFFSET_OUT_OF_RANGE** (#4) — independent, cheap.
3. **C — ListOffsets timestamp seek** (#3) — independent.
4. **D Phase 1 — Raft controller** — gating prerequisite.
5. **D Phases 2 → 3 → 4** — placement, follower fetch + HWM/LEO split, acks=all.
6. **D Phases 5 → 6** — failover, then custom/expandable partitions.

A/B/C can run in parallel and ship before D. D is strictly sequential. The Phase 3
HWM/LEO split gets its own test pass against the RF=1 suite before Phase 4+ is added.

## 4. Testing strategy

- **Unit:** `storage.Log` (OOR cases, `OffsetForTimestamp`, HWM/LEO split);
  `record.ParseHeader` (timestamps); controller FSM `Apply` determinism.
- **Integration (franz-go, `-tags integration`):** ListGroups/DescribeGroups;
  OOR-triggered `auto.offset.reset`; `OffsetForTimestamp`; RF=3 produce/consume;
  acks=all wait; leader-kill failover; CreatePartitions.
- **Regression gate:** the full existing RF=1 suite must stay green after the Phase 3
  storage change before any later phase lands.

## 5. Docs to update on completion

- [HLD.md](HLD.md): §1/§8 — note `hashicorp/raft` dependency; move replication,
  failover, and custom partition counts out of Non-goals once shipped.
- [LLD.md](LLD.md): controller FSM command set + on-disk Raft layout; HWM/LEO split;
  `.timeindex` absence rationale.
- [../codeindex.md](../codeindex.md): add `internal/controller/`,
  `internal/replication/`, and the new protocol files to the layout table.
