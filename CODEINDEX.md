# Code index

Navigation map for `mq`. Read this first in a new session. See [docs/HLD.md](docs/HLD.md)
for the big picture and [docs/LLD.md](docs/LLD.md) for byte-level detail.

## Layout

| Path | Responsibility |
|------|----------------|
| [cmd/mqbroker/main.go](cmd/mqbroker/main.go) | Entrypoint: load config, build broker + coordinator + server, run flush/retention loops, handle SIGINT/SIGTERM. |
| [internal/config/config.go](internal/config/config.go) | `Config` + `Load()` — flags > `MQ_*` env > defaults (incl. `NodeID`, `Brokers`). |
| [internal/cluster/cluster.go](internal/cluster/cluster.go) | Static membership + deterministic placement: `LeaderFor(topic,p)`, `ReplicasFor(topic,p,rf)` (RF placement seed), `IsLeader`, `GroupCoordinator(group)`, `IsCoordinator`. mq's stand-in for KRaft/ZooKeeper. |
| [internal/controller/](internal/controller/) | Raft metadata controller (Workstream D, Phases 1–2): `fsm.go` (replicated topic/partition/leader/ISR state machine + read views + apply/restore notify hook), `command.go` (flat JSON command set), `controller.go` (hashicorp/raft wiring, leader `Apply` with forward-and-retry, failover), `rpc.go` (leader-forwarding RPC, port +2). Cluster-mode only; as of Phase 2 it owns placement on the client path (topic creation + Metadata). |
| [internal/kbytes/](internal/kbytes/) | Wire primitives. `reader.go` / `writer.go`: int/string/bytes/array (fixed, non-compact). The only place (de)serialization happens. |
| [internal/protocol/](internal/protocol/) | Kafka request/response structs + per-version encode/decode. One file per API group. |
| [internal/record/batch.go](internal/record/batch.go) | RecordBatch v2 header parse/patch + CRC32-C; `Iterate` over concatenated batches. |
| [internal/storage/](internal/storage/) | Segmented append-only log per partition. `log.go` (Append/Read/recovery/retention/flush), `segment.go` (.log+.index files), `index.go` (sparse index + binary search). |
| [internal/broker/broker.go](internal/broker/broker.go) | Topic catalog + storage logs. Without a controller, logs only for **led** partitions (`LocalLog` returns `ErrNotLeader` otherwise). With a controller (`SetController`), placement/catalog come from the FSM, topic creation is proposed through it, and `reconcileFromFSM` opens a log for every **replicated** partition (leader or follower). Flush-all + retention sweep. |
| [internal/group/](internal/group/) | `coordinator.go` (join/sync/heartbeat/leave state machine + reaper), `offsets.go` (persisted committed-offset store). |
| [internal/server/](internal/server/) | `server.go` (TCP accept + frame loop), `handlers.go` (the glue: decode → broker/storage/group → encode), `integration_test.go` (franz-go, `-tags integration`). |

## Protocol files (internal/protocol)

| File | APIs | Supported version cap |
|------|------|-----------------------|
| `apikeys.go` | API keys, `SupportedVersions` table, error codes | — |
| `header.go` | request header v1, response header v0 | — |
| `apiversions.go` | ApiVersions (18) | ≤2 |
| `metadata.go` | Metadata (3) | ≤8 |
| `produce.go` | Produce (0) | ≤7 |
| `fetch.go` | Fetch (1) | ≤11 |
| `listoffsets.go` | ListOffsets (2) | ≤5 |
| `group.go` | FindCoordinator(10), JoinGroup(11), SyncGroup(14), Heartbeat(12), LeaveGroup(13) | ≤2 except FindCoordinator/Join |
| `describegroups.go` | DescribeGroups (15) | ≤2 |
| `listgroups.go` | ListGroups (16) | ≤2 |
| `offsets.go` | OffsetCommit (8), OffsetFetch (9) | ≤6 / **≤5** |
| `createtopics.go` | CreateTopics (19) | ≤4 |

> **Version caps are deliberate.** Each is held below that API's Kafka "flexible-version"
> threshold so clients negotiate the simpler fixed (non-compact) encoding. OffsetFetch turns
> flexible at v6, so it is capped at v5 — getting this wrong makes the decoder misread compact
> bytes and hang.

## "Where do I change X?"

- **Add/extend an API version** → its file in `internal/protocol/` (encode/decode switch on
  `version`); bump the cap in `apikeys.go`; nothing else changes (handlers are version-agnostic).
- **Change on-disk format** → `internal/storage/segment.go` + `index.go`; recovery in
  `log.go:recoverActive`.
- **Change produce/fetch/offset behavior** → the matching method in
  `internal/server/handlers.go` (the only cross-layer glue). Fetch long-polls up to
  `max.wait.ms` (`maxFetchWait` cap) so caught-up consumers don't busy-poll.
- **Consumer-group semantics** (rebalance, generations, timeouts) → `internal/group/coordinator.go`.
- **Partition placement / group-coordinator assignment** → `internal/cluster/cluster.go`
  (`LeaderFor`, `GroupCoordinator`). Routing decisions in `handlers.go` (`leaderLog`,
  `notCoordinator`, `metadata`, `findCoordinator`).
- **Add a config knob** → `internal/config/config.go` (add field + env + flag).

## Run / test

```bash
go run ./cmd/mqbroker            # start broker (./data, :9092)
go test ./...                    # unit tests
go test -tags integration ./...  # franz-go wire-compat tests
docker compose up --build        # containerized, port 9092
```
