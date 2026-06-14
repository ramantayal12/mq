# High-Level Design — `mq`, a Kafka-wire-compatible message queue

## 1. Purpose & goals

`mq` is a single-process message broker, written from scratch in Go, that speaks the **real
Apache Kafka TCP wire protocol**. Existing Kafka clients (franz-go, sarama, librdkafka,
kafka-python, the `kafka-console-*` scripts) connect to it **unmodified** and can produce,
consume, and run consumer groups.

The goal is to demonstrate Kafka's core mechanics with the smallest faithful surface area:

- **In scope:** the binary wire protocol, topics, partitions, an append-only segmented
  on-disk log (SSD storage), producers, consumers, and consumer groups with committed offsets.
- **Out of scope:** transactions / idempotent producers, exactly-once semantics, SASL/TLS
  auth, log compaction, quotas. See §8.
- **In active development (Workstream D):** replication with automatic failover, via an
  embedded Raft metadata controller (`hashicorp/raft` + `raft-boltdb/v2`) — the project's
  first external runtime dependencies. This supersedes the former replication-factor-1
  static-placement model (§9) and the prior "no replication / no external broker deps"
  non-goals; see §8 and [docs/GAPS_PLAN.md](GAPS_PLAN.md).

## 2. The four foundational decisions

| # | Decision | Consequence |
|---|----------|-------------|
| 1 | **Real Kafka wire protocol** | Off-the-shelf clients work. Cost: must match Kafka's binary request/response framing exactly. Mitigated by decision in §3. |
| 2 | **Single broker by default; optional static-membership cluster** | One process is the simple default. With `KAFKA_BROKERS` set, several brokers form a cluster with replication factor 1 (see §10) — partitions are spread across brokers and each consumer group has one coordinator broker. No replication/failover. |
| 3 | **Consumer groups + committed offsets** | Full join/sync/heartbeat protocol; offsets persist so restarted consumers resume. |
| 4 | **Async-flush segment files** | Append to OS page cache, `fsync` periodically. Fast, Kafka-default-like; small data-loss window on hard crash. |

## 3. Scope-limiting strategies (how we stay faithful but small)

1. **Advertise only pre-"flexible" API versions.** Kafka's "flexible versions" (KIP-482) add
   compact strings, tagged fields and varint-length arrays. By advertising, per API, a *max
   version below its flexible threshold*, every client negotiates down to the simpler
   fixed-layout encoding — we never implement compact/tagged types.

2. **Treat record batches as opaque blobs.** The producer sends `RecordBatch` v2 byte blobs;
   Fetch returns them. The broker parses **only the batch header** (base offset, last-offset
   delta, record count, CRC) to assign offsets — never individual records. This makes
   **compression transparent**: only the records section is compressed, the header is not, so
   the broker never needs gzip/snappy/lz4/zstd codecs.

3. **The broker is always the coordinator.** Single process ⇒ `FindCoordinator` returns
   itself; group state lives in memory; only committed offsets touch disk.

4. **Assignment is client-side, broker is a relay.** We implement JoinGroup→SyncGroup
   faithfully (the elected leader computes the partition assignment and ships it via
   SyncGroup); the broker just relays member metadata and the resulting assignment.

## 4. System context

```
        ┌──────────────────────────────────────────────┐
        │   Kafka client (franz-go / sarama / console)   │
        └───────────────┬────────────────────────────────┘
                        │  Kafka wire protocol over TCP (:9092)
                        │  4-byte length-prefixed request/response frames
        ┌───────────────▼────────────────────────────────┐
        │                  mq broker (1 process)          │
        │                                                 │
        │  server  → decode header, dispatch by api_key   │
        │  protocol→ per-API request/response codec       │
        │  handlers→ glue: protocol ⇄ broker/storage/group│
        │  broker  → topic/partition registry + metadata  │
        │  group   → coordinator + persisted offsets      │
        │  storage → segmented append-only log per part.  │
        └───────────────┬────────────────────────────────┘
                        │  files
        ┌───────────────▼────────────────────────────────┐
        │  data dir:  <topic>-<part>/NNN.log + NNN.index  │
        │             __offsets/<group>/<topic>-<part>    │
        └─────────────────────────────────────────────────┘
```

## 5. Components & responsibilities (Single-Responsibility split)

| Package | Responsibility | Knows about |
|---------|----------------|-------------|
| `kbytes` | Pure byte-level primitives: read/write int8/16/32/64, varint/varlong, string, bytes, arrays (fixed, non-compact). | nothing else |
| `protocol` | Kafka request/response structs + per-version encode/decode; API-key & version constants. | `kbytes` |
| `record` | RecordBatch v2 header parse/patch + CRC32-C. | `kbytes` |
| `storage` | Segmented append-only log per partition: append, read, offsets, segment roll, flush, recovery. | `record` (header only) |
| `broker` | Topic/partition registry; create/list/auto-create; provides cluster metadata. | `storage` |
| `group` | Consumer-group coordinator state machine + persisted committed-offset store. | `storage` (offset files) |
| `server` | TCP accept loop, per-connection frame loop, dispatch. | `protocol` |
| `handlers` | The only glue layer: turns decoded requests into `broker`/`storage`/`group` calls and back into responses. | all of the above |

Dependency direction flows **inward toward primitives**; storage never imports protocol, and
protocol never imports storage (Dependency-Inversion: handlers depend on small interfaces such
as `Appender`/`LogReader`/`Coordinator`, not concretes — so tests inject fakes).

## 6. Request lifecycle

1. Client opens a TCP connection; sends an `ApiVersions` request first.
2. `server`/`conn` reads a 4-byte big-endian length, then that many bytes = one request frame.
3. The request header (`api_key`, `api_version`, `correlation_id`, `client_id`) is decoded.
4. Dispatch on `api_key` to the matching handler; the handler decodes the request body at the
   negotiated `api_version`, performs the work, and builds a response body.
5. The response is written as `correlation_id` + body, length-prefixed.
6. Per connection, requests are processed sequentially (preserving Kafka's ordering contract).

## 7. Key data flows

- **Produce:** decode batches per topic-partition → for each, `storage.Log.Append(bytes)`
  assigns a base offset, patches the batch header + CRC, writes to the active segment →
  respond with the base offset per partition.
- **Fetch:** for each requested topic-partition+offset → `storage.Log.Read(offset, maxBytes)`
  locates the segment via the sparse index and returns whole batches → assemble Fetch response.
- **Consume group join:** `FindCoordinator`(→self) → `JoinGroup` (members collected, leader
  elected) → `SyncGroup` (leader's assignment relayed to all) → periodic `Heartbeat` →
  `OffsetCommit`/`OffsetFetch` persist & resume positions → `LeaveGroup` on shutdown.

## 8. Non-goals (explicitly out of scope)

Transactions & idempotent producer, exactly-once, SASL/TLS, log compaction, quotas, schema
registry, flexible-version (compact/tagged) wire encodings, and server-side partition assignors.

**Amended (Workstream D).** Replication and multi-broker failover are no longer non-goals.
They are being added via an embedded Raft metadata controller that plays the role KRaft
plays in Kafka, which introduces the project's first external runtime dependencies —
`github.com/hashicorp/raft` and `github.com/hashicorp/raft-boltdb/v2`. This is a deliberate,
accepted break from the original "no external broker deps / no KRaft-style metadata quorum"
stance. See [docs/GAPS_PLAN.md](GAPS_PLAN.md) for the phased plan.

## 9. Horizontal scaling (cluster mode)

`mq` scales out the way a real Kafka cluster does *from the client's perspective* — Metadata
advertises all brokers and a leader per partition, clients route produce/fetch to the partition
leader, and FindCoordinator points each consumer group at one coordinator broker — but it
replaces Kafka's **controller quorum (KRaft/ZooKeeper)** with **static membership + deterministic
placement** ([internal/cluster](../internal/cluster)). Replication factor is 1 (one copy per
partition; no failover).

- **Membership** is configured identically on every broker: `KAFKA_NODE_ID` + `KAFKA_BROKERS`
  (`"0@host0:9092,1@host1:9092,2@host2:9092"`). Empty `KAFKA_BROKERS` = single-broker mode.
- **Partition placement** is a pure function — `leader(topic, p) = brokers[(hash(topic)+p) %
  N]` — so every broker computes the same assignment with zero coordination. A broker opens a
  partition's log only if it leads that partition; a produce/fetch for a partition it doesn't
  lead returns `NOT_LEADER_OR_FOLLOWER` (6) and the client refetches metadata.
- **Group coordination** uses `coordinator(group) = brokers[hash(group) % N]`. A group request
  arriving at the wrong broker returns `NOT_COORDINATOR` (16); the client re-runs FindCoordinator.
  Committed offsets live on the coordinator broker's disk.
- **Trade-off vs. Kafka:** no consensus layer means custom per-topic partition counts can't be
  agreed cluster-wide without a controller, so in cluster mode every topic uses the shared
  `KAFKA_NUM_PARTITIONS`; and a dead broker's partitions are unavailable until it returns (no
  replication). Both are deliberate scope cuts, not design accidents.

## 10. Deployment

Shipped as a Kafka-style Docker image: static binary, `EXPOSE 9092`, data on a
`VOLUME`, configured by `KAFKA_*` env vars (notably `KAFKA_ADVERTISED_LISTENERS`, since the
host:port returned in Metadata is what clients reconnect to). See the Dockerfile and
`docker-compose.yml`.

See `docs/LLD.md` for package contracts, on-disk byte layouts, and algorithms.
