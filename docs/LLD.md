# Low-Level Design — `mq`

Companion to `docs/HLD.md`. This document defines exact contracts, on-disk byte layouts, the
wire encodings we support, concurrency rules, and the algorithms. All multi-byte integers on
the wire and on disk are **big-endian** (Kafka convention), except RecordBatch varints which
are **zig-zag varint** per the Kafka spec.

---

## 1. `internal/kbytes` — wire primitives

Two types wrapping a byte slice; the single place serialization lives (DRY).

```go
type Reader struct { buf []byte; pos int; err error }
func NewReader(b []byte) *Reader
func (r *Reader) Int8() int8
func (r *Reader) Int16() int16
func (r *Reader) Int32() int32
func (r *Reader) Int64() int64
func (r *Reader) Varint() int64        // zig-zag varint
func (r *Reader) String() string       // INT16 length, then bytes; len -1 -> "" (treat as null)
func (r *Reader) NullableString() *string
func (r *Reader) Bytes() []byte        // INT32 length, then bytes
func (r *Reader) NullableBytes() []byte
func (r *Reader) ArrayLen() int        // INT32 count; -1 -> null array (return 0)
func (r *Reader) Raw(n int) []byte     // n bytes verbatim (used for opaque record-set)
func (r *Reader) Remaining() []byte
func (r *Reader) Err() error           // first error encountered; callers check once

type Writer struct { buf []byte }
func NewWriter() *Writer
func (w *Writer) Int8/Int16/Int32/Int64(v ...)
func (w *Writer) String(s string)      // INT16 len + bytes
func (w *Writer) NullableString(*string)
func (w *Writer) Bytes(b []byte)       // INT32 len + bytes; nil -> -1
func (w *Writer) ArrayLen(n int)
func (w *Writer) Raw(b []byte)
func (w *Writer) Bytes() []byte         // finalize -> backing slice
```

**Error model:** `Reader` is sticky-error — on a short read it records `err` and returns zero
values; callers decode a whole struct then check `r.Err()` once. No panics on malformed input.

We deliberately implement **only fixed (non-compact)** forms. No compact strings, no tagged
fields, no unsigned varint array lengths — unreachable because of the version caps (§7).

---

## 2. `internal/protocol`

### 2.1 Headers

```go
type RequestHeader struct {       // request header v1 (used for all our APIs)
    APIKey        int16
    APIVersion    int16
    CorrelationID int32
    ClientID      *string         // NullableString
}
```
**Caveat:** `ApiVersions` responses use **header v0** (correlation id only, which is all our
response header ever is). Request header is v1 for every API we accept. (Header v2/flexible is
never reached due to version caps.)

Response: we always write `INT32 correlation_id` then the body.

### 2.2 API key + version constants (`apikeys.go`)

```go
const ( ApiProduce=0; ApiFetch=1; ApiListOffsets=2; ApiMetadata=3;
        ApiOffsetCommit=8; ApiOffsetFetch=9; ApiFindCoordinator=10;
        ApiJoinGroup=11; ApiHeartbeat=12; ApiLeaveGroup=13; ApiSyncGroup=14;
        ApiApiVersions=18; ApiCreateTopics=19 )
```
Supported version ranges (min,max) — **max is capped below each API's flexible threshold**:

| API | min | max | flexible starts at |
|-----|-----|-----|--------------------|
| ApiVersions | 0 | 2 | 3 |
| Metadata | 0 | 8 | 9 |
| Produce | 0 | 7 | 9 |
| Fetch | 0 | 11 | 12 |
| ListOffsets | 0 | 5 | 6 |
| FindCoordinator | 0 | 2 | 3 |
| JoinGroup | 0 | 4 | 6 |
| SyncGroup | 0 | 2 | 4 |
| Heartbeat | 0 | 2 | 4 |
| LeaveGroup | 0 | 2 | 4 |
| OffsetCommit | 0 | 6 | 8 |
| OffsetFetch | 0 | 5 | 6 |
| CreateTopics | 0 | 4 | 5 |

> SyncGroup/Heartbeat/LeaveGroup are additionally capped at v2 (below their v3
> `group_instance_id` / batched-members changes) to keep decode simple. **OffsetFetch turns
> flexible at v6, so it must cap at v5** — advertising v6 makes clients send compact-encoded
> bytes the fixed decoder misreads.

### 2.3 Versioning strategy (Open/Closed)

Each API has one request struct and one response struct holding the **superset** of fields.
Decode/encode switch on `apiVersion` to add/skip the fields that a given version carries (e.g.
Produce gained `transactional_id` in v3, throttle-time in responses at varying versions; Fetch
gained `session_id` in v7, `last_stable_offset`/`log_start_offset`/`aborted_transactions` in
the response partition header at v4/v5). Adding a new version edits only that API's
encode/decode, never the handler or callers.

Per-version field deltas we must honor (the ones that bite):
- **Produce req:** `transactional_id` (NullableString) present iff v≥3.
- **Produce resp:** per-partition `log_append_time` (v≥2), `log_start_offset` (v≥5);
  top-level `throttle_time_ms` (v≥1, position: end in v1-v7).
- **Fetch req:** `replica_id`,`max_wait`,`min_bytes` always; `max_bytes` (v≥3),
  `isolation_level` (v≥4), `session_id`,`session_epoch` (v≥7); per-partition
  `log_start_offset` (v≥5), `current_leader_epoch` (v≥9 → unreachable, cap 11 but we ignore).
- **Fetch resp:** top-level `throttle_time_ms` (v≥1), `error_code`+`session_id` (v≥7);
  per-partition `last_stable_offset` (v≥4), `log_start_offset` (v≥5),
  `aborted_transactions` (v≥4, we always write empty/null), `preferred_read_replica` (v≥11).
- **Metadata resp:** `throttle_time_ms` (v≥3); broker `rack` (v≥1);
  `cluster_id` (v≥2); `controller_id` (v≥1); topic `is_internal` (v≥1); partition
  `offline_replicas` (v≥5); top-level `cluster_authorized_operations` (v≥8, write -1).
- **ListOffsets req:** `isolation_level` (v≥2); per-partition `current_leader_epoch` (v≥4).
  v0 used `max_num_offsets` + returned an offsets array; v≥1 returns single `offset`+`timestamp`.
  We support both shapes.
- **FindCoordinator:** `key_type` (v≥1); resp `error_message` (v≥1).
- **JoinGroup req:** `rebalance_timeout_ms` (v≥1); `group_instance_id` (v≥5→unreachable).
  resp: leader echoes member list with metadata; others get empty list.
- **OffsetCommit req:** `generation_id`+`member_id` (v≥1); `retention_time` (v2-v4);
  per-partition `commit_timestamp` (v1 only), `committed_leader_epoch` (v≥6).
- **OffsetFetch:** topics nullable (v≥2 → null means "all"); resp `error_code` top-level (v≥2).

> When in doubt, the authoritative field-by-field layout per version is the Kafka protocol
> spec; encode/decode mirror it for the capped range only.

### 2.4 Request/response structs

`messages.go` defines plain structs (e.g. `ProduceRequest`, `ProduceResponse`,
`FetchRequest`...). Each has `func (x *T) decode(r *kbytes.Reader, version int16)` and
`func (x *T) encode(w *kbytes.Writer, version int16)`.

---

## 3. `internal/record` — RecordBatch v2

We only parse/patch the **batch header** (fixed 61-byte prefix). Layout (offsets from batch
start):

| off | size | field |
|-----|------|-------|
| 0  | 8 | baseOffset (int64) |
| 8  | 4 | batchLength (int32) — bytes after this field |
| 12 | 4 | partitionLeaderEpoch (int32) |
| 16 | 1 | magic (int8) == 2 |
| 17 | 4 | **crc** (uint32, CRC32-C/Castagnoli) — covers bytes from off 21 to end |
| 21 | 2 | attributes (int16) — bits 0-2 compression codec |
| 23 | 4 | lastOffsetDelta (int32) |
| 27 | 8 | baseTimestamp (int64) |
| 35 | 8 | maxTimestamp (int64) |
| 43 | 8 | producerId (int64) |
| 51 | 2 | producerEpoch (int16) |
| 53 | 4 | baseSequence (int32) |
| 57 | 4 | recordCount (int32) |
| 61 | … | records (possibly compressed — opaque to us) |

```go
type BatchHeader struct {
    BaseOffset      int64
    BatchLength     int32
    LastOffsetDelta int32
    RecordCount     int32
    Magic           int8
}
func ParseHeader(b []byte) (BatchHeader, error)        // validates magic==2, len>=61
func SetBaseOffset(b []byte, off int64)                // patches off 0..8
func RecomputeCRC(b []byte)                             // crc32c over b[21:], write at off 17
func LastOffset(b []byte) int64                         // baseOffset + lastOffsetDelta
```
`crc32.MakeTable(crc32.Castagnoli)` from stdlib `hash/crc32`. A produced request may contain a
*sequence* of batches concatenated; we iterate using `batchLength` (record-set total length =
12 + batchLength per batch).

---

## 4. `internal/storage` — segmented log

### 4.1 On-disk layout
```
<data-dir>/<topic>-<partition>/
    00000000000000000000.log      record batches concatenated, append-only
    00000000000000000000.index    sparse index entries (see below)
```
Segment file name = its **base offset**, zero-padded to 20 digits (Kafka convention).

**Index entry** (8 bytes, fixed): `relativeOffset int32` (offset - segment baseOffset),
`position int32` (byte offset within the `.log`). Entries are appended sparsely — one per
`indexIntervalBytes` (default 4 KiB) of log written. Sorted by construction ⇒ binary search.

### 4.2 Types & contracts
```go
type Log struct {              // one partition
    dir       string
    mu        sync.RWMutex
    segments  []*segment       // sorted by baseOffset; last is active
    active    *segment
    nextOffset int64           // log end offset (next to assign)
    cfg       Config
}
func Open(dir string, cfg Config) (*Log, error)   // creates dir, recovers (§4.4)
func (l *Log) Append(batch []byte) (baseOffset int64, err error)  // patches header, may roll
func (l *Log) Read(offset int64, maxBytes int32) ([]byte, error)  // whole batches up to maxBytes
func (l *Log) EarliestOffset() int64              // first segment baseOffset
func (l *Log) LatestOffset() int64                // == nextOffset (high watermark)
func (l *Log) Flush() error                        // fsync active segment
func (l *Log) Close() error

type segment struct {
    baseOffset int64
    logFile    *os.File
    idxFile    *os.File
    logSize    int32
    index      []indexEntry   // loaded in memory for the active+read segments
    bytesSinceIdx int32
}
```
Small interfaces for handlers (DIP):
```go
type Appender  interface { Append(batch []byte) (int64, error) }
type LogReader interface { Read(offset int64, maxBytes int32) ([]byte, error)
                           EarliestOffset() int64; LatestOffset() int64 }
```

### 4.3 Append algorithm
1. `l.mu.Lock()`.
2. `base := l.nextOffset`; `record.SetBaseOffset(batch, base)`; `record.RecomputeCRC(batch)`.
3. If `active.logSize + len(batch) > cfg.SegmentBytes` and `active.logSize>0`: roll —
   `active.Flush()`, open new segment named `base`.
4. Write `batch` to `active.logFile` at `active.logSize`.
5. If `active.bytesSinceIdx >= cfg.IndexIntervalBytes`: append index entry
   `{int32(base-active.baseOffset), active.logSize}`; reset counter.
6. `count := record.ParseHeader(batch).RecordCount`; `l.nextOffset += int64(count)`;
   advance sizes. Unlock. Return `base`.

> Offsets advance by **record count**, not batch count — matching Kafka. `lastOffsetDelta`
> is consistent with this.

### 4.4 Read algorithm
1. `RLock`. If `offset >= nextOffset` → return empty (no error; client polls again).
   If `offset < EarliestOffset()` → return from earliest (or `OFFSET_OUT_OF_RANGE` per policy;
   we clamp to earliest for simplicity).
2. Find segment: largest `baseOffset <= offset` (binary search over `segments`).
3. In that segment's index, binary search the largest `relativeOffset <= (offset-base)` →
   start `position`.
4. From `position`, scan batches forward (`batchLength` hops), reading the header of each, until
   a batch whose `LastOffset() >= offset`. That batch is the start.
5. Copy whole batches from there while accumulated bytes `<= maxBytes` (always return at least
   one batch even if it exceeds maxBytes, to guarantee progress — Kafka does this too).
6. Return the byte slice (a record-set the Fetch handler embeds verbatim).

### 4.5 Recovery (startup)
For each partition dir: list `*.log`, sort by base offset, rebuild in-memory index for the
active segment by scanning batch headers from position 0 (cheap, sequential), set
`nextOffset = lastBatch.LastOffset()+1`. If the final batch is truncated (fewer bytes than its
`batchLength` declares, e.g. crash mid-append), truncate the file to the last good boundary.
Non-active segments keep their on-disk `.index`; load lazily on read.

### 4.6 Flush (async)
`flush.go` runs one goroutine per `Log` (or one shared ticker iterating logs): every
`cfg.FlushMs`, call `Flush()` on logs dirtied since last tick. Also flush on segment roll and
on `Close()`. Default `FlushMs=1000`.

### 4.7 Retention (basic, Phase 6)
A periodic sweep deletes whole **non-active** segments whose newest record is older than
`cfg.RetentionMs`, or to keep total size under `cfg.RetentionBytes` (oldest-first). Never
deletes the active segment.

---

## 4b. `internal/cluster` — membership & placement

Replaces Kafka's controller quorum with static config + a pure placement function, so brokers
agree without any inter-broker RPC (there is none).

```go
type BrokerInfo struct { NodeID int32; Host string; Port int32 }
type Cluster struct { self int32; brokers []BrokerInfo } // brokers sorted by NodeID
func Single(nodeID int32, host string, port int32) *Cluster
func Parse(nodeID int32, spec, selfHost string, selfPort int32) (*Cluster, error) // "id@host:port,..."
func (c) LeaderFor(topic string, partition int32) int32   // brokers[(fnv32a(topic)+p) % N].NodeID
func (c) IsLeader(topic string, partition int32) bool
func (c) GroupCoordinator(groupID string) int32           // brokers[fnv32a(groupID) % N].NodeID
func (c) IsCoordinator(groupID string) bool
func (c) Brokers() []BrokerInfo; func (c) Broker(id int32) (BrokerInfo, bool); func (c) Size() int
```

- **Empty spec ⇒ single node**, fully backward compatible (`IsLeader`/`IsCoordinator` always true).
- Placement depends only on the topic name, partition index and member ordering (sorted by node
  id) — identical on every broker. Replication factor 1: the leader is the sole replica.
- **Routing in handlers:** `metadata` advertises all `Brokers()` and `LeaderFor` per partition;
  `produce`/`fetch`/`listOffsets` call `broker.LocalLog`, which returns `ErrNotLeader` →
  `NOT_LEADER_OR_FOLLOWER` (6) when this node isn't the leader; group handlers check
  `IsCoordinator` and return `NOT_COORDINATOR` (16) otherwise. Both errors make franz-go refetch
  metadata / re-run FindCoordinator, so the client self-corrects.
- **Partition counts:** a single-node cluster honors a CreateTopics count; a multi-node cluster
  forces the cluster-wide `NumPartitions` (agreeing on a custom count needs a controller).

## 5. `internal/broker` — topic/partition registry
```go
type Partition struct { ID int32; Log storage.Appender /*+LogReader*/ ; Leader int32 }
type Topic struct { Name string; Partitions []*Partition }
type Broker struct {
    mu        sync.RWMutex
    nodeID    int32           // always 0 (single broker)
    host      string; port int32   // advertised
    dataDir   string
    topics    map[string]*Topic
    cfg       Config
}
func New(cfg Config) (*Broker, error)         // loads existing topic dirs from disk
func (b *Broker) Topic(name string) (*Topic, bool)
func (b *Broker) CreateTopic(name string, partitions int32) (*Topic, error)
func (b *Broker) GetOrCreate(name string) *Topic   // auto-create with cfg.NumPartitions
func (b *Broker) Topics() []*Topic
func (b *Broker) PartitionLog(topic string, p int32) (*storage.Log, error)
```
Self metadata for Metadata responses: one broker `{nodeID:0, host, port}`; every partition's
leader/replica/ISR = `[0]`.

---

## 6. `internal/group` — coordinator + offsets

### 6.1 Coordinator state machine
```go
type member struct {
    id            string
    clientID, host string
    metadata      []byte         // subscription, opaque; relayed to leader
    assignment    []byte         // from SyncGroup, opaque; returned to this member
    lastHeartbeat time.Time
    sessionTimeout time.Duration
}
type group struct {
    id           string
    generationID int32
    leaderID     string
    protocolType string           // "consumer"
    protocol     string           // chosen assignment protocol name (e.g. "range")
    state        groupState       // Empty|PreparingRebalance|CompletingRebalance|Stable
    members      map[string]*member
    mu           sync.Mutex
}
type Coordinator struct { mu sync.Mutex; groups map[string]*group; offsets *OffsetStore }
```
Interface used by handlers (DIP):
```go
type GroupCoordinator interface {
    Join(req JoinReq) (JoinResp, error)
    Sync(req SyncReq) (SyncResp, error)
    Heartbeat(group, member string, gen int32) error
    Leave(group, member string) error
    CommitOffset(group string, gen int32, member string, offs []OffsetCommit) error
    FetchOffset(group string, parts []TopicPartition) ([]OffsetFetch, error)
}
```
**Join:** if new member, mint `member_id` (`clientID-uuid`); add to group; transition to
`PreparingRebalance`, bump `generationID`. First member to join becomes `leaderID`. Response to
the **leader** includes every member's metadata; to **followers** an empty member list. All get
the same `generationID`, `leaderID`, chosen `protocol`.

**Sync:** the leader's request carries `[member_id -> assignment]`; we store each, transition to
`Stable`. Every member's SyncGroup response returns *its own* assignment bytes.

**Heartbeat:** validate generation, refresh `lastHeartbeat`; if a rebalance is pending return
`REBALANCE_IN_PROGRESS` (27) so the client rejoins. A reaper goroutine evicts members past
`sessionTimeout` and triggers a rebalance.

**Error codes used:** `UNKNOWN_MEMBER_ID`(25), `ILLEGAL_GENERATION`(22),
`REBALANCE_IN_PROGRESS`(27), `NOT_COORDINATOR`(16, never, single broker), `NONE`(0).

### 6.2 Offset store (persisted)
```
<data-dir>/__offsets/<group>/<topic>-<partition>     # 16 bytes: offset int64 + metadata-len + metadata
```
```go
type OffsetStore struct { dir string; mu sync.RWMutex; cache map[key]commit }
func (s *OffsetStore) Commit(group, topic string, p int32, offset int64, meta string) error  // write file + cache
func (s *OffsetStore) Fetch(group, topic string, p int32) (offset int64, meta string, found bool)
```
Last-write-wins; write is `os.WriteFile` to a temp file + rename (atomic). Loaded into cache on
startup by walking `__offsets/`.

---

## 7. `internal/server` + handlers

### 7.1 Connection loop (`conn.go`)
```
for {
    len  := readInt32(conn)             // frame size
    buf  := readFull(conn, len)
    hdr  := decode RequestHeader v1
    resp := dispatch(hdr.APIKey, hdr.APIVersion, kbytes.Reader(rest))
    out  := INT32(len(corrID+body)) + INT32(corrID) + body
    write(conn, out)
}
```
One goroutine per connection; sequential per connection. Unknown/unsupported `api_key` or a
version outside our range → respond with `UNSUPPORTED_VERSION`(35) where the response shape
allows, else close. `ApiVersions` is answered even for unknown versions (Kafka special-case:
reply v0 body with error 35 so the client can renegotiate).

### 7.2 Dispatch & handlers (`handlers.go`)
A `map[int16]func(version int16, r *kbytes.Reader) []byte` (or a switch). Each handler:
decode request → call broker/storage/group via the small interfaces → encode response. This is
the **only** package that imports protocol, broker, storage, and group together (SRP: it is the
glue; nobody else crosses layers).

Handler responsibilities summary:
- **ApiVersions** → list the table in §2.2.
- **Metadata** → brokers=[self]; for requested topics (nil/empty ⇒ all): if missing and
  auto-create on ⇒ `GetOrCreate`; emit partitions with leader/replicas/isr = [0].
- **Produce** → per partition: split record-set into batches, `Append` each, return base offset
  of the first; honor `acks` (we always behave as acks=1; acks=0 ⇒ no response).
- **Fetch** → per partition: `Read(fetchOffset, partitionMaxBytes)`, set `high_watermark =
  LatestOffset()`, embed record-set; respect top-level `max_wait`/`min_bytes` with a simple
  bounded wait (optional; default return immediately).
- **ListOffsets** → timestamp `-2`⇒`EarliestOffset`, `-1`⇒`LatestOffset`; other timestamps ⇒
  latest (we don't index by time).
- **FindCoordinator** → return self (node 0, advertised host/port).
- **Join/Sync/Heartbeat/Leave/OffsetCommit/OffsetFetch** → delegate to `group.Coordinator`.
- **CreateTopics** → `broker.CreateTopic`; idempotent-ish (existing ⇒ `TOPIC_ALREADY_EXISTS`(36)).

---

## 8. Concurrency rules

- One goroutine per TCP connection; no shared mutable state between connections except the
  broker registry, partition logs, and coordinator — each guarded by its own mutex.
- `storage.Log`: `RWMutex`; `Append` takes write lock, `Read` takes read lock and copies bytes
  out before unlocking (returned slice is owned by caller).
- `broker.Broker`: `RWMutex` over the topic map; topic creation takes the write lock.
- `group.Coordinator`: a top-level mutex for the groups map, plus a per-`group` mutex for member
  changes. Lock order: coordinator → group (never the reverse) to avoid deadlock.
- Background goroutines: flush ticker, retention sweeper, group session reaper — all use the
  same public locking methods, no privileged access.

---

## 9. `config.go`

```go
type Config struct {
    Listeners          string   // KAFKA_LISTENERS,           default "0.0.0.0:9092"
    AdvertisedHost     string   // from KAFKA_ADVERTISED_LISTENERS, default "localhost"
    AdvertisedPort     int32    //                          default 9092
    LogDirs            string   // KAFKA_LOG_DIRS,             default "/var/lib/kafka" (or ./data dev)
    NumPartitions      int32    // KAFKA_NUM_PARTITIONS,       default 1
    SegmentBytes       int32    // KAFKA_SEGMENT_BYTES,        default 64<<20
    IndexIntervalBytes int32    //                          default 4096
    FlushMs            int      // KAFKA_FLUSH_MS,             default 1000
    RetentionMs        int64    // KAFKA_RETENTION_MS,         default 7*24h (0 = disabled)
    RetentionBytes     int64    //                          default 0 (disabled)
    AutoCreateTopics   bool     // KAFKA_AUTO_CREATE_TOPICS,   default true
}
func Load() Config  // precedence: flags > env (KAFKA_*) > defaults
```

---

## 10. Test plan (per package)

- `kbytes`: encode→decode round-trips for every primitive incl. negative lengths/null.
- `record`: parse real franz-go-produced batch bytes; patch base offset; CRC matches a known
  vector; multi-batch record-set iteration.
- `storage`: append/read round-trip; segment roll boundary; sparse-index lookup correctness
  vs. linear scan; restart recovery incl. truncated tail.
- `group`: join→sync→stable transitions; generation bump; reaper eviction; offset
  commit/fetch persistence across `OffsetStore` reopen.
- `protocol`: per-API encode/decode at min and max supported version.
- **Integration (`//go:build integration`)**: boot broker on `:0`, drive with franz-go —
  produce/fetch (plain + snappy), 2-member group split + failover, commit/resume.
