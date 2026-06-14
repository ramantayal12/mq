# Plan — Object-Storage Backend (GCS, MinIO-testable), AutoMQ-style

Status: proposal. Goal: let `mq` store partition data in object storage (GCS in prod,
MinIO locally) instead of — or behind — the local segmented files, borrowing AutoMQ's
shared-storage design. This is a design + phased implementation plan, not yet code.

---

## 1. Where we are today

Storage today is a per-partition, append-only segmented commit log on the local
filesystem ([internal/storage/log.go](../internal/storage/log.go),
[segment.go](../internal/storage/segment.go)):

- One `*storage.Log` per topic-partition, rooted at `<data-dir>/<topic>-<partition>/`.
- Each log is a list of `segment`s (`NNN.log` + `NNN.index` file pairs), 64 MiB default roll.
- Durability = async `fsync` (the `FlushMs` ticker). Crash window is one flush interval.
- The broker holds a log only for partitions it leads/follows; durability across nodes
  comes from **ISR replication** over Raft-driven placement (phases 1–6, see
  [GAPS_PLAN.md](GAPS_PLAN.md) and the memory index).

The handlers and broker depend on `*storage.Log` only through two narrow interfaces —
`Appender` (`Append`) and `Reader` (`Read`, `EarliestOffset`, `LatestOffset`) — plus the
HWM/replica methods. **That narrow surface is what makes this swap tractable.**

The full method surface a backend must satisfy:

| Method | Used by |
|---|---|
| `Append(batch) (base, err)` | produce path |
| `AppendReplica(batch)` | follower fetch (replication) |
| `Read(offset, maxBytes)` | consumer fetch |
| `HighWatermark` / `SetHighWatermark` / `HoldHighWatermark` | replication, acks=all |
| `OffsetForTimestamp(ts)` | ListOffsets-by-time |
| `EarliestOffset` / `LatestOffset` | ListOffsets, metadata |
| `Flush` | flush ticker |
| `EnforceRetention(maxAgeMs, maxBytes)` | retention ticker |
| `Open(dir, cfg)` / `Close` | broker lifecycle |

## 2. What we borrow from AutoMQ

AutoMQ replaces Kafka's local-disk + ISR model with a **shared storage** model on top of
S3/GCS. The pieces, and how each maps onto `mq`:

| AutoMQ concept | What it does | `mq` mapping |
|---|---|---|
| **Stream** | Append-only offset-addressed sequence | One topic-partition log |
| **WAL** (Delta WAL) | Small local/EBS write-ahead log; absorbs writes durably with low latency before they reach S3 | Local append-only `wal/` file per node (keep what we have, repurposed) |
| **Stream Set Object** (SSO) | One S3 object batching recent data from *many* streams; uploaded frequently | One GCS object per upload flush, holding batches from many partitions |
| **Stream Object** | Per-stream object produced by compaction | Compacted per-partition object covering a wide offset range |
| **Object metadata** | Which object holds which `[stream, offsetRange]`; lives in the KRaft controller | An **object index** (offset-range → object key + position), in the Raft FSM or a manifest |
| **LogCache / BlockCache** | In-memory write buffer + read-through cache | In-memory `pending` buffer (pre-upload) + LRU read cache |
| **Compaction** | Merge many small SSOs into fewer, larger, per-stream objects | Background compactor goroutine |

The strategic payoff AutoMQ gets, and what we are buying:

- **Durability is the object store's job** (GCS = 11 nines). With data in GCS, cross-node
  ISR replication of *data* becomes optional — brokers can become near-stateless and
  partition reassignment needs no data movement.
- **Cost / elasticity**: object storage is cheap and infinite; no per-broker disk sizing.

We will **not** rip out ISR replication in this plan. We introduce the object backend
behind the existing interface first; making brokers stateless (the bigger AutoMQ win) is
called out as a follow-on in §7, gated on the backend landing.

## 3. Local-vs-prod object client decision

MinIO speaks the **S3 API**. GCS exposes an **S3-compatible XML/interop endpoint**
(`storage.googleapis.com`, HMAC keys) *and* a native JSON API. To keep one code path that
runs against both MinIO (local) and GCS (prod), the recommendation is:

> Define a small internal `ObjectStore` interface (`Put`, `Get(range)`, `List`, `Delete`)
> and implement it once with an **S3-compatible client** (`aws-sdk-go-v2` or `minio-go`),
> pointed at MinIO locally and at GCS's S3-interop endpoint in prod via config
> (`endpoint`, `accessKey`, `secretKey`, `bucket`). A native-GCS implementation can be
> added later behind the same interface if we want GCS JSON-API features.

`minio-go` is the lightest dependency and supports ranged GETs cleanly; `aws-sdk-go-v2`
is heavier but more standard. **Recommend `minio-go`** for the smaller surface. (Decision
to confirm before Phase 1.)

This keeps the local story exactly what the user asked for: **MinIO for dev, GCS for prod,
identical code.**

## 4. Target design

### 4.1 Object layout in the bucket

```
<bucket>/
  wal/<node-id>/<seq>            # optional: WAL segments mirrored to object store
  data/<topic>/<partition>/
      sso/<uploadSeq>           # stream-set objects: recent batches, many partitions
      obj/<baseOffset>          # compacted per-partition objects
  meta/<topic>/<partition>/manifest.json   # object index (if not in Raft FSM)
```

Each object is a concatenation of **opaque record-batch bytes** — the same blobs the log
stores today. We keep the "batch as opaque blob" invariant (HLD §3): an object is just a
byte range we can `Read`, with an index telling us which offset lives at which position.

### 4.2 The write path (`Append`)

1. Patch base offset + CRC (unchanged from today's `Log.Append`).
2. Append the batch to the **local WAL** file and to an in-memory `pending` buffer; assign
   the offset and advance the LEO. **Durability point** = WAL append (fsync per `FlushMs`,
   or per-batch for acks=all). Return to the producer here — object upload is async.
3. A background **uploader** flushes `pending` to the object store when it crosses a size
   (e.g. 8–16 MiB) or time (e.g. 250 ms) threshold, writing one SSO and recording
   `[offsetRange] → objectKey@position` in the object index. On success it trims the WAL.

### 4.3 The read path (`Read(offset, maxBytes)`)

1. If `offset` is still in the `pending` buffer / WAL (not yet uploaded) → serve from memory.
2. Else look up the object index for the object covering `offset`, ranged-GET the byte
   span, slice whole batches up to `maxBytes` (the same logic as
   `segment.readBatchesFrom`), and populate the **read cache**.
3. `OffsetForTimestamp`, `EarliestOffset`, `LatestOffset` answer from the object index +
   in-memory state — no object fetch needed for the common cases.

### 4.4 The object index (metadata)

The index maps offset ranges to objects. Options:

- **A. In the Raft FSM** (we already run `hashicorp/raft`). Faithful to AutoMQ (metadata in
  the controller), survives node loss, but adds Raft write volume per upload.
- **B. Per-partition manifest object** in the bucket (`meta/.../manifest.json`),
  read on open, updated on upload/compaction. Simpler, no Raft coupling, but needs
  careful atomic-update / fencing.

**Recommend B for Phase 1** (decouples the backend from the controller, easier to test on
MinIO), with a note that A is the long-term home for the stateless-broker work in §7.

### 4.5 Retention & compaction

- `EnforceRetention` deletes whole objects below the age/size cutoff via `ObjectStore.Delete`
  and drops their index entries — the object-store analogue of dropping old segments.
- A **compactor** goroutine merges many small SSOs into larger per-partition objects
  (AutoMQ "stream objects"), bounding object count and read amplification.

## 5. Code shape

Introduce a backend seam so the existing file log and the new object log are
interchangeable, selected by config. Keep the change surgical — handlers/broker keep
talking to the same interface.

```
internal/storage/
  log.go            # existing local backend (unchanged)
  segment.go        #   "
  backend.go        # NEW: Backend interface = the method table in §1; LocalLog implements it
  object/
    store.go        # ObjectStore interface + minio-go impl (MinIO + GCS-interop)
    log.go          # ObjectLog: WAL + pending + uploader + read cache, implements Backend
    index.go        # object index (offset-range -> objectKey@pos), manifest load/save
    compactor.go    # background SSO compaction
```

`broker.openLocked` switches on config to construct either a local `*storage.Log` or an
`*object.ObjectLog`. Everything downstream is interface-typed.

### Config additions ([internal/config/config.go](../internal/config/config.go))

| Flag / env | Meaning |
|---|---|
| `--storage-backend` / `MQ_STORAGE_BACKEND` | `local` (default) or `object` |
| `MQ_OBJECT_ENDPOINT` | `http://localhost:9000` (MinIO) or `https://storage.googleapis.com` (GCS) |
| `MQ_OBJECT_BUCKET` | bucket name |
| `MQ_OBJECT_ACCESS_KEY` / `MQ_OBJECT_SECRET_KEY` | MinIO creds / GCS HMAC keys |
| `MQ_OBJECT_REGION` | region (GCS interop wants one set) |
| `MQ_OBJECT_UPLOAD_BYTES` / `MQ_OBJECT_UPLOAD_MS` | uploader flush thresholds |

Defaulting `MQ_STORAGE_BACKEND=local` means **zero behavior change** unless opted in.

## 6. Phased implementation & verification

Each phase is independently shippable and verifiable (CLAUDE.md §4).

1. **Backend seam.** Extract a `Backend` interface; make `*storage.Log` implement it;
   route the broker through it. → *verify:* full existing test suite passes unchanged
   (`go test ./...`), `local` is the default.
2. **ObjectStore + MinIO harness.** `ObjectStore` interface + `minio-go` impl; a
   `docker-compose` MinIO service; a test that puts/ranged-gets/lists/deletes. → *verify:*
   integration test green against a real local MinIO container.
3. **ObjectLog write path.** WAL + pending + uploader + object index (manifest). Implement
   `Append`/`Flush`/`LatestOffset`/`EarliestOffset`. → *verify:* produce N batches, restart
   the log, confirm offsets recovered from WAL + manifest; objects appear in MinIO.
4. **ObjectLog read path.** `Read` from pending → object (ranged GET) → cache;
   `OffsetForTimestamp`. → *verify:* round-trip produce/consume through `mqbroker` with a
   franz-go client and `MQ_STORAGE_BACKEND=object` against MinIO; bytes identical to local.
5. **Replication methods + retention.** `AppendReplica`, HWM methods, `EnforceRetention`,
   compactor. → *verify:* existing replication/acks-all integration tests pass with the
   object backend; old objects get deleted; small objects get compacted.
6. **Parity gate.** Run the whole suite with `MQ_STORAGE_BACKEND=object`. → *verify:*
   green, plus a soak produce/consume to confirm no offset gaps or data loss across an
   uploader flush boundary and a simulated crash (kill before upload → WAL recovers it).

## 7. Follow-on (out of scope here): stateless brokers

The full AutoMQ payoff — drop data-plane ISR replication, make partition reassignment a
pure metadata move — depends on (a) the object backend from this plan and (b) moving the
object index into the Raft FSM (§4.4 option A) so any broker can serve any partition from
GCS. Tracked separately; **not** part of this plan to keep the blast radius small.

## 8. Open decisions to confirm before coding

1. Object client: **`minio-go`** (recommended) vs `aws-sdk-go-v2`.
2. Object index home for Phase 1: **manifest object** (recommended) vs Raft FSM.
3. Do we keep ISR replication on with the object backend (recommended, safe) or treat GCS
   as the sole durability source from day one (bigger change, §7)?
4. WAL: reuse the existing local segment files as the WAL, or a dedicated append-only WAL
   file per node?

---

## 9. Executable breakdown

Granular, ordered steps. Each is small, has an exact verify command, and leaves the tree
green. `M` = code change, `T` = test, `I` = infra. Default stays `local` until step 6.4, so
every step before that is provably non-breaking.

### Review notes that shape these steps
- The handlers already expose `logHandle` ([handlers.go:678](../internal/server/handlers.go#L678))
  — most of the read/append surface. Reuse/extend it; don't invent a parallel interface.
- The seam must cover **three** consumers of `*storage.Log`, not just the broker:
  the broker's 6 signatures (`replicaLog`, `getLog`, `LocalLog`, `snapshotLogs`,
  `openLocked`, and the `logs` map), the **replication** package
  (`LogResolver` + `AppendReplica`/`SetHighWatermark`/`LatestOffset`,
  [replication.go:42](../internal/replication/replication.go#L42)), and `EnforceRetention`
  called in [broker.go:677](../internal/broker/broker.go#L677).
- Consumer-group offsets (`group.NewOffsetStore(cfg.LogDirs)`,
  [main.go:39](../cmd/mqbroker/main.go#L39)) are **separate** from partition logs and stay
  local — out of scope for this swap.

### Phase 1 — Backend seam (no new behavior)

- **1.1 (M)** Add `storage.Backend` interface in a new `internal/storage/backend.go` =
  the exact method table from §1 (`Append, AppendReplica, Read, HighWatermark,
  SetHighWatermark, HoldHighWatermark, OffsetForTimestamp, EarliestOffset, LatestOffset,
  Flush, EnforceRetention, Close`). Add `var _ Backend = (*Log)(nil)`.
  → *verify:* `go build ./...` compiles; assertion proves `*Log` satisfies it.
- **1.2 (M)** Change the broker's `logs` map and the 6 method signatures from
  `*storage.Log` to `storage.Backend`. Leave `storage.Open` returning `*Log`; assign into
  the interface at the call site in `openLocked`.
  → *verify:* `go build ./... && go vet ./...`.
- **1.3 (M)** Change `replication.LogResolver` to return `storage.Backend`.
  → *verify:* `go build ./...`.
- **1.4 (T)** No new tests. → *verify:* `go test ./...` fully green, unchanged behavior.

### Phase 2 — ObjectStore + MinIO harness

- **2.1 (M)** Add `internal/storage/object/store.go`: `ObjectStore` interface
  (`Put(key, []byte)`, `Get(key, off, len) []byte`, `List(prefix) []string`, `Delete(key)`)
  + `minio-go` impl reading `MQ_OBJECT_*` config. `go get github.com/minio/minio-go/v7`.
  → *verify:* `go build ./...`.
- **2.2 (I)** Add a `minio` service to [docker-compose.yml](../docker-compose.yml) and a
  one-line bucket-create init. → *verify:* `docker compose up minio` healthy; bucket exists.
- **2.3 (T)** Integration test `store_integration_test.go` (build-tagged `//go:build integration`):
  put → ranged-get → list → delete against `MQ_OBJECT_ENDPOINT=localhost:9000`.
  → *verify:* `go test -tags=integration ./internal/storage/object/ -run Store`.

### Phase 3 — ObjectLog write path

- **3.1 (M)** `object/index.go`: in-memory offset-range → `objectKey@pos` index +
  JSON manifest load/save (`meta/<topic>/<partition>/manifest.json`).
  → *verify:* unit test round-trips a manifest through `ObjectStore`.
- **3.2 (M)** `object/log.go`: `ObjectLog` with WAL file + `pending` buffer; implement
  `Append` (patch offset/CRC, WAL-append, advance LEO), `Flush`, `LatestOffset`,
  `EarliestOffset`, `HighWatermark`/`SetHighWatermark`/`HoldHighWatermark`, `Close`.
  Stub `Read`/`AppendReplica`/`OffsetForTimestamp`/`EnforceRetention` to land in later phases.
  Add `var _ storage.Backend = (*ObjectLog)(nil)`.
  → *verify:* `go build ./...`.
- **3.3 (M)** Uploader goroutine: flush `pending`→SSO on `MQ_OBJECT_UPLOAD_BYTES`/`_MS`,
  update the index, trim WAL on success.
  → *verify (T):* produce N batches, assert objects appear in MinIO and manifest covers
  `[0, N)`.
- **3.4 (T)** Recovery test: write, drop the `ObjectLog`, reopen → LEO/earliest recovered
  from manifest + WAL tail. → *verify:* `go test -tags=integration ./internal/storage/object/ -run Recover`.

### Phase 4 — ObjectLog read path

- **4.1 (M)** Implement `Read`: serve from `pending`/WAL if not yet uploaded; else index
  lookup → ranged `Get` → slice whole batches up to `maxBytes` (mirror
  `segment.readBatchesFrom`); fill an LRU read cache. Implement `OffsetForTimestamp`.
  → *verify:* `go build ./...`.
- **4.2 (T)** Byte-parity test: same batches through `*storage.Log` and `*ObjectLog` return
  **identical** `Read` bytes across an upload-flush boundary.
  → *verify:* `go test -tags=integration ./internal/storage/object/ -run Parity`.
- **4.3 (T)** End-to-end: run `mqbroker` with `MQ_STORAGE_BACKEND=object` against MinIO,
  produce+consume with a franz-go client. → *verify:* consumed records == produced.

### Phase 5 — Replication, retention, compaction

- **5.1 (M)** Implement `AppendReplica` (preserve leader offset, no CRC recompute; same
  dup/gap rules as [log.go:148](../internal/storage/log.go#L148)).
  → *verify (T):* the existing replication integration test passes with the object backend.
- **5.2 (M)** Implement `EnforceRetention`: delete objects fully below the age/size cutoff
  via `ObjectStore.Delete`, prune their index entries.
  → *verify (T):* old objects gone from MinIO; earliest offset advances.
- **5.3 (M)** `object/compactor.go`: merge small SSOs into per-partition objects, rewrite
  index atomically. → *verify (T):* object count drops; `Read` parity preserved post-compaction.

### Phase 6 — Wire-up & parity gate

- **6.1 (M)** Add `MQ_STORAGE_BACKEND` + `MQ_OBJECT_*` to
  [config.go](../internal/config/config.go) (default `local`).
  → *verify:* `go test ./internal/config/`.
- **6.2 (M)** `broker.openLocked` constructs `*ObjectLog` when backend=`object`, else
  `*storage.Log`. → *verify:* `go build ./...`.
- **6.3 (I)** Document the MinIO/GCS run recipes in the README / compose.
  → *verify:* a fresh `docker compose up` + produce/consume works on the object backend.
- **6.4 (T)** **Parity gate:** run the whole suite against the object backend
  (`MQ_STORAGE_BACKEND=object go test -tags=integration ./...`), plus a soak that kills the
  broker before an uploader flush and confirms WAL recovery (no offset gaps, no loss).
  → *verify:* green suite + clean soak.

### Suggested PR slicing
P1 (steps 1.x) → P2 (2.x) → P3 (3.x) → P4 (4.x) → P5 (5.x) → P6 (6.x). Each PR is
green and, through 6.1, default-`local` so it ships dark. 6.2–6.4 flip the switch.
