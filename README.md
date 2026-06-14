# mq — a Kafka-wire-compatible message queue in Go

`mq` is a single-process message broker that speaks the **real Apache Kafka TCP wire
protocol**. Existing Kafka clients (franz-go, sarama, librdkafka, kafka-python, the
`kafka-console-*` scripts) connect to it **unmodified** and can produce, consume, and run
consumer groups. It is built from scratch on the Go standard library to demonstrate Kafka's
core mechanics — the binary protocol, a segmented on-disk log, partitions, and consumer
groups — without clustering, replication, or transactions.

## Features

- Real Kafka wire protocol (Produce, Fetch, Metadata, ListOffsets, consumer-group APIs, …).
- Multiple topics, each with multiple partitions.
- **Horizontal scaling**: run several brokers as a static-membership cluster — partitions are
  spread across brokers and clients route to the partition leader (replication factor 1).
- Multiple consumers per topic via **consumer groups** with server-side join/sync/heartbeat.
- **Committed offsets** persisted to disk, so restarted consumers resume.
- **Segmented append-only log** on disk (SSD storage) with a sparse offset index, async
  flushing, crash recovery, and basic age/size retention.
- Transparent producer compression (gzip/snappy/lz4/zstd) — batches are stored opaquely.

## Quick start

### Run locally

```bash
go run ./cmd/mqbroker --listen 0.0.0.0:9092 --advertised-host localhost --advertised-port 9092 --log-dirs ./data
```

### Run with Docker (Kafka-style image)

```bash
docker compose up --build
# broker is now on localhost:9092
```

### Try it with the Kafka console tools

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --from-beginning --group g1
```

### Connect a client that runs in Docker

When your client runs in its own container (not on the host), the broker and client must share
a Docker network, and `KAFKA_ADVERTISED_LISTENERS` must be the broker's **service name** — not
`localhost`. A client connects to the bootstrap address, then the broker returns its advertised
address in Metadata and the client reconnects there. If that's `localhost`, the client reconnects
to *its own* container and fails. Point it at the service name (`kafka`) so it resolves over the
shared network.

Add your client to the broker's compose network. The official Kafka image's console tools work
unmodified:

```yaml
# docker-compose.yml — broker advertises its service name to other containers
services:
  kafka:
    image: ghcr.io/ramantayal12/mq:latest
    environment:
      KAFKA_LISTENERS: "0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "kafka:9092"   # service name, reachable on kafka-net
      KAFKA_AUTO_CREATE_TOPICS: "true"
    networks: [kafka-net]

  # Any Kafka client image. Here, the console tools shipped with Apache Kafka.
  client:
    image: apache/kafka:3.7.0
    depends_on:
      kafka:
        condition: service_healthy        # waits for the in-binary healthcheck
    networks: [kafka-net]
    entrypoint: ["sleep", "infinity"]

networks:
  kafka-net:
```

```bash
docker compose up -d
# produce/consume from inside the client container, addressing the broker by service name:
docker compose exec client /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic demo
docker compose exec client /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic demo --from-beginning --group g1
```

> mq advertises a **single** address, so pick the one your clients use. If you advertise
> `kafka:9092` (for in-container clients) but also want to reach the broker from the host, map
> the port and add `127.0.0.1 kafka` to the host's `/etc/hosts` so `kafka:9092` resolves there
> too — mq has no dual-listener support.

## Observability (Metrics & Dashboards)

`mq` is instrumented with Prometheus metrics covering requests, latency, offsets, and consumer group lag.
When running with `docker compose up --build`, a full observability stack is launched alongside the broker:

- **Prometheus** scrapes the broker every 5s (accessible at `http://localhost:9095`).
- **Grafana** provides pre-built dashboards (accessible at `http://localhost:3005`, login as `admin`/`admin` or view anonymously).
- The raw metrics endpoint is available on the broker at `http://localhost:7080/metrics`.

### Health check

The broker binary ships a Kafka ApiVersions probe used as the container `HEALTHCHECK`
(the distroless image has no shell/curl). It performs the same handshake every Kafka client
does first and exits non-zero if the broker isn't serving:

```bash
mqbroker healthcheck            # probes 127.0.0.1:9092 by default
mqbroker healthcheck host:9092  # probe a specific address
```

`docker compose up` reports the broker as `healthy` once the probe succeeds
(`docker compose ps`).

## Running a cluster (horizontal scaling)

Give every broker the **same** member list and a unique node id. Partitions are placed
deterministically across the brokers, so a client seeded with any one broker discovers the rest
and routes to the right leader.

```bash
# 3 brokers on one host (different ports + data dirs)
go run ./cmd/mqbroker --node-id 0 --brokers "0@localhost:9092,1@localhost:9093,2@localhost:9094" --listen :9092 --advertised-port 9092 --log-dirs ./data0 &
go run ./cmd/mqbroker --node-id 1 --brokers "0@localhost:9092,1@localhost:9093,2@localhost:9094" --listen :9093 --advertised-port 9093 --log-dirs ./data1 &
go run ./cmd/mqbroker --node-id 2 --brokers "0@localhost:9092,1@localhost:9093,2@localhost:9094" --listen :9094 --advertised-port 9094 --log-dirs ./data2 &
# any client: --bootstrap-server localhost:9092 (it learns all three)
```

A multi-container example is in [docker-compose.cluster.yml](docker-compose.cluster.yml)
(`docker compose -f docker-compose.cluster.yml up --build`).

> In cluster mode all topics use the shared `KAFKA_NUM_PARTITIONS` (custom per-topic counts need a
> controller, which is out of scope), and there is no replication — a down broker's partitions
> are unavailable until it returns. See [docs/HLD.md](docs/HLD.md) §9.

## Configuration

Precedence: command-line flags > `KAFKA_*` environment variables > defaults.

| Env var | Flag | Default | Meaning |
|---------|------|---------|---------|
| `KAFKA_NODE_ID` | `--node-id` | `0` | this broker's node id |
| `KAFKA_BROKERS` | `--brokers` | `""` | static cluster membership `id@host:port,…` (empty = single broker) |
| `KAFKA_LISTENERS` | `--listen` | `0.0.0.0:9092` | bind address |
| `KAFKA_ADVERTISED_LISTENERS` | `--advertised-host` / `--advertised-port` | `localhost:9092` | host:port returned in Metadata (clients reconnect here) |
| `KAFKA_LOG_DIRS` | `--log-dirs` | `./data` | data directory for log segments |
| `KAFKA_NUM_PARTITIONS` | `--partitions` | `1` | default partitions for auto-created topics |
| `KAFKA_REPLICATION_FACTOR`| `--replication-factor`| `1` | replicas per partition in cluster mode |
| `KAFKA_RAFT_BOOTSTRAP` | `--raft-bootstrap` | `false` | bootstraps the Raft metadata controller quorum |
| `KAFKA_SEGMENT_BYTES` | `--segment-bytes` | `67108864` | segment roll size |
| `KAFKA_FLUSH_MS` | `--flush-ms` | `1000` | background fsync interval |
| `KAFKA_RETENTION_MS` | — | `604800000` | segment age retention (0 = off) |
| `KAFKA_RETENTION_BYTES` | — | `0` | total-size retention (0 = off) |
| `KAFKA_AUTO_CREATE_TOPICS` | `--auto-create-topics` | `true` | create unknown topics on demand |
| `KAFKA_METRICS_ADDR` | `--metrics-addr` | `:7080` | bind address for Prometheus metrics endpoint |
| `KAFKA_STORAGE_BACKEND` | `--storage-backend` | `local` | storage backend: `local` (disk) or `object` (S3/GCS) |
| `KAFKA_OBJECT_ENDPOINT` | — | `""` | S3/GCS API endpoint |
| `KAFKA_OBJECT_BUCKET` | — | `kafka-data` | Object storage bucket name |
| `KAFKA_OBJECT_ACCESS_KEY`| — | `""` | Access key ID / HMAC access key |
| `KAFKA_OBJECT_SECRET_KEY`| — | `""` | Secret key / HMAC secret key |
| `KAFKA_OBJECT_REGION` | — | `us-east-1` | Cloud region |
| `KAFKA_OBJECT_UPLOAD_BYTES`| — | `8388608` | Segment upload size threshold (8MB) |
| `KAFKA_OBJECT_UPLOAD_MS` | — | `250` | Segment upload latency threshold (250ms) |

> **`KAFKA_ADVERTISED_LISTENERS` matters most.** Whatever host:port the broker returns in
> Metadata is where clients reconnect for produce/fetch — it must be reachable from the client.


## Testing

```bash
go test ./...                      # fast unit tests (codec, record, storage)
go test -tags integration ./...    # drives the broker with the real franz-go client
```

## Using the GHCR image (drop-in Kafka replacement)

Pre-built multi-arch images (`linux/amd64` and `linux/arm64`) are published to
GitHub Container Registry on every push to `main` and on every semver tag.

### Pull the image

```bash
docker pull ghcr.io/ramantayal12/mq:latest      # latest from main
docker pull ghcr.io/ramantayal12/mq:0.1.0        # pinned version
```

### Replace Kafka in docker-compose

Swap the Kafka service image in your existing `docker-compose.yml`:

#### Option A: Local Disk Storage Backend

```yaml
# Before (Apache Kafka / Confluent)
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_NUM_PARTITIONS: "3"

# After (mq — drop-in replacement, no ZooKeeper/KRaft controller needed)
services:
  kafka:
    image: ghcr.io/ramantayal12/mq:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_LISTENERS: "0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "localhost:9092"
      KAFKA_NUM_PARTITIONS: "3"
      KAFKA_AUTO_CREATE_TOPICS: "true"
    volumes:
      - kafka-data:/var/lib/kafka
```

#### Option B: Tiered Storage (Object-Backed) Backend

To run `mq` with the AutoMQ-style **tiered storage backend** (S3/GCS or MinIO compatible), configure the object store credentials and endpoint:

```yaml
services:
  kafka:
    image: ghcr.io/ramantayal12/mq:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_LISTENERS: "0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "localhost:9092"
      KAFKA_NUM_PARTITIONS: "3"
      KAFKA_AUTO_CREATE_TOPICS: "true"
      # Tiered Object Storage config
      KAFKA_STORAGE_BACKEND: "object"
      KAFKA_OBJECT_ENDPOINT: "https://storage.googleapis.com" # Or S3 endpoint
      KAFKA_OBJECT_BUCKET: "my-gcs-mq-bucket"
      KAFKA_OBJECT_ACCESS_KEY: "GCS_HMAC_ACCESS_KEY"
      KAFKA_OBJECT_SECRET_KEY: "GCS_HMAC_SECRET_KEY"
      KAFKA_OBJECT_REGION: "us-east-1"
      KAFKA_OBJECT_UPLOAD_BYTES: "8388608" # 8MB threshold
      KAFKA_OBJECT_UPLOAD_MS: "250" # 250ms latency threshold
    volumes:
      - kafka-wal:/var/lib/kafka/wal # Local WAL directory for durability
```

> **Note:** When using either backend, you can remove the ZooKeeper service entirely — `mq` handles coordination locally or via its own embedded Raft controller without ZooKeeper.


### Replace in Kubernetes

Update the container image in your Deployment or StatefulSet:

```yaml
containers:
  - name: kafka
    image: ghcr.io/ramantayal12/mq:0.1.0   # was: confluentinc/cp-kafka:7.6.0
    ports:
      - containerPort: 9092
    env:
      - name: KAFKA_LISTENERS
        value: "0.0.0.0:9092"
      - name: KAFKA_ADVERTISED_LISTENERS
        value: "$(POD_IP):9092"
      - name: KAFKA_NUM_PARTITIONS
        value: "3"
```

### Environment variable mapping (Kafka → mq)

`mq` uses Kafka-style `KAFKA_*` env var names, so most config carries over verbatim. The
table below lists the few names that differ from Apache Kafka/Confluent and the value-format
caveats.

| Kafka config | mq env var | Notes |
|---|---|---|
| `KAFKA_LISTENERS` | `KAFKA_LISTENERS` | Same name; omit the `PLAINTEXT://` prefix in the value |
| `KAFKA_ADVERTISED_LISTENERS` | `KAFKA_ADVERTISED_LISTENERS` | Same name; host:port clients reconnect to (no `PLAINTEXT://`) |
| `KAFKA_LOG_DIRS` | `KAFKA_LOG_DIRS` | Same name; data directory (default `/var/lib/kafka`) |
| `KAFKA_NUM_PARTITIONS` | `KAFKA_NUM_PARTITIONS` | Same name; default partitions for auto-created topics |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `KAFKA_AUTO_CREATE_TOPICS` | Drops the `_ENABLE` suffix; `true` / `false` |
| `KAFKA_LOG_SEGMENT_BYTES` | `KAFKA_SEGMENT_BYTES` | Drops the `LOG_` prefix; segment roll size in bytes |
| `KAFKA_LOG_RETENTION_MS` | `KAFKA_RETENTION_MS` | Drops the `LOG_` prefix; segment age retention (0 = off) |
| `KAFKA_LOG_RETENTION_BYTES` | `KAFKA_RETENTION_BYTES` | Drops the `LOG_` prefix; total-size retention (0 = off) |
| — | `KAFKA_STORAGE_BACKEND` | mq-specific; `local` (default) or `object` |
| — | `KAFKA_OBJECT_ENDPOINT` | mq-specific; object storage connection endpoint |


> **What works unchanged**: Any Kafka client library (franz-go, sarama, librdkafka, kafka-python,
> confluent-kafka, etc.) connects to `mq` without code changes — just point the bootstrap server
> to the `mq` broker address.

## Design docs

- [docs/HLD.md](docs/HLD.md) — high-level design.
- [docs/LLD.md](docs/LLD.md) — package contracts, on-disk byte layouts, algorithms.
- [codeindex.md](codeindex.md) — navigation map of the codebase.

## Scope / non-goals

- **In Scope**: Raft-replicated metadata controller (similar to KRaft), in-sync replica (ISR) partition replication, and tiered object storage (GCS/S3) backend.
- **Out of Scope / Gaps**: Transactions / idempotent producer tracking (parsed but not enforced), Exactly-Once Semantics, SASL/TLS authentication, access control lists (ACLs), log compaction, and quotas.

