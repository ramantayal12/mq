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

> In cluster mode all topics use the shared `MQ_NUM_PARTITIONS` (custom per-topic counts need a
> controller, which is out of scope), and there is no replication — a down broker's partitions
> are unavailable until it returns. See [docs/HLD.md](docs/HLD.md) §9.

## Configuration

Precedence: command-line flags > `MQ_*` environment variables > defaults.

| Env var | Flag | Default | Meaning |
|---------|------|---------|---------|
| `MQ_NODE_ID` | `--node-id` | `0` | this broker's node id |
| `MQ_BROKERS` | `--brokers` | `""` | static cluster membership `id@host:port,…` (empty = single broker) |
| `MQ_LISTENERS` | `--listen` | `0.0.0.0:9092` | bind address |
| `MQ_ADVERTISED_LISTENERS` | `--advertised-host` / `--advertised-port` | `localhost:9092` | host:port returned in Metadata (clients reconnect here) |
| `MQ_LOG_DIRS` | `--log-dirs` | `./data` | data directory for log segments |
| `MQ_NUM_PARTITIONS` | `--partitions` | `1` | default partitions for auto-created topics |
| `MQ_SEGMENT_BYTES` | `--segment-bytes` | `67108864` | segment roll size |
| `MQ_FLUSH_MS` | `--flush-ms` | `1000` | background fsync interval |
| `MQ_RETENTION_MS` | — | `604800000` | segment age retention (0 = off) |
| `MQ_RETENTION_BYTES` | — | `0` | total-size retention (0 = off) |
| `MQ_AUTO_CREATE_TOPICS` | `--auto-create-topics` | `true` | create unknown topics on demand |

> **`MQ_ADVERTISED_LISTENERS` matters most.** Whatever host:port the broker returns in
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

# After (mq — drop-in replacement, no ZooKeeper needed)
services:
  kafka:
    image: ghcr.io/ramantayal12/mq:latest
    ports:
      - "9092:9092"
    environment:
      MQ_LISTENERS: "0.0.0.0:9092"
      MQ_ADVERTISED_LISTENERS: "localhost:9092"
      MQ_NUM_PARTITIONS: "3"
      MQ_AUTO_CREATE_TOPICS: "true"
    volumes:
      - mq-data:/var/lib/mq
```

> Remove the ZooKeeper service entirely — `mq` does not need it.

### Replace in Kubernetes

Update the container image in your Deployment or StatefulSet:

```yaml
containers:
  - name: kafka
    image: ghcr.io/ramantayal12/mq:0.1.0   # was: confluentinc/cp-kafka:7.6.0
    ports:
      - containerPort: 9092
    env:
      - name: MQ_LISTENERS
        value: "0.0.0.0:9092"
      - name: MQ_ADVERTISED_LISTENERS
        value: "$(POD_IP):9092"
      - name: MQ_NUM_PARTITIONS
        value: "3"
```

### Environment variable mapping (Kafka → mq)

| Kafka config | mq equivalent | Notes |
|---|---|---|
| `KAFKA_LISTENERS` | `MQ_LISTENERS` | Bind address, omit the `PLAINTEXT://` prefix |
| `KAFKA_ADVERTISED_LISTENERS` | `MQ_ADVERTISED_LISTENERS` | Host:port clients reconnect to |
| `KAFKA_LOG_DIRS` | `MQ_LOG_DIRS` | Data directory (default `/var/lib/mq`) |
| `KAFKA_NUM_PARTITIONS` | `MQ_NUM_PARTITIONS` | Default partitions for auto-created topics |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `MQ_AUTO_CREATE_TOPICS` | `true` / `false` |
| `KAFKA_LOG_SEGMENT_BYTES` | `MQ_SEGMENT_BYTES` | Segment roll size in bytes |
| `KAFKA_LOG_RETENTION_MS` | `MQ_RETENTION_MS` | Segment age retention (0 = off) |
| `KAFKA_LOG_RETENTION_BYTES` | `MQ_RETENTION_BYTES` | Total-size retention (0 = off) |

> **What works unchanged**: Any Kafka client library (franz-go, sarama, librdkafka, kafka-python,
> confluent-kafka, etc.) connects to `mq` without code changes — just point the bootstrap server
> to the `mq` broker address.

## Design docs

- [docs/HLD.md](docs/HLD.md) — high-level design.
- [docs/LLD.md](docs/LLD.md) — package contracts, on-disk byte layouts, algorithms.
- [codeindex.md](codeindex.md) — navigation map of the codebase.

## Scope / non-goals

No replication or multi-broker clustering, no transactions / idempotent producer, no
exactly-once, no SASL/TLS, no KRaft/ZooKeeper, no log compaction, no quotas. See
[docs/HLD.md](docs/HLD.md) §8.
