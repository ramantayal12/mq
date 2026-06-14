# Functional tests

Black-box, end-to-end tests that drive **real `mqbroker` processes** over the Kafka wire
protocol with [franz-go](https://github.com/twmb/franz-go) — the same path a production
Kafka client takes. (The in-process wire-compat tests live under
[internal/server](../internal/server); these compile and spawn the binary instead.)

> No Kafka CLI binaries (`kafka-console-producer.sh`, etc.) are installed in this repo, so
> the suite is written against franz-go rather than shelling out to them.

All code is behind the `functional` build tag, so `go build ./...` / `go test ./...` ignore
it.

## Layout (one responsibility per package)

| Path | Responsibility |
|------|----------------|
| [harness/](harness/) | Shared, reusable helpers (no `testing` dependency): `build.go` compiles the binary + allocates ports; `broker.go` spawns/targets a single node; `cluster.go` spawns a real N-node RF Raft cluster and can `Kill` a node; `load.go` generates sustained real-time load; `metrics.go` scrapes/parses `/metrics`. |
| [smoke/](smoke/) | Fast correctness: produce/consume round-trip, consumer-group commit/resume, `/metrics` exposes the `kafka_*` series. |
| [load/](load/) | Sustained, bursty, multi-topic/multi-group load to populate dashboards; asserts traffic flowed and per-topic + `kafka_consumer_group_lag` series exist. |
| [failover/](failover/) | 3-node RF=3 cluster; kills a broker mid-flight and proves committed data stays readable and new writes still succeed after leader re-election. |

## Run

```bash
./test/run.sh            # all suites against throwaway brokers
./test/run.sh smoke      # one suite (smoke | load | failover)
# or directly:
go test -tags functional ./test/smoke/ ./test/load/ ./test/failover/
```

`smoke` and `load` build + spawn a single broker per package; `failover` spawns its own
3-node cluster. Each waits for readiness before testing and tears everything down after.

## Watch load in Grafana

Spawned brokers use ephemeral ports, so to see traffic on the bundled dashboards, run the
load suite against the docker-compose stack:

```bash
docker compose up --build -d                 # broker :9092, Prometheus :9095, Grafana :3005
KAFKA_FUNC_ADDR=localhost:9092 \
KAFKA_FUNC_METRICS_URL=http://localhost:7080/metrics \
LOAD_DURATION=3m \
  ./test/run.sh load
# open http://localhost:3005  (admin / admin)
```

`KAFKA_FUNC_ADDR` makes the `smoke` and `load` suites target an existing broker instead of
spawning one. `LOAD_DURATION` (e.g. `2m`, `5m`) controls how long the load suite runs
(default 20s).
