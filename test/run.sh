#!/usr/bin/env bash
# Functional test runner for the mq broker.
#
#   ./test/run.sh                  # build + spawn throwaway brokers, run smoke+load+failover
#   ./test/run.sh smoke            # run a single suite (smoke | load | failover)
#
# Drive a long load run against a running docker-compose stack and watch Grafana:
#   docker compose up --build -d
#   MQ_FUNC_ADDR=localhost:9092 \
#   MQ_FUNC_METRICS_URL=http://localhost:7080/metrics \
#   LOAD_DURATION=3m \
#     ./test/run.sh load
set -euo pipefail

cd "$(dirname "$0")/.."

suite="${1:-all}"
case "$suite" in
  smoke|load|failover) pkgs="./test/$suite/" ;;
  all)                 pkgs="./test/smoke/ ./test/load/ ./test/failover/" ;;
  *) echo "usage: $0 [smoke|load|failover|all]" >&2; exit 2 ;;
esac

echo ">> running franz-go functional tests: $suite"
go test -tags functional -v -count=1 -timeout 300s $pkgs
