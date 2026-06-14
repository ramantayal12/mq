# syntax=docker/dockerfile:1

# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src
# Cache module downloads first (only re-runs when go.mod/go.sum change).
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary so it runs on a distroless/scratch base.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /mqbroker ./cmd/mqbroker

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot

# Conventional Kafka broker port.
EXPOSE 9092 7080

# Persist log segments across container restarts (like Kafka's /var/lib/kafka/data).
VOLUME /var/lib/mq
ENV MQ_LOG_DIRS=/var/lib/mq \
    MQ_LISTENERS=0.0.0.0:9092 \
    MQ_ADVERTISED_LISTENERS=localhost:9092 \
    MQ_METRICS_ADDR=0.0.0.0:7080

COPY --from=build /mqbroker /mqbroker
ENTRYPOINT ["/mqbroker"]
