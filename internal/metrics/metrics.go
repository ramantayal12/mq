// Package metrics defines and registers Prometheus metrics for the mq broker.
// Other packages import this and call Observe/Inc — metric definitions live here only.
package metrics

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// --- Counters ---

var ProduceRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "mq_produce_requests_total",
	Help: "Total produce requests.",
}, []string{"topic"})

var ProduceBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "mq_produce_bytes_total",
	Help: "Bytes written via produce.",
}, []string{"topic"})

var FetchRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "mq_fetch_requests_total",
	Help: "Total fetch requests.",
}, []string{"topic"})

var FetchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "mq_fetch_bytes_total",
	Help: "Bytes read via fetch.",
}, []string{"topic"})

var Requests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "mq_requests_total",
	Help: "Total requests by Kafka API key name.",
}, []string{"api"})

// --- Histograms ---

var ProduceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "mq_produce_latency_seconds",
	Help:    "End-to-end produce latency.",
	Buckets: prometheus.DefBuckets,
}, []string{"topic"})

var FetchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "mq_fetch_latency_seconds",
	Help:    "End-to-end fetch latency.",
	Buckets: prometheus.DefBuckets,
}, []string{"topic"})

var RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "mq_request_latency_seconds",
	Help:    "Per-API request latency.",
	Buckets: prometheus.DefBuckets,
}, []string{"api"})

// --- Gauges ---

var ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "mq_active_connections",
	Help: "Current open TCP connections.",
})

var TopicsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "mq_topics_total",
	Help: "Number of known topics.",
})

var PartitionsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "mq_partitions_total",
	Help: "Total partitions across all topics.",
})

var PartitionLEO = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mq_partition_log_end_offset",
	Help: "Log end offset per partition.",
}, []string{"topic", "partition"})

var PartitionHWM = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mq_partition_high_watermark",
	Help: "High watermark per partition.",
}, []string{"topic", "partition"})

var PartitionSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mq_partition_log_size_bytes",
	Help: "On-disk log size per partition.",
}, []string{"topic", "partition"})

var GroupMembers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mq_consumer_group_members",
	Help: "Members in each consumer group.",
}, []string{"group"})

var GroupLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "mq_consumer_group_lag",
	Help: "Consumer group lag (LEO minus committed offset).",
}, []string{"group", "topic", "partition"})

func init() {
	prometheus.MustRegister(
		ProduceRequests, ProduceBytes, ProduceLatency,
		FetchRequests, FetchBytes, FetchLatency,
		Requests, RequestLatency,
		ActiveConnections, TopicsTotal, PartitionsTotal,
		PartitionLEO, PartitionHWM, PartitionSize,
		GroupMembers, GroupLag,
	)
}

// apiNames maps Kafka API key numbers to human-readable names for the "api" label.
var apiNames = map[int16]string{
	0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
	8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator",
	11: "JoinGroup", 12: "Heartbeat", 13: "LeaveGroup",
	14: "SyncGroup", 15: "DescribeGroups", 16: "ListGroups",
	18: "ApiVersions", 19: "CreateTopics", 37: "CreatePartitions",
}

// APIName returns a human-readable name for a Kafka API key.
func APIName(key int16) string {
	if n, ok := apiNames[key]; ok {
		return n
	}
	return strconv.Itoa(int(key))
}

// PartitionLabel returns the string label for a partition index.
func PartitionLabel(p int32) string {
	return strconv.Itoa(int(p))
}

// BrokerCollector refreshes gauge-style metrics from broker/coordinator state on
// each Prometheus scrape. Call RegisterBrokerCollector once at startup.
type BrokerCollector struct {
	mu      sync.Mutex
	refresh func() // called on each Collect to update gauges
}

// RegisterBrokerCollector registers a callback that is invoked on each Prometheus
// scrape to refresh gauge metrics (offsets, lag, group membership).
func RegisterBrokerCollector(refreshFn func()) {
	c := &BrokerCollector{refresh: refreshFn}
	prometheus.MustRegister(c)
}

func (c *BrokerCollector) Describe(ch chan<- *prometheus.Desc) {
	// Gauges are already registered; this collector only triggers the refresh.
}

func (c *BrokerCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refresh != nil {
		c.refresh()
	}
}
