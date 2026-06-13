// Package config loads broker configuration from flags, then MQ_* environment
// variables, then built-in defaults (in that precedence order).
package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
)

// Config is the full broker configuration.
type Config struct {
	NodeID             int32  // this broker's node id within the cluster
	Brokers            string // static membership "id@host:port,..."; empty = single broker
	Listen             string // bind address, e.g. "0.0.0.0:9092"
	AdvertisedHost     string // host returned in Metadata so clients can reconnect
	AdvertisedPort     int32  // port returned in Metadata
	LogDirs            string // root data directory
	NumPartitions      int32  // default partitions for auto-created topics
	SegmentBytes       int32  // segment roll threshold
	IndexIntervalBytes int32  // sparse index interval
	FlushMs            int    // background flush interval (ms)
	RetentionMs        int64  // segment age retention (0 = disabled)
	RetentionBytes     int64  // total-size retention (0 = disabled)
	AutoCreateTopics   bool   // create unknown topics on Metadata/Produce
	RaftBootstrap      bool   // this broker bootstraps the Raft metadata controller quorum
	ReplicationFactor  int32  // replicas per partition in cluster mode (clamped to quorum size)
}

// Default returns the built-in defaults.
func Default() Config {
	return Config{
		NodeID:             0,
		Brokers:            "",
		Listen:             "0.0.0.0:9092",
		AdvertisedHost:     "localhost",
		AdvertisedPort:     9092,
		LogDirs:            "./data",
		NumPartitions:      1,
		SegmentBytes:       64 << 20,
		IndexIntervalBytes: 4096,
		FlushMs:            1000,
		RetentionMs:        7 * 24 * 60 * 60 * 1000,
		RetentionBytes:     0,
		AutoCreateTopics:   true,
		ReplicationFactor:  1,
	}
}

// Load builds a Config from defaults <- env (MQ_*) <- flags.
func Load(args []string) Config {
	c := Default()

	// Environment overrides defaults.
	if v := os.Getenv("MQ_LISTENERS"); v != "" {
		c.Listen = v
	}
	if v := os.Getenv("MQ_ADVERTISED_LISTENERS"); v != "" {
		host, port := splitHostPort(v)
		if host != "" {
			c.AdvertisedHost = host
		}
		if port != 0 {
			c.AdvertisedPort = port
		}
	}
	if v := os.Getenv("MQ_LOG_DIRS"); v != "" {
		c.LogDirs = v
	}
	if v := os.Getenv("MQ_BROKERS"); v != "" {
		c.Brokers = v
	}
	c.NodeID = envInt32("MQ_NODE_ID", c.NodeID)
	c.NumPartitions = envInt32("MQ_NUM_PARTITIONS", c.NumPartitions)
	c.ReplicationFactor = envInt32("MQ_REPLICATION_FACTOR", c.ReplicationFactor)
	c.SegmentBytes = envInt32("MQ_SEGMENT_BYTES", c.SegmentBytes)
	c.FlushMs = int(envInt32("MQ_FLUSH_MS", int32(c.FlushMs)))
	c.RetentionMs = envInt64("MQ_RETENTION_MS", c.RetentionMs)
	c.RetentionBytes = envInt64("MQ_RETENTION_BYTES", c.RetentionBytes)
	if v := os.Getenv("MQ_AUTO_CREATE_TOPICS"); v != "" {
		c.AutoCreateTopics = v == "true" || v == "1"
	}
	if v := os.Getenv("MQ_RAFT_BOOTSTRAP"); v != "" {
		c.RaftBootstrap = v == "true" || v == "1"
	}

	// Flags override everything.
	fs := flag.NewFlagSet("mqbroker", flag.ContinueOnError)
	nodeID := fs.Int("node-id", int(c.NodeID), "this broker's node id")
	brokers := fs.String("brokers", c.Brokers, "static cluster membership id@host:port,... (empty=single broker)")
	listen := fs.String("listen", c.Listen, "bind address host:port")
	advHost := fs.String("advertised-host", c.AdvertisedHost, "advertised host")
	advPort := fs.Int("advertised-port", int(c.AdvertisedPort), "advertised port")
	logDirs := fs.String("log-dirs", c.LogDirs, "data directory")
	numParts := fs.Int("partitions", int(c.NumPartitions), "default partitions per topic")
	replFactor := fs.Int("replication-factor", int(c.ReplicationFactor), "replicas per partition in cluster mode (clamped to quorum size)")
	segBytes := fs.Int("segment-bytes", int(c.SegmentBytes), "segment roll size")
	flushMs := fs.Int("flush-ms", c.FlushMs, "flush interval ms")
	autoCreate := fs.Bool("auto-create-topics", c.AutoCreateTopics, "auto-create unknown topics")
	raftBootstrap := fs.Bool("raft-bootstrap", c.RaftBootstrap, "bootstrap the Raft metadata controller quorum (one broker only)")
	_ = fs.Parse(args)

	c.NodeID = int32(*nodeID)
	c.Brokers = *brokers
	c.Listen = *listen
	c.AdvertisedHost = *advHost
	c.AdvertisedPort = int32(*advPort)
	c.LogDirs = *logDirs
	c.NumPartitions = int32(*numParts)
	c.ReplicationFactor = int32(*replFactor)
	c.SegmentBytes = int32(*segBytes)
	c.FlushMs = *flushMs
	c.AutoCreateTopics = *autoCreate
	c.RaftBootstrap = *raftBootstrap
	return c
}

func splitHostPort(s string) (string, int32) {
	// Accept "host:port" or Kafka-style "PLAINTEXT://host:port".
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return s, 0
	}
	port, _ := strconv.Atoi(s[i+1:])
	return s[:i], int32(port)
}

func envInt32(key string, def int32) int32 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return int32(n)
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
