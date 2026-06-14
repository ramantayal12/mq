// Command mqbroker runs the single-broker, Kafka-wire-compatible message queue.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/controller"
	"mq/internal/group"
	"mq/internal/metrics"
	"mq/internal/server"
	"mq/internal/storage/object"
)

func main() {
	cfg := config.Load(os.Args[1:])
	slog.SetLogLoggerLevel(slog.LevelInfo)

	cl, err := cluster.Parse(cfg.NodeID, cfg.Brokers, cfg.AdvertisedHost, cfg.AdvertisedPort)
	if err != nil {
		slog.Error("failed to parse cluster membership", "err", err)
		os.Exit(1)
	}

	b, err := broker.New(cfg, cl)
	if err != nil {
		slog.Error("failed to init broker", "err", err)
		os.Exit(1)
	}

	store, err := group.NewOffsetStore(cfg.LogDirs)
	if err != nil {
		slog.Error("failed to init offset store", "err", err)
		os.Exit(1)
	}
	coord := group.NewCoordinator(store)

	srv, err := server.New(cfg.Listen, b, coord)
	if err != nil {
		slog.Error("failed to listen", "addr", cfg.Listen, "err", err)
		os.Exit(1)
	}

	// In cluster mode, stand up the Raft metadata controller and route the broker's
	// placement through its FSM (Phase 2 — see docs/GAPS_PLAN.md). Single-broker mode
	// keeps the original dependency-light, controller-free path. The object backend keeps
	// its index in the FSM, so it needs a controller even on a single node.
	objectBackend := cfg.StorageBackend == "object"
	var ctrl *controller.Controller
	if cl.Size() > 1 || objectBackend {
		ctrl, err = startController(cfg, cl)
		if err != nil {
			slog.Error("failed to start controller", "err", err)
			os.Exit(1)
		}
		defer func() {
			if err := ctrl.Close(); err != nil {
				slog.Warn("controller close failed", "err", err)
			}
		}()
		ctrl.WaitForLeader(10 * time.Second)
		// The object Storage indexes through the FSM, so build it once the controller has a
		// leader and install it before SetController reconciles this node's partitions.
		if objectBackend {
			objStorage, err := buildObjectStorage(cfg, ctrl)
			if err != nil {
				slog.Error("failed to init object storage", "err", err)
				os.Exit(1)
			}
			b.SetObjectStorage(objStorage)
		}
		b.SetController(ctrl)
	}

	slog.Info("mq broker started",
		"node_id", cfg.NodeID,
		"cluster_size", cl.Size(),
		"listen", cfg.Listen,
		"advertised", cfg.AdvertisedHost,
		"port", cfg.AdvertisedPort,
		"data", cfg.LogDirs)

	// Register a Prometheus collector that refreshes gauge metrics on each scrape.
	metrics.RegisterBrokerCollector(func() {
		refreshGauges(b, coord)
	})

	// Start the Prometheus /metrics HTTP server if configured.
	var httpSrv *http.Server
	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		httpSrv = &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
		go func() {
			slog.Info("metrics server started", "addr", cfg.MetricsAddr)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics server failed", "err", err)
			}
		}()
	}

	// Background flush + retention loops.
	ctx, cancel := context.WithCancel(context.Background())
	go flushLoop(ctx, b, time.Duration(cfg.FlushMs)*time.Millisecond)
	go retentionLoop(ctx, b)

	// Serve until a termination signal arrives.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		slog.Info("shutting down")
	case err := <-serveErr:
		if err != nil {
			slog.Error("serve error", "err", err)
		}
	}

	cancel()
	if httpSrv != nil {
		httpSrv.Close()
	}
	srv.Close()
	coord.Close()
	if err := b.FlushAll(); err != nil {
		slog.Warn("flush on shutdown failed", "err", err)
	}
	if err := b.Close(); err != nil {
		slog.Warn("close failed", "err", err)
	}
}

// refreshGauges updates Prometheus gauges from broker and coordinator state.
func refreshGauges(b *broker.Broker, coord *group.Coordinator) {
	topics := b.KnownTopics()
	metrics.TopicsTotal.Set(float64(len(topics)))
	var totalParts int
	// leo caches each partition's log-end offset so group-lag can be computed without
	// re-opening logs below.
	leo := map[string]int64{}
	for _, t := range topics {
		totalParts += int(t.Partitions)
		for p := int32(0); p < t.Partitions; p++ {
			pl := metrics.PartitionLabel(p)
			log, err := b.LocalLog(t.Name, p)
			if err != nil {
				continue
			}
			end := log.LatestOffset()
			leo[t.Name+"/"+pl] = end
			metrics.PartitionLEO.WithLabelValues(t.Name, pl).Set(float64(end))
			metrics.PartitionHWM.WithLabelValues(t.Name, pl).Set(float64(log.HighWatermark()))
			metrics.PartitionSize.WithLabelValues(t.Name, pl).Set(float64(log.Size()))
		}
	}
	metrics.PartitionsTotal.Set(float64(totalParts))

	for _, g := range coord.ListGroups() {
		desc, ok := coord.DescribeGroup(g.GroupID)
		if !ok {
			continue
		}
		metrics.GroupMembers.WithLabelValues(g.GroupID).Set(float64(len(desc.Members)))
	}

	// Per-group lag = log-end offset minus the group's committed offset, per partition.
	for _, c := range coord.Store().ListCommitted() {
		pl := metrics.PartitionLabel(c.Partition)
		end, ok := leo[c.Topic+"/"+pl]
		if !ok {
			continue // partition not led here (or unknown); skip
		}
		lag := end - c.Offset
		if lag < 0 {
			lag = 0
		}
		metrics.GroupLag.WithLabelValues(c.Group, c.Topic, pl).Set(float64(lag))
	}
}

// startController builds the Raft metadata controller for a multi-broker cluster. The
// raft transport binds on the Kafka listen host at advertised-port+1 and advertises
// AdvertisedHost:advertised-port+1; each peer's raft address is its Kafka host at
// port+1. Only the broker started with --raft-bootstrap forms the initial quorum.
func startController(cfg config.Config, cl *cluster.Cluster) (*controller.Controller, error) {
	listenHost, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("parse listen address %q: %w", cfg.Listen, err)
	}
	raftPort := cfg.AdvertisedPort + 1
	rpcPort := cfg.AdvertisedPort + 2
	peers := make([]controller.Peer, 0, cl.Size())
	for _, bi := range cl.Brokers() {
		peers = append(peers, controller.Peer{
			NodeID:   bi.NodeID,
			RaftAddr: fmt.Sprintf("%s:%d", bi.Host, bi.Port+1),
			RPCAddr:  fmt.Sprintf("%s:%d", bi.Host, bi.Port+2),
		})
	}
	// A single-node controller (object backend at cluster size 1) must bootstrap its own
	// one-server quorum, since no other broker will.
	bootstrap := cfg.RaftBootstrap || cl.Size() == 1
	return controller.New(controller.Config{
		NodeID:    cfg.NodeID,
		RaftBind:  fmt.Sprintf("%s:%d", listenHost, raftPort),
		Advertise: fmt.Sprintf("%s:%d", cfg.AdvertisedHost, raftPort),
		RPCBind:   fmt.Sprintf("%s:%d", listenHost, rpcPort),
		RaftDir:   filepath.Join(cfg.LogDirs, "raft"),
		Bootstrap: bootstrap,
		Peers:     peers,
	}, controller.NewFSM())
}

// buildObjectStorage constructs the node-level object Storage for the object backend: a minio-go
// ObjectStore over the configured endpoint/bucket, indexed through the controller's FSM, with a
// per-node WAL under <log-dirs>/wal.
func buildObjectStorage(cfg config.Config, ctrl *controller.Controller) (*object.Storage, error) {
	store, err := object.NewMinIO(object.Config{
		Endpoint:  cfg.ObjectEndpoint,
		Bucket:    cfg.ObjectBucket,
		AccessKey: cfg.ObjectAccessKey,
		SecretKey: cfg.ObjectSecretKey,
		Region:    cfg.ObjectRegion,
	})
	if err != nil {
		return nil, err
	}
	return object.NewStorage(store, ctrl.IndexStore(), object.StorageConfig{
		NodeID:      strconv.Itoa(int(cfg.NodeID)),
		WALDir:      filepath.Join(cfg.LogDirs, "wal"),
		UploadBytes: cfg.ObjectUploadBytes,
		UploadMS:    cfg.ObjectUploadMs,
	})
}

func retentionLoop(ctx context.Context, b *broker.Broker) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.EnforceRetention()
		}
	}
}

func flushLoop(ctx context.Context, b *broker.Broker, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.FlushAll(); err != nil {
				slog.Warn("periodic flush failed", "err", err)
			}
		}
	}
}
