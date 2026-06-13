// Command mqbroker runs the single-broker, Kafka-wire-compatible message queue.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
	"mq/internal/controller"
	"mq/internal/group"
	"mq/internal/server"
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
	// keeps the original dependency-light, controller-free path.
	var ctrl *controller.Controller
	if cl.Size() > 1 {
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
		b.SetController(ctrl)
	}

	slog.Info("mq broker started",
		"node_id", cfg.NodeID,
		"cluster_size", cl.Size(),
		"listen", cfg.Listen,
		"advertised", cfg.AdvertisedHost,
		"port", cfg.AdvertisedPort,
		"data", cfg.LogDirs)

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
	srv.Close()
	coord.Close()
	if err := b.FlushAll(); err != nil {
		slog.Warn("flush on shutdown failed", "err", err)
	}
	if err := b.Close(); err != nil {
		slog.Warn("close failed", "err", err)
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
	return controller.New(controller.Config{
		NodeID:    cfg.NodeID,
		RaftBind:  fmt.Sprintf("%s:%d", listenHost, raftPort),
		Advertise: fmt.Sprintf("%s:%d", cfg.AdvertisedHost, raftPort),
		RPCBind:   fmt.Sprintf("%s:%d", listenHost, rpcPort),
		RaftDir:   filepath.Join(cfg.LogDirs, "raft"),
		Bootstrap: cfg.RaftBootstrap,
		Peers:     peers,
	}, controller.NewFSM())
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
