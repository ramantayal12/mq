// Command mqbroker runs the single-broker, Kafka-wire-compatible message queue.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mq/internal/broker"
	"mq/internal/cluster"
	"mq/internal/config"
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
