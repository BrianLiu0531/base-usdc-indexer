// Command indexer follows USDC transfers for a set of watched addresses on Base.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sync/errgroup"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/api"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/config"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/indexer"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/metrics"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/outbox"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/reconcile"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("indexer stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("configuration:\n%w", err)
	}
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var log *slog.Logger
	if cfg.LogJSON {
		log = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	} else {
		log = slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rpc, err := chain.DialRPC(ctx, chain.RPCConfig{URLs: cfg.RPCURLs, MaxRPS: cfg.RPCMaxRPS, MaxAttempts: cfg.RPCMaxAttempts, Timeout: cfg.RPCTimeout}, log)
	if err != nil {
		return err
	}
	defer rpc.Close()
	id, err := rpc.ChainID(ctx)
	if err != nil {
		return err
	}
	if id != cfg.Network.ChainID {
		return fmt.Errorf("RPC is on chain %d but NETWORK=%s expects %d", id, cfg.Network.Name, cfg.Network.ChainID)
	}

	st, err := store.Open(ctx, cfg.DatabaseURL, id)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	metrics.Register(reg)
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ix := indexer.New(indexer.Config{
		Token: cfg.Network.USDC, Watch: cfg.Watch, StartBlock: cfg.StartBlock, MaxRange: cfg.MaxBlockRange,
		MaxUnfinalizedBatch: cfg.MaxUnfinalizedBatch, ConfirmAt: cfg.ConfirmAt, PollInterval: cfg.PollInterval,
	}, rpc, st, log)
	relay := &outbox.Relay{Store: st, Publisher: outbox.LogPublisher{Log: log}, BatchSize: 100, Interval: time.Second, Log: log}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           (&api.Server{Store: st, Network: cfg.Network, Watch: cfg.Watch, ConfirmAt: cfg.ConfirmAt, Metrics: reg, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("starting", "network", cfg.Network.Name, "chain_id", id, "watching", len(cfg.Watch), "confirm_at", cfg.ConfirmAt.String(), "http", cfg.HTTPAddr)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return ix.Run(ctx) })
	g.Go(func() error { return relay.Run(ctx) })
	if cfg.ReconcileInterval > 0 {
		rec := &reconcile.Reconciler{Chain: rpc, Store: st, Token: cfg.Network.USDC, Watch: cfg.Watch, Interval: cfg.ReconcileInterval, Log: log}
		g.Go(func() error { return rec.Run(ctx) })
	}
	g.Go(func() error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	})

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		log.Info("shut down cleanly")
		return nil
	}
	return err
}
