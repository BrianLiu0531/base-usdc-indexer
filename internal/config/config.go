// Package config reads settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

type Config struct {
	Network             usdc.Network
	RPCURLs             []string
	DatabaseURL         string
	Watch               []common.Address
	StartBlock          uint64
	ConfirmAt           store.Level
	MaxBlockRange       uint64
	MaxUnfinalizedBatch uint64
	PollInterval        time.Duration
	ReconcileInterval   time.Duration
	HTTPAddr            string
	RPCMaxRPS           float64
	RPCMaxAttempts      int
	RPCTimeout          time.Duration
	LogLevel            slog.Level
	LogJSON             bool
	OutboxSQSQueueURL   string // publish outbox events to this SQS FIFO queue instead of the log
}

// FromEnv loads the configuration; see .env.example for every variable.
func FromEnv() (Config, error) {
	e := &env{}
	c := Config{
		RPCURLs:             e.list("RPC_URLS", true),
		DatabaseURL:         e.str("DATABASE_URL", "", true),
		StartBlock:          e.uint("START_BLOCK", 0),
		MaxBlockRange:       e.uint("MAX_BLOCK_RANGE", 2000),
		MaxUnfinalizedBatch: e.uint("MAX_UNFINALIZED_BATCH", 100),
		PollInterval:        e.duration("POLL_INTERVAL", 2*time.Second),
		ReconcileInterval:   e.duration("RECONCILE_INTERVAL", 5*time.Minute),
		HTTPAddr:            e.str("HTTP_ADDR", ":8080", false),
		RPCMaxRPS:           e.float("RPC_MAX_RPS", 10),
		RPCMaxAttempts:      int(e.uint("RPC_MAX_ATTEMPTS", 5)),
		RPCTimeout:          e.duration("RPC_TIMEOUT", 15*time.Second),
		LogJSON:             e.str("LOG_FORMAT", "json", false) == "json",
		OutboxSQSQueueURL:   e.str("OUTBOX_SQS_QUEUE_URL", "", false),
	}

	network, err := usdc.Lookup(e.str("NETWORK", "base-sepolia", false))
	e.add(err)
	c.Network = network

	level, err := store.ParseLevel(e.str("CONFIRM_AT", "finalized", false))
	e.add(err)
	c.ConfirmAt = level

	e.add(c.LogLevel.UnmarshalText([]byte(e.str("LOG_LEVEL", "info", false))))

	for _, raw := range e.list("WATCH_ADDRESSES", true) {
		if !common.IsHexAddress(raw) {
			e.add(fmt.Errorf("WATCH_ADDRESSES: %q is not an address", raw))
			continue
		}
		c.Watch = append(c.Watch, common.HexToAddress(raw))
	}
	if c.MaxBlockRange == 0 || c.MaxUnfinalizedBatch == 0 {
		e.add(errors.New("MAX_BLOCK_RANGE and MAX_UNFINALIZED_BATCH must be positive"))
	}
	return c, errors.Join(e.errs...)
}

type env struct{ errs []error }

func (e *env) add(err error) {
	if err != nil {
		e.errs = append(e.errs, err)
	}
}

func (e *env) str(key, def string, required bool) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		if required {
			e.add(fmt.Errorf("%s is required", key))
		}
		return def
	}
	return v
}

func (e *env) list(key string, required bool) []string {
	var out []string
	for _, part := range strings.Split(e.str(key, "", required), ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *env) uint(key string, def uint64) uint64 {
	v := e.str(key, "", false)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		e.add(fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func (e *env) float(key string, def float64) float64 {
	v := e.str(key, "", false)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		e.add(fmt.Errorf("%s must be a positive number", key))
	}
	return f
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	v := e.str(key, "", false)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.add(fmt.Errorf("%s: %w", key, err))
	}
	return d
}
