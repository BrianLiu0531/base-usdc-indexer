// Package outbox relays events from the outbox table to a message bus.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/metrics"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
)

// Publisher delivers events. It must return an error unless every event was accepted.
type Publisher interface {
	Publish(ctx context.Context, events []store.Event) error
}

// LogPublisher writes events to the log. It is the default until a real bus is configured.
type LogPublisher struct{ Log *slog.Logger }

func (p LogPublisher) Publish(_ context.Context, events []store.Event) error {
	for _, e := range events {
		p.Log.Info("event", "type", e.Type, "id", e.EventID, "payload", string(e.Payload))
	}
	return nil
}

// Relay moves events from the outbox to a Publisher, at least once and in order.
type Relay struct {
	Store     *store.Store
	Publisher Publisher
	BatchSize int
	Interval  time.Duration
	Log       *slog.Logger
}

// Run relays until ctx is cancelled. A full batch is followed immediately by the next one.
func (r *Relay) Run(ctx context.Context) error {
	failures := 0
	for {
		n, err := r.Store.PublishOutbox(ctx, r.BatchSize, r.Publisher.Publish)
		wait := r.Interval
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			failures++
			wait = min(time.Minute, r.Interval<<min(failures, 6))
			r.Log.Warn("publishing outbox events failed", "err", err, "retry_in", wait)
		default:
			failures = 0
			metrics.OutboxPublished.Add(float64(n))
			if n == r.BatchSize {
				wait = 0
			}
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
}
