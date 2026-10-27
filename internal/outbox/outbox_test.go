package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain/chaintest"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/indexer"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store/storetest"
)

type recorder struct {
	fail bool
	got  []store.Event
}

func (r *recorder) Publish(_ context.Context, events []store.Event) error {
	if r.fail {
		return errors.New("bus unavailable")
	}
	r.got = append(r.got, events...)
	return nil
}

func TestPublishesEachEventOnceInOrder(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, 84532)
	f := chaintest.New(84532, 1)
	alice := common.HexToAddress("0xa11ce")
	for i := range 5 {
		f.Mine(chaintest.Spec{From: common.HexToAddress("0xca201"), To: alice, Value: int64(i + 1)})
	}
	f.SetFinality(5, 5)
	ix := indexer.New(indexer.Config{Token: common.Address{1}, Watch: []common.Address{alice}, StartBlock: 1, ConfirmAt: store.Finalized},
		f, s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for range 10 {
		if res, err := ix.Step(ctx); err != nil || res.CaughtUp {
			break
		}
	}

	// A failed publish leaves every event pending
	bus := &recorder{fail: true}
	if _, err := s.PublishOutbox(ctx, 2, bus.Publish); err == nil {
		t.Fatal("expected an error")
	}

	bus.fail = false
	total := 0
	for {
		n, err := s.PublishOutbox(ctx, 2, bus.Publish)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total != 5 || len(bus.got) != 5 {
		t.Fatalf("published %d (%d received), want 5", total, len(bus.got))
	}
	for i := 1; i < len(bus.got); i++ {
		if bus.got[i].ID <= bus.got[i-1].ID {
			t.Fatal("events out of order")
		}
	}
}
