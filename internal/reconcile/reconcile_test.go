package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain/chaintest"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/indexer"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store/storetest"
)

var (
	token = common.HexToAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	alice = common.HexToAddress("0x00000000000000000000000000000000000a11ce")
	carol = common.HexToAddress("0x00000000000000000000000000000000000ca201")
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func sync(t *testing.T, ix *indexer.Indexer) {
	t.Helper()
	for range 100 {
		res, err := ix.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if res.CaughtUp {
			return
		}
	}
	t.Fatal("did not catch up")
}

func TestReconcilesAgainstBalanceOf(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, 84532)
	f := chaintest.New(84532, 1)
	f.Opening[alice] = big.NewInt(1_000_000) // balance before indexing started
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 500})
	f.Mine(chaintest.Spec{From: alice, To: carol, Value: 200})
	f.MineEmpty(2)
	f.SetFinality(4, 4)

	watch := []common.Address{alice}
	ix := indexer.New(indexer.Config{Token: token, Watch: watch, StartBlock: 1, ConfirmAt: store.Finalized}, f, s, quiet)
	sync(t, ix)
	r := &Reconciler{Chain: f, Store: s, Token: token, Watch: watch, Log: quiet}

	// First check fixes the opening balance
	recs, err := r.Check(ctx)
	if err != nil || len(recs) != 1 || !recs[0].OK {
		t.Fatalf("first check: %+v, %v", recs, err)
	}
	if base, _ := s.Baseline(ctx, alice); base.Offset.Int64() != 1_000_000 {
		t.Fatalf("opening balance %v", base.Offset)
	}

	// Later checks must agree as new transfers are indexed
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 42})
	f.MineEmpty(1)
	f.SetFinality(6, 6)
	sync(t, ix)
	if recs, err := r.Check(ctx); err != nil || !recs[0].OK || recs[0].Onchain.Int64() != 1_000_342 {
		t.Fatalf("second check: %+v, %v", recs, err)
	}

	// A balance change the indexer never saw (a missed log) is reported
	f.Opening[alice] = big.NewInt(1_000_001)
	recs, err = r.Check(ctx)
	if err != nil || recs[0].OK {
		t.Fatalf("expected a mismatch, got %+v, %v", recs, err)
	}
	if last, _ := s.LatestReconciliation(ctx, alice); last == nil || last.OK || last.Expected.Int64() != 1_000_342 {
		t.Fatalf("stored reconciliation %+v", last)
	}
}

func TestSkipsUntilSomethingIsFinalized(t *testing.T) {
	s := storetest.Open(t, 84532)
	f := chaintest.New(84532, 1)
	r := &Reconciler{Chain: f, Store: s, Token: token, Watch: []common.Address{alice}, Log: quiet}
	if recs, err := r.Check(context.Background()); err != nil || len(recs) != 0 {
		t.Fatalf("got %+v, %v", recs, err)
	}
	f.SetFailRate(1)
	if _, err := f.BalanceOf(context.Background(), token, alice, 0); !errors.Is(err, chaintest.ErrInjected) {
		t.Fatal("fake should fail")
	}
}
