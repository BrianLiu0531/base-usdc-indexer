// Package reconcile checks indexed balances against balanceOf on chain.
//
// The first check for an address fixes its opening balance (on-chain balance minus indexed net flow).
// Every later check must then satisfy: on-chain balance = opening balance + indexed net flow.
// A missed or duplicated transfer anywhere in history breaks that equation.
package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/metrics"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

type Reconciler struct {
	Chain    chain.Client
	Store    *store.Store
	Token    common.Address
	Watch    []common.Address
	Interval time.Duration
	Log      *slog.Logger
}

// Run checks every Interval until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		if _, err := r.Check(ctx); err != nil && ctx.Err() == nil {
			r.Log.Warn("reconciliation failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Check reconciles every watched address at the indexer's finalized height.
// Finalized data cannot change, so a mismatch is a real error rather than a race with new blocks.
func (r *Reconciler) Check(ctx context.Context) ([]store.Reconciliation, error) {
	st, err := r.Store.State(ctx)
	if err != nil {
		return nil, err
	}
	if st.Finalized == 0 {
		return nil, nil
	}
	var out []store.Reconciliation
	for _, a := range r.Watch {
		rec, err := r.checkAddress(ctx, a, st.Finalized)
		if err != nil {
			return out, fmt.Errorf("%s: %w", a.Hex(), err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (r *Reconciler) checkAddress(ctx context.Context, a common.Address, block uint64) (store.Reconciliation, error) {
	onchain, err := r.Chain.BalanceOf(ctx, r.Token, a, block)
	if err != nil {
		return store.Reconciliation{}, err
	}
	flow, err := r.Store.FlowOf(ctx, a, block, store.Pending)
	if err != nil {
		return store.Reconciliation{}, err
	}
	base, err := r.Store.Baseline(ctx, a)
	if err != nil {
		return store.Reconciliation{}, err
	}
	if base == nil {
		if base, err = r.Store.SetBaseline(ctx, a, store.Baseline{Block: block, Offset: new(big.Int).Sub(onchain, flow.Net())}); err != nil {
			return store.Reconciliation{}, err
		}
	}
	expected := new(big.Int).Add(base.Offset, flow.Net())
	rec := store.Reconciliation{
		Address:  strings.ToLower(a.Hex()),
		Block:    block,
		Onchain:  onchain,
		Expected: expected,
		OK:       expected.Cmp(onchain) == 0,
	}
	if err := r.Store.RecordReconciliation(ctx, rec); err != nil {
		return rec, err
	}
	if rec.OK {
		metrics.ReconcileMismatch.WithLabelValues(rec.Address).Set(0)
		r.Log.Debug("balance reconciled", "address", rec.Address, "block", block, "balance", usdc.FormatAmount(onchain))
	} else {
		metrics.ReconcileMismatch.WithLabelValues(rec.Address).Set(1)
		r.Log.Error("balance mismatch", "address", rec.Address, "block", block,
			"onchain", usdc.FormatAmount(onchain), "indexed", usdc.FormatAmount(expected),
			"difference", usdc.FormatAmount(new(big.Int).Sub(onchain, expected)))
	}
	return rec, nil
}
