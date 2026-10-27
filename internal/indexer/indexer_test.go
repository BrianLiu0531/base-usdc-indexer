package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain/chaintest"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store/storetest"
)

const chainID = 84532

var (
	token = common.HexToAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	alice = common.HexToAddress("0x00000000000000000000000000000000000a11ce") // watched
	bob   = common.HexToAddress("0x0000000000000000000000000000000000000b0b") // watched
	carol = common.HexToAddress("0x00000000000000000000000000000000000ca201")
	dave  = common.HexToAddress("0x0000000000000000000000000000000000000da7")
	watch = []common.Address{alice, bob}
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newIndexer(f chain.Client, s *store.Store, mutate ...func(*Config)) *Indexer {
	cfg := Config{Token: token, Watch: watch, StartBlock: 1, MaxRange: 50, MaxUnfinalizedBatch: 8, ConfirmAt: store.Finalized}
	for _, m := range mutate {
		m(&cfg)
	}
	return New(cfg, f, s, quiet)
}

// sync steps until the indexer is caught up, tolerating the transient errors a real node produces.
func sync(t *testing.T, ix *Indexer) {
	t.Helper()
	for range 2000 {
		res, err := ix.Step(context.Background())
		switch {
		case err == nil && res.CaughtUp:
			return
		case err == nil, errors.Is(err, chaintest.ErrInjected), errors.Is(err, errChainMoved):
		default:
			t.Fatalf("step: %v", err)
		}
	}
	t.Fatal("indexer did not catch up")
}

// assertMatchesChain checks the database holds exactly the canonical watched transfers, with the
// right finality, and that the outbox announced each confirmed transfer once.
func assertMatchesChain(t *testing.T, s *store.Store, f *chaintest.Fake, start uint64, confirmAt store.Level) {
	t.Helper()
	ctx := context.Background()
	tips, _ := f.Tips(ctx)

	var want []chain.Transfer
	for _, tr := range f.Canonical(watch) {
		if tr.BlockNumber >= start {
			want = append(want, tr)
		}
	}
	got, err := s.AllTransfers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("stored %d transfers, chain has %d", len(got), len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.TxHash != w.TxHash.Hex() || g.LogIndex != int(w.LogIndex) || g.BlockHash != w.BlockHash.Hex() ||
			g.Value.Cmp(w.Value) != 0 || g.From != strings.ToLower(w.From.Hex()) || g.To != strings.ToLower(w.To.Hex()) {
			t.Fatalf("transfer %d: got %+v, want %+v", i, g, w)
		}
		wantStatus := store.Pending
		switch {
		case w.BlockNumber <= tips.Finalized:
			wantStatus = store.Finalized
		case w.BlockNumber <= tips.Safe:
			wantStatus = store.Safe
		}
		if g.Status != wantStatus {
			t.Fatalf("transfer in block %d: status %v, want %v", w.BlockNumber, g.Status, wantStatus)
		}
		if g.Confirmed != (wantStatus >= confirmAt) {
			t.Fatalf("transfer in block %d: confirmed=%v at status %v", w.BlockNumber, g.Confirmed, g.Status)
		}
	}

	// Replaying the outbox must leave exactly the confirmed canonical transfers "credited"
	events, err := s.OutboxEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	credited := map[string]int{}
	ids := map[string]bool{}
	for _, e := range events {
		if ids[e.EventID] {
			t.Fatalf("duplicate event id %s", e.EventID)
		}
		ids[e.EventID] = true
		var p struct {
			TxHash    string `json:"txHash"`
			LogIndex  int    `json:"logIndex"`
			BlockHash string `json:"blockHash"`
		}
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s:%d:%s", p.TxHash, p.LogIndex, p.BlockHash)
		switch e.Type {
		case "transfer.confirmed":
			credited[key]++
		case "transfer.reverted":
			credited[key]--
		}
	}
	for _, g := range got {
		key := fmt.Sprintf("%s:%d:%s", g.TxHash, g.LogIndex, g.BlockHash)
		want := 0
		if g.Confirmed && g.Value.Sign() > 0 {
			want = 1
		}
		if credited[key] != want {
			t.Fatalf("transfer %s credited %d times, want %d", key, credited[key], want)
		}
		delete(credited, key)
	}
	for key, n := range credited {
		if n != 0 {
			t.Fatalf("orphaned transfer %s is still credited %d times", key, n)
		}
	}
}

func TestIndexesEachWatchedTransferOnce(t *testing.T) {
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 100})
	f.Mine(chaintest.Spec{From: bob, To: carol, Value: 30}, chaintest.Spec{From: carol, To: dave, Value: 999}) // second is unwatched
	f.Mine(chaintest.Spec{From: alice, To: bob, Value: 5})                                                     // matches both filters
	f.MineEmpty(5)
	f.SetFinality(6, 4)

	sync(t, newIndexer(f, s))
	assertMatchesChain(t, s, f, 1, store.Finalized)
	if got, _ := s.AllTransfers(context.Background()); len(got) != 3 {
		t.Fatalf("got %d transfers, want 3", len(got))
	}

	// A restarted indexer resumes from the cursor and writes nothing twice
	sync(t, newIndexer(f, s))
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

func TestRejectsStaleBatches(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 100})
	f.MineEmpty(3)
	sync(t, newIndexer(f, s))

	// A second writer that read an older cursor must not be able to commit
	stale := &chain.Header{Number: 1, Hash: common.HexToHash("0x01")}
	_, err := s.Apply(ctx, store.Batch{Parent: stale, Blocks: []chain.Header{{Number: 2}}})
	if !errors.Is(err, store.ErrCursorMoved) {
		t.Fatalf("got %v, want ErrCursorMoved", err)
	}
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

func TestConfirmsOnceAtTheConfiguredLevel(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	ix := newIndexer(f, s)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 100})
	f.Mine(chaintest.Spec{From: carol, To: bob, Value: 200})
	f.MineEmpty(4)

	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized) // pending, no events yet

	f.SetFinality(2, 0)
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized) // safe, still no events

	f.SetFinality(6, 1)
	sync(t, ix)
	f.SetFinality(6, 6)
	sync(t, ix)
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized)
	if events, _ := s.OutboxEvents(ctx); len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
}

func TestReorgReplacesOrphanedTransfers(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	confirmPending := func(c *Config) { c.ConfirmAt = store.Pending }
	ix := newIndexer(f, s, confirmPending)

	for i := range 6 {
		f.Mine(chaintest.Spec{From: carol, To: alice, Value: int64(10 + i)})
	}
	f.SetFinality(3, 2)
	sync(t, ix)

	// Blocks 5 and 6 are replaced; the transfer from block 6 is re-mined in the new fork
	removed := f.Reorg(2)
	f.MineEmpty(1)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: removed[1].Value.Int64(), TxHash: removed[1].TxHash})
	f.Mine(chaintest.Spec{From: alice, To: carol, Value: 3})

	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Pending)

	events, _ := s.OutboxEvents(ctx)
	types := map[string]int{}
	for _, e := range events {
		types[e.Type]++
	}
	// 6 original + 2 new confirmations; 2 reverted
	if types["transfer.confirmed"] != 8 || types["transfer.reverted"] != 2 {
		t.Fatalf("events by type: %v", types)
	}
}

func TestDetectsReorgWithoutNewBlocks(t *testing.T) {
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	ix := newIndexer(f, s)
	f.MineEmpty(3)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 1})
	sync(t, ix)

	// Same height after the reorg, then a shorter chain
	f.Reorg(1)
	f.Mine(chaintest.Spec{From: carol, To: bob, Value: 2})
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized)

	// A shorter chain looks exactly like a node that is behind, so nothing is rewound yet...
	cur, _ := s.Cursor(context.Background())
	f.Reorg(2)
	f.MineEmpty(1)
	sync(t, ix)
	if after, _ := s.Cursor(context.Background()); after.Hash != cur.Hash {
		t.Fatal("rewound without evidence of a reorg")
	}
	// ...until the chain grows past the cursor and the parent hash proves the fork
	f.MineEmpty(2)
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

// lagging is a node that has only reached block head, as served by a load balancer or a failover endpoint.
type lagging struct {
	*chaintest.Fake
	head uint64
}

func (l *lagging) Tips(ctx context.Context) (chain.Tips, error) {
	tips, err := l.Fake.Tips(ctx)
	return chain.Tips{Latest: min(tips.Latest, l.head), Safe: min(tips.Safe, l.head), Finalized: min(tips.Finalized, l.head)}, err
}

func (l *lagging) Header(ctx context.Context, n uint64) (chain.Header, error) {
	if n > l.head {
		return chain.Header{}, chain.ErrNotFound
	}
	return l.Fake.Header(ctx, n)
}

func TestWaitsForALaggingNode(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	for range 10 {
		f.Mine(chaintest.Spec{From: carol, To: alice, Value: 1})
	}
	sync(t, newIndexer(f, s, func(c *Config) { c.ConfirmAt = store.Pending }))

	// Fail over to a node 4 blocks behind: the indexer must wait, not treat the missing blocks as a reorg
	slow := newIndexer(&lagging{Fake: f, head: 6}, s, func(c *Config) { c.ConfirmAt = store.Pending })
	for range 5 {
		res, err := slow.Step(ctx)
		if err != nil || !res.CaughtUp || res.Reorg {
			t.Fatalf("step on lagging node: %+v, %v", res, err)
		}
	}
	events, _ := s.OutboxEvents(ctx)
	for _, e := range events {
		if e.Type == "transfer.reverted" {
			t.Fatal("a lagging node caused a revert")
		}
	}
	assertMatchesChain(t, s, f, 1, store.Pending)
}

func TestZeroValueTransfersAreStoredButNotAnnounced(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	// Address poisoning: a 0-value transferFrom makes a look-alike address appear in the victim's history
	f.Mine(chaintest.Spec{From: alice, To: dave, Value: 0}, chaintest.Spec{From: carol, To: alice, Value: 10})
	f.SetFinality(1, 1)
	sync(t, newIndexer(f, s))
	assertMatchesChain(t, s, f, 1, store.Finalized)
	if events, _ := s.OutboxEvents(ctx); len(events) != 1 {
		t.Fatalf("got %d events, want 1 (the zero-value transfer must not be announced)", len(events))
	}
}

// TestConcurrentIndexersNeverDuplicate runs two indexers against one database at the same time,
// the indexer's equivalent of two requests settling the same bet at once.
func TestConcurrentIndexersNeverDuplicate(t *testing.T) {
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	for i := range 120 {
		f.Mine(chaintest.Spec{From: carol, To: alice, Value: int64(i + 1)}, chaintest.Spec{From: bob, To: dave, Value: 1})
	}
	f.SetFinality(115, 100)

	errs := make(chan error, 2)
	for range 2 {
		ix := newIndexer(f, s, func(c *Config) { c.MaxRange = 7 })
		go func() {
			for range 5000 {
				res, err := ix.Step(context.Background())
				if err != nil && !errors.Is(err, store.ErrCursorMoved) {
					errs <- err
					return
				}
				if err == nil && res.CaughtUp {
					errs <- nil
					return
				}
			}
			errs <- errors.New("did not catch up")
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

func TestSurvivesReorgDuringFetch(t *testing.T) {
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	ix := newIndexer(f, s)
	for range 5 {
		f.Mine(chaintest.Spec{From: carol, To: alice, Value: 7})
	}
	fired := false
	f.BeforeTransfers = func() {
		if !fired {
			fired = true
			f.Reorg(3)
			f.Mine(chaintest.Spec{From: bob, To: dave, Value: 8})
			f.MineEmpty(3)
		}
	}
	_, err := ix.Step(context.Background())
	if !errors.Is(err, errChainMoved) {
		t.Fatalf("first step: got %v, want errChainMoved", err)
	}
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

func TestShrinksTheLogRangeWhenRejected(t *testing.T) {
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	f.MaxLogRange = 7
	for i := range 120 {
		if i%9 == 0 {
			f.Mine(chaintest.Spec{From: carol, To: alice, Value: int64(i + 1)})
		} else {
			f.Mine()
		}
	}
	f.SetFinality(115, 110)
	ix := newIndexer(f, s, func(c *Config) { c.MaxRange = 100 })
	sync(t, ix)
	assertMatchesChain(t, s, f, 1, store.Finalized)
	if ix.span > 7 {
		t.Fatalf("span %d exceeds the node limit", ix.span)
	}
}

func TestRefusesToRewindFinalizedData(t *testing.T) {
	ctx := context.Background()
	s := storetest.Open(t, chainID)
	f := chaintest.New(chainID, 1)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 1})
	f.MineEmpty(4)
	f.SetFinality(5, 5)
	sync(t, newIndexer(f, s))

	// A node on a different chain disagrees with blocks we already treat as final
	other := chaintest.New(chainID, 2)
	other.Mine(chaintest.Spec{From: carol, To: bob, Value: 1}, chaintest.Spec{From: carol, To: bob, Value: 2})
	other.MineEmpty(6)
	other.SetFinality(7, 7)
	_, err := newIndexer(other, s).Step(ctx)
	if !errors.Is(err, store.ErrBelowFinalized) {
		t.Fatalf("got %v, want ErrBelowFinalized", err)
	}
	assertMatchesChain(t, s, f, 1, store.Finalized)
}

// TestRandomisedChainHistory mines, reorgs, finalizes, fails RPC calls and restarts the indexer at
// random, and checks that after every round the database matches the canonical chain exactly.
func TestRandomisedChainHistory(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 3
	}
	if n, err := strconv.Atoi(os.Getenv("SOAK_SEEDS")); err == nil {
		seeds = n // e.g. SOAK_SEEDS=500 go test -run Randomised ./internal/indexer
	}
	people := []common.Address{alice, bob, carol, dave}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 42))
			s := storetest.Open(t, chainID)
			f := chaintest.New(chainID, uint64(seed))
			f.MaxLogRange = uint64(3 + rng.IntN(20))
			confirmAt := store.Level(rng.IntN(3))
			newIx := func() *Indexer { return newIndexer(f, s, func(c *Config) { c.ConfirmAt = confirmAt }) }
			ix := newIx()
			var safe, finalized uint64

			for round := range 40 {
				switch op := rng.IntN(10); {
				case op < 5: // mine a few blocks
					for range 1 + rng.IntN(4) {
						var specs []chaintest.Spec
						for range rng.IntN(3) {
							specs = append(specs, chaintest.Spec{From: people[rng.IntN(4)], To: people[rng.IntN(4)], Value: int64(1 + rng.IntN(1000))})
						}
						f.Mine(specs...)
					}
				case op < 7: // reorg, sometimes re-including a removed transfer
					if depth := min(int(f.Head()-f.Finalized()), 1+rng.IntN(6)); depth > 0 {
						removed := f.Reorg(depth)
						for range depth + rng.IntN(3) {
							if len(removed) > 0 && rng.IntN(2) == 0 {
								// A transaction can be mined at most once on the new fork
								i := rng.IntN(len(removed))
								r := removed[i]
								removed = append(removed[:i], removed[i+1:]...)
								f.Mine(chaintest.Spec{From: r.From, To: r.To, Value: r.Value.Int64(), TxHash: r.TxHash})
							} else {
								f.MineEmpty(1)
							}
						}
					}
				case op < 9: // finality advances
					head := f.Head()
					safe = max(safe, head-min(head, uint64(rng.IntN(4))))
					finalized = max(finalized, safe-min(safe, uint64(rng.IntN(6))))
					f.SetFinality(safe, finalized)
				default: // crash and restart
					ix = newIx()
				}
				f.SetFailRate([]float64{0, 0, 0.1, 0.3}[rng.IntN(4)])
				for range rng.IntN(4) {
					_, _ = ix.Step(context.Background())
				}
				if round%10 == 9 {
					f.SetFailRate(0)
					sync(t, ix)
					assertMatchesChain(t, s, f, 1, confirmAt)
				}
			}
			f.SetFailRate(0)
			sync(t, ix)
			assertMatchesChain(t, s, f, 1, confirmAt)
		})
	}
}
