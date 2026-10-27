// Package chaintest provides an in-memory chain that can mine blocks, reorg and fail on demand.
package chaintest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain"
)

// Spec describes a transfer to include in a mined block. A zero TxHash gets a unique one.
type Spec struct {
	From, To common.Address
	Value    int64
	TxHash   common.Hash
}

type block struct {
	header    chain.Header
	transfers []chain.Transfer
}

// Fake is a chain.Client backed by memory. It is safe for concurrent use.
type Fake struct {
	mu        sync.Mutex
	chainID   uint64
	blocks    []block // canonical chain; index == block number
	safe      uint64
	finalized uint64
	fork      uint64 // changes the hashes of blocks mined after a reorg
	txSeq     uint64

	rng      *rand.Rand
	failRate float64

	// MaxLogRange, when set, makes Transfers reject ranges longer than this many blocks.
	MaxLogRange uint64
	// BeforeTransfers, when set, runs before each Transfers call (outside the lock), e.g. to reorg mid-fetch.
	BeforeTransfers func()
	// Opening balances used by BalanceOf.
	Opening map[common.Address]*big.Int
}

// New returns a chain containing only the genesis block.
func New(chainID uint64, seed uint64) *Fake {
	f := &Fake{chainID: chainID, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), Opening: map[common.Address]*big.Int{}}
	f.blocks = []block{{header: chain.Header{Number: 0, Hash: crypto.Keccak256Hash([]byte("genesis"))}}}
	return f
}

// SetFailRate makes each call fail with the given probability.
func (f *Fake) SetFailRate(p float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failRate = p
}

// ErrInjected is returned by calls that SetFailRate made fail.
var ErrInjected = errors.New("injected RPC failure")

func (f *Fake) maybeFail() error {
	if f.failRate > 0 && f.rng.Float64() < f.failRate {
		return ErrInjected
	}
	return nil
}

// Mine appends one block containing the given transfers and returns its number.
func (f *Fake) Mine(specs ...Spec) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	parent := f.blocks[len(f.blocks)-1].header
	n := parent.Number + 1
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf, n)
	binary.BigEndian.PutUint64(buf[8:], f.fork)
	binary.BigEndian.PutUint64(buf[16:], uint64(len(specs)))
	h := chain.Header{Number: n, ParentHash: parent.Hash, Hash: crypto.Keccak256Hash(parent.Hash.Bytes(), buf), Time: 1_700_000_000 + n*2}
	b := block{header: h}
	for i, s := range specs {
		tx := s.TxHash
		if tx == (common.Hash{}) {
			f.txSeq++
			tx = crypto.Keccak256Hash([]byte(fmt.Sprintf("tx-%d", f.txSeq)))
		}
		b.transfers = append(b.transfers, chain.Transfer{
			BlockNumber: n, BlockHash: h.Hash, TxHash: tx, LogIndex: uint(i),
			From: s.From, To: s.To, Value: big.NewInt(s.Value),
		})
	}
	f.blocks = append(f.blocks, b)
	return n
}

// MineEmpty mines n blocks without transfers.
func (f *Fake) MineEmpty(n int) {
	for range n {
		f.Mine()
	}
}

// Reorg removes the newest depth blocks; mine new ones afterwards to build the competing fork.
// It returns the transfers that were removed, so tests can re-include some of them.
func (f *Fake) Reorg(depth int) []chain.Transfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	head := uint64(len(f.blocks) - 1)
	if uint64(depth) > head || head-uint64(depth) < f.finalized {
		panic(fmt.Sprintf("reorg of %d blocks would remove finalized block %d", depth, f.finalized))
	}
	var removed []chain.Transfer
	for _, b := range f.blocks[len(f.blocks)-depth:] {
		removed = append(removed, b.transfers...)
	}
	f.blocks = f.blocks[:len(f.blocks)-depth]
	f.fork++
	f.safe = min(f.safe, head-uint64(depth))
	return removed
}

// SetFinality moves the safe and finalized heights (they never go past the head).
func (f *Fake) SetFinality(safe, finalized uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	head := uint64(len(f.blocks) - 1)
	f.safe, f.finalized = min(safe, head), min(finalized, head)
}

// Head returns the latest block number.
func (f *Fake) Head() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.blocks) - 1)
}

// Finalized returns the finalized height.
func (f *Fake) Finalized() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finalized
}

// Canonical returns every transfer on the canonical chain that involves a watched address.
func (f *Fake) Canonical(watch []common.Address) []chain.Transfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.collect(watch, 0, uint64(len(f.blocks)-1))
}

func (f *Fake) collect(watch []common.Address, from, to uint64) []chain.Transfer {
	w := map[common.Address]bool{}
	for _, a := range watch {
		w[a] = true
	}
	var out []chain.Transfer
	for n := from; n <= to && n < uint64(len(f.blocks)); n++ {
		for _, t := range f.blocks[n].transfers {
			if w[t.From] || w[t.To] {
				out = append(out, t)
			}
		}
	}
	return out
}

func (f *Fake) ChainID(context.Context) (uint64, error) { return f.chainID, nil }

func (f *Fake) Tips(context.Context) (chain.Tips, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.maybeFail(); err != nil {
		return chain.Tips{}, err
	}
	return chain.Tips{Latest: uint64(len(f.blocks) - 1), Safe: f.safe, Finalized: f.finalized}, nil
}

func (f *Fake) Header(ctx context.Context, n uint64) (chain.Header, error) {
	hs, err := f.Headers(ctx, []uint64{n})
	if err != nil {
		return chain.Header{}, err
	}
	return hs[0], nil
}

func (f *Fake) Headers(_ context.Context, numbers []uint64) ([]chain.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	out := make([]chain.Header, len(numbers))
	for i, n := range numbers {
		if n >= uint64(len(f.blocks)) {
			return nil, fmt.Errorf("%w: %d", chain.ErrNotFound, n)
		}
		out[i] = f.blocks[n].header
	}
	return out, nil
}

func (f *Fake) Transfers(_ context.Context, _ common.Address, watch []common.Address, from, to uint64) ([]chain.Transfer, error) {
	if f.BeforeTransfers != nil {
		f.BeforeTransfers()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	if f.MaxLogRange > 0 && to-from+1 > f.MaxLogRange {
		return nil, fmt.Errorf("%w: requested %d blocks", chain.ErrRangeTooLarge, to-from+1)
	}
	return f.collect(watch, from, to), nil
}

func (f *Fake) BalanceOf(_ context.Context, _ common.Address, holder common.Address, blockNumber uint64) (*big.Int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	bal := new(big.Int)
	if o := f.Opening[holder]; o != nil {
		bal.Set(o)
	}
	for _, t := range f.collect([]common.Address{holder}, 0, blockNumber) {
		if t.To == holder {
			bal.Add(bal, t.Value)
		}
		if t.From == holder {
			bal.Sub(bal, t.Value)
		}
	}
	return bal, nil
}
