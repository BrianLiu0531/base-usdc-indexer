// Package chain defines what the indexer needs from a node, and a JSON-RPC implementation of it.
package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Header is the part of a block header the indexer stores to detect reorgs.
type Header struct {
	Number     uint64
	Hash       common.Hash
	ParentHash common.Hash
	Time       uint64
}

// Tips are the chain's current block heights at each level of finality.
// On an OP Stack chain such as Base, "safe" blocks have their data posted to L1
// and "finalized" blocks are derived from finalized L1 blocks, so they cannot reorg.
type Tips struct {
	Latest    uint64
	Safe      uint64
	Finalized uint64
}

// Transfer is one decoded ERC-20 Transfer log.
type Transfer struct {
	BlockNumber uint64
	BlockHash   common.Hash
	TxHash      common.Hash
	LogIndex    uint
	From        common.Address
	To          common.Address
	Value       *big.Int
}

// Key identifies a log independently of the block it landed in.
// After a reorg the same transaction can be re-mined in a different block, keeping this key.
func (t Transfer) Key() string { return fmt.Sprintf("%s:%d", t.TxHash.Hex(), t.LogIndex) }

// Client is the read-only view of the chain used by the indexer and reconciler.
type Client interface {
	ChainID(ctx context.Context) (uint64, error)
	Tips(ctx context.Context) (Tips, error)
	Header(ctx context.Context, number uint64) (Header, error)
	// Headers returns the headers for the given block numbers, in the same order.
	Headers(ctx context.Context, numbers []uint64) ([]Header, error)
	// Transfers returns token Transfer logs in from..to that involve any watched address,
	// ordered by block and log index, each listed once.
	Transfers(ctx context.Context, token common.Address, watch []common.Address, from, to uint64) ([]Transfer, error)
	BalanceOf(ctx context.Context, token, holder common.Address, block uint64) (*big.Int, error)
}

// ErrRangeTooLarge means the node refused a log query because the block range or result set was too big.
// The caller should retry with a smaller range.
var ErrRangeTooLarge = errors.New("log query range too large")

// Range returns the block numbers from..to inclusive.
func Range(from, to uint64) []uint64 {
	if to < from {
		return nil
	}
	out := make([]uint64, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, n)
	}
	return out
}

// ErrNodeBehind means the node answering a request has not reached the requested block yet.
var ErrNodeBehind = errors.New("node is behind")

// ErrNotFound means the requested block does not exist on the node's current chain.
var ErrNotFound = errors.New("block not found")
