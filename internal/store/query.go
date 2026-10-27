package store

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

const transferColumns = `block_number, block_hash, block_time, tx_hash, log_index, from_address, to_address, value::text, status, confirmed_at IS NOT NULL`

func scanTransfer(r pgx.CollectableRow) (TransferRow, error) {
	var (
		t      TransferRow
		n, bt  int64
		value  string
		status int16
	)
	err := r.Scan(&n, &t.BlockHash, &bt, &t.TxHash, &t.LogIndex, &t.From, &t.To, &value, &status, &t.Confirmed)
	t.BlockNumber, t.BlockTime, t.Value, t.Status = uint64(n), time.Unix(bt, 0).UTC(), parseBig(value), Level(status)
	return t, err
}

// TransferRow is a stored transfer.
type TransferRow struct {
	BlockNumber uint64    `json:"blockNumber"`
	BlockHash   string    `json:"blockHash"`
	BlockTime   time.Time `json:"blockTime"`
	TxHash      string    `json:"txHash"`
	LogIndex    int       `json:"logIndex"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Value       *big.Int  `json:"-"`
	Status      Level     `json:"-"`
	Confirmed   bool      `json:"confirmed"`
}

// Page is a keyset position: transfers strictly older than (BlockNumber, LogIndex).
type Page struct {
	BeforeBlock uint64
	BeforeLog   int
	Limit       int
}

// ListTransfers returns transfers sent or received by address, newest first.
func (s *Store) ListTransfers(ctx context.Context, address common.Address, p Page) ([]TransferRow, error) {
	before := int64(p.BeforeBlock)
	if before == 0 {
		before = 1<<62 - 1
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+transferColumns+`
		FROM transfers
		WHERE chain_id = $1 AND (from_address = $2 OR to_address = $2) AND (block_number, log_index) < ($3, $4)
		ORDER BY block_number DESC, log_index DESC
		LIMIT $5`, s.chainID, addr(address), before, p.BeforeLog, p.Limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTransfer)
}

// Flow is the total received and sent by an address.
type Flow struct {
	Received, Sent *big.Int
	Count          int
}

// Net is received minus sent.
func (f Flow) Net() *big.Int { return new(big.Int).Sub(f.Received, f.Sent) }

// FlowOf sums an address's transfers in blocks up to and including upTo, at or above the given level.
func (s *Store) FlowOf(ctx context.Context, address common.Address, upTo uint64, atLeast Level) (Flow, error) {
	var received, sent string
	var f Flow
	err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(value) FILTER (WHERE to_address = $2), 0)::text,
			COALESCE(SUM(value) FILTER (WHERE from_address = $2), 0)::text,
			count(*)
		FROM transfers
		WHERE chain_id = $1 AND (from_address = $2 OR to_address = $2) AND block_number <= $3 AND status >= $4`,
		s.chainID, addr(address), int64(min(upTo, 1<<62)), int16(atLeast)).Scan(&received, &sent, &f.Count)
	f.Received, f.Sent = parseBig(received), parseBig(sent)
	return f, err
}

// Baseline is the opening balance fixed by the first reconciliation.
type Baseline struct {
	Block  uint64
	Offset *big.Int
}

// Baseline returns the address's opening balance, or nil if it has not been reconciled yet.
func (s *Store) Baseline(ctx context.Context, address common.Address) (*Baseline, error) {
	var (
		block  int64
		offset string
	)
	err := s.pool.QueryRow(ctx, `SELECT block, offset_value::text FROM reconcile_baselines WHERE chain_id = $1 AND address = $2`,
		s.chainID, addr(address)).Scan(&block, &offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Baseline{Block: uint64(block), Offset: parseBig(offset)}, nil
}

// SetBaseline stores the opening balance if none exists yet and returns whichever is stored.
func (s *Store) SetBaseline(ctx context.Context, address common.Address, b Baseline) (*Baseline, error) {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO reconcile_baselines (chain_id, address, block, offset_value) VALUES ($1, $2, $3, $4::numeric)
		ON CONFLICT DO NOTHING`, s.chainID, addr(address), int64(b.Block), b.Offset.String()); err != nil {
		return nil, err
	}
	return s.Baseline(ctx, address)
}

// Reconciliation is one balance check of a watched address.
type Reconciliation struct {
	Address   string    `json:"address"`
	Block     uint64    `json:"block"`
	Onchain   *big.Int  `json:"-"`
	Expected  *big.Int  `json:"-"`
	OK        bool      `json:"ok"`
	CheckedAt time.Time `json:"checkedAt"`
}

func (s *Store) RecordReconciliation(ctx context.Context, r Reconciliation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO reconciliations (chain_id, address, block, onchain, expected, ok) VALUES ($1, $2, $3, $4::numeric, $5::numeric, $6)`,
		s.chainID, r.Address, int64(r.Block), r.Onchain.String(), r.Expected.String(), r.OK)
	return err
}

// LatestReconciliation returns the most recent check for address, or nil.
func (s *Store) LatestReconciliation(ctx context.Context, address common.Address) (*Reconciliation, error) {
	var (
		r                 Reconciliation
		block             int64
		onchain, expected string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT address, block, onchain::text, expected::text, ok, checked_at FROM reconciliations
		WHERE chain_id = $1 AND address = $2 ORDER BY id DESC LIMIT 1`, s.chainID, addr(address)).
		Scan(&r.Address, &block, &onchain, &expected, &r.OK, &r.CheckedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.Block, r.Onchain, r.Expected = uint64(block), parseBig(onchain), parseBig(expected)
	return &r, nil
}

// Event is an outbox entry.
type Event struct {
	ID      int64
	EventID string
	Type    string
	Payload json.RawMessage
}

// PublishOutbox hands up to limit unpublished events, oldest first, to publish, and marks them
// published only if it succeeds. Rows are locked with SKIP LOCKED, so several relays can run at once.
// Delivery is at-least-once: if the process dies after publishing but before committing, the events
// are sent again, which consumers handle by deduplicating on EventID.
func (s *Store) PublishOutbox(ctx context.Context, limit int, publish func(context.Context, []Event) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, event_id, type, payload FROM outbox
			WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
			var e Event
			err := r.Scan(&e.ID, &e.EventID, &e.Type, &e.Payload)
			return e, err
		})
		if err != nil || len(events) == 0 {
			return err
		}
		if err := publish(ctx, events); err != nil {
			return err
		}
		ids := make([]int64, len(events))
		for i, e := range events {
			ids[i] = e.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		n = len(events)
		return nil
	})
	return n, err
}

// OutboxEvents returns every outbox event in order (for tests and debugging).
func (s *Store) OutboxEvents(ctx context.Context) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, event_id, type, payload FROM outbox ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.EventID, &e.Type, &e.Payload)
		return e, err
	})
}

// AllTransfers returns every stored transfer in chain order (for tests and debugging).
func (s *Store) AllTransfers(ctx context.Context) ([]TransferRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+transferColumns+`
		FROM transfers WHERE chain_id = $1 ORDER BY block_number, log_index`, s.chainID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTransfer)
}
