// Package store persists blocks, transfers and outbox events in PostgreSQL.
//
// Every write that changes the indexer's position happens in one transaction that also checks
// the cursor has not moved, so a crash or a second instance can never leave partial or duplicate data.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain"
)

// Level is a finality level. Transfers move up through the levels and never down.
type Level int16

const (
	Pending   Level = 0 // included in the latest block, may still be reorged
	Safe      Level = 1 // batch data posted to L1
	Finalized Level = 2 // derived from finalized L1 blocks; cannot be reorged
)

func (l Level) String() string {
	switch l {
	case Pending:
		return "pending"
	case Safe:
		return "safe"
	case Finalized:
		return "finalized"
	}
	return fmt.Sprintf("level(%d)", l)
}

// ParseLevel parses "pending", "safe" or "finalized".
func ParseLevel(s string) (Level, error) {
	for _, l := range []Level{Pending, Safe, Finalized} {
		if strings.EqualFold(s, l.String()) {
			return l, nil
		}
	}
	return 0, fmt.Errorf("unknown finality level %q (use pending, safe or finalized)", s)
}

var (
	// ErrCursorMoved means another writer changed the indexed chain since the caller read the cursor.
	ErrCursorMoved = errors.New("cursor moved since it was read")
	// ErrBelowFinalized means a rewind would remove finalized blocks, which must never happen.
	ErrBelowFinalized = errors.New("refusing to rewind below the finalized block")
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store is the PostgreSQL store for one chain.
type Store struct {
	pool    *pgxpool.Pool
	chainID int64
}

// Option customises Open.
type Option func(*pgxpool.Config)

// WithSchema puts all tables in the given schema (used to isolate tests).
func WithSchema(schema string) Option {
	return func(c *pgxpool.Config) { c.ConnConfig.RuntimeParams["search_path"] = schema }
}

// Open connects to PostgreSQL. Call Migrate before first use.
func Open(ctx context.Context, databaseURL string, chainID uint64, opts ...Option) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	for _, o := range opts {
		o(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{pool: pool, chainID: int64(chainID)}, nil
}

func (s *Store) Close() { s.pool.Close() }

// ChainID is the chain this store holds data for.
func (s *Store) ChainID() uint64 { return uint64(s.chainID) }

// Migrate applies any migrations that have not run yet.
func (s *Store) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Serialise concurrent start-ups
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7461001)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		for _, name := range names {
			version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
			var done bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
				return err
			}
			if done {
				continue
			}
			sql, err := migrations.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("migration %s: %w", version, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
				return err
			}
		}
		return nil
	})
}

// Cursor returns the highest stored block, or nil before the first batch.
func (s *Store) Cursor(ctx context.Context) (*chain.Header, error) {
	return cursor(ctx, s.pool, s.chainID)
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func cursor(ctx context.Context, q querier, chainID int64) (*chain.Header, error) {
	return scanHeader(q.QueryRow(ctx, `SELECT number, hash, parent_hash, time FROM blocks WHERE chain_id = $1 ORDER BY number DESC LIMIT 1`, chainID))
}

// BlockAtOrBelow returns the highest stored block at or below n, or nil if there is none.
func (s *Store) BlockAtOrBelow(ctx context.Context, n uint64) (*chain.Header, error) {
	return scanHeader(s.pool.QueryRow(ctx,
		`SELECT number, hash, parent_hash, time FROM blocks WHERE chain_id = $1 AND number <= $2 ORDER BY number DESC LIMIT 1`, s.chainID, int64(n)))
}

func scanHeader(row pgx.Row) (*chain.Header, error) {
	var (
		number, time int64
		hash, parent string
	)
	if err := row.Scan(&number, &hash, &parent, &time); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &chain.Header{Number: uint64(number), Hash: common.HexToHash(hash), ParentHash: common.HexToHash(parent), Time: uint64(time)}, nil
}

// SyncState is the finality the indexer has applied, and the latest head it has seen.
type SyncState struct {
	Safe, Finalized, LatestSeen uint64
}

func (s *Store) State(ctx context.Context) (SyncState, error) {
	var st struct{ safe, fin, latest int64 }
	err := s.pool.QueryRow(ctx, `SELECT safe_block, finalized_block, latest_seen FROM sync_state WHERE chain_id = $1`, s.chainID).
		Scan(&st.safe, &st.fin, &st.latest)
	if errors.Is(err, pgx.ErrNoRows) {
		return SyncState{}, nil
	}
	return SyncState{Safe: uint64(st.safe), Finalized: uint64(st.fin), LatestSeen: uint64(st.latest)}, err
}

// Batch is one step forward: new canonical blocks on top of Parent, the transfers in them,
// and the chain's current finality.
type Batch struct {
	Parent    *chain.Header // the cursor the batch was built on; nil for the first batch
	Blocks    []chain.Header
	Transfers []chain.Transfer
	Tips      chain.Tips
	ConfirmAt Level
}

// Result reports what a batch changed.
type Result struct {
	Inserted  int // new transfer rows
	Confirmed int // transfers that reached the confirmation level (one outbox event each)
}

// blocksKeptBelowFinalized is a margin of block hashes kept under the finalized height.
const blocksKeptBelowFinalized = 128

// Apply writes a batch atomically. It fails with ErrCursorMoved if Parent is no longer the cursor.
// Transfers that are already stored are skipped, so replaying a batch is harmless.
func (s *Store) Apply(ctx context.Context, b Batch) (Result, error) {
	var res Result
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockAndCheck(ctx, tx, b.Parent); err != nil {
			return err
		}

		top := b.Parent
		for i := range b.Blocks {
			h := b.Blocks[i]
			if _, err := tx.Exec(ctx, `INSERT INTO blocks (chain_id, number, hash, parent_hash, time) VALUES ($1, $2, $3, $4, $5)`,
				s.chainID, int64(h.Number), h.Hash.Hex(), h.ParentHash.Hex(), int64(h.Time)); err != nil {
				return fmt.Errorf("insert block %d: %w", h.Number, err)
			}
			top = &b.Blocks[i]
		}
		if top == nil {
			return nil
		}

		if len(b.Transfers) > 0 {
			n, err := s.insertTransfers(ctx, tx, b.Transfers, b.Blocks)
			if err != nil {
				return err
			}
			res.Inserted = n
		}

		// Finality can only be applied to blocks we have stored
		safe, fin := min(b.Tips.Safe, top.Number), min(b.Tips.Finalized, top.Number)
		if _, err := tx.Exec(ctx, `UPDATE transfers SET status = 2 WHERE chain_id = $1 AND status < 2 AND block_number <= $2`, s.chainID, int64(fin)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE transfers SET status = 1 WHERE chain_id = $1 AND status < 1 AND block_number <= $2`, s.chainID, int64(safe)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync_state (chain_id, safe_block, finalized_block, latest_seen) VALUES ($1, $2, $3, $4)
			ON CONFLICT (chain_id) DO UPDATE SET
				safe_block = GREATEST(sync_state.safe_block, EXCLUDED.safe_block),
				finalized_block = GREATEST(sync_state.finalized_block, EXCLUDED.finalized_block),
				latest_seen = EXCLUDED.latest_seen,
				updated_at = now()`,
			s.chainID, int64(safe), int64(fin), int64(b.Tips.Latest)); err != nil {
			return err
		}

		confirmed, err := s.emitConfirmations(ctx, tx, b.ConfirmAt)
		if err != nil {
			return err
		}
		res.Confirmed = confirmed

		_, err = tx.Exec(ctx, `DELETE FROM blocks WHERE chain_id = $1 AND number < $2`, s.chainID, int64(fin)-blocksKeptBelowFinalized)
		return err
	})
	return res, err
}

// lockAndCheck serialises writers for this chain and verifies the cursor is still `expected`.
func (s *Store) lockAndCheck(ctx context.Context, tx pgx.Tx, expected *chain.Header) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7461002, $1::int)`, int32(s.chainID)); err != nil {
		return err
	}
	cur, err := cursor(ctx, tx, s.chainID)
	if err != nil {
		return err
	}
	switch {
	case cur == nil && expected == nil:
		return nil
	case cur == nil || expected == nil, cur.Number != expected.Number, cur.Hash != expected.Hash:
		return ErrCursorMoved
	}
	return nil
}

func (s *Store) insertTransfers(ctx context.Context, tx pgx.Tx, ts []chain.Transfer, blocks []chain.Header) (int, error) {
	times := make(map[uint64]uint64, len(blocks))
	for _, h := range blocks {
		times[h.Number] = h.Time
	}
	var (
		numbers, blockTimes                       []int64
		logIndexes                                []int32
		blockHashes, txHashes, froms, tos, values []string
	)
	for _, t := range ts {
		bt, ok := times[t.BlockNumber]
		if !ok {
			return 0, fmt.Errorf("transfer %s is in block %d, which is not part of the batch", t.Key(), t.BlockNumber)
		}
		numbers = append(numbers, int64(t.BlockNumber))
		blockTimes = append(blockTimes, int64(bt))
		blockHashes = append(blockHashes, t.BlockHash.Hex())
		txHashes = append(txHashes, t.TxHash.Hex())
		logIndexes = append(logIndexes, int32(t.LogIndex))
		froms = append(froms, addr(t.From))
		tos = append(tos, addr(t.To))
		values = append(values, t.Value.String())
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (chain_id, block_number, block_time, block_hash, tx_hash, log_index, from_address, to_address, value, status)
		SELECT $1, n, bt, bh, th, li, f, t, v::numeric, 0
		FROM unnest($2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::int[], $7::text[], $8::text[], $9::text[])
			AS u(n, bt, bh, th, li, f, t, v)
		ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING`,
		s.chainID, numbers, blockTimes, blockHashes, txHashes, logIndexes, froms, tos, values)
	if err != nil {
		return 0, fmt.Errorf("insert transfers: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// eventPayload builds an outbox payload from a transfers-shaped row alias.
func eventPayload(eventType, alias string) string {
	return fmt.Sprintf(`'%[1]s:' || %[2]s.chain_id || ':' || %[2]s.tx_hash || ':' || %[2]s.log_index || ':' || %[2]s.block_hash,
		'%[1]s',
		jsonb_build_object(
			'type', '%[1]s',
			'chainId', %[2]s.chain_id,
			'txHash', %[2]s.tx_hash,
			'logIndex', %[2]s.log_index,
			'blockNumber', %[2]s.block_number,
			'blockHash', %[2]s.block_hash,
			'from', %[2]s.from_address,
			'to', %[2]s.to_address,
			'value', %[2]s.value::text
		)`, eventType, alias)
}

// emitConfirmations marks transfers that reached the confirmation level and queues one event for each.
func (s *Store) emitConfirmations(ctx context.Context, tx pgx.Tx, level Level) (int, error) {
	tag, err := tx.Exec(ctx, `
		WITH c AS (
			UPDATE transfers SET confirmed_at = now()
			WHERE chain_id = $1 AND confirmed_at IS NULL AND status >= $2
			RETURNING *
		)
		INSERT INTO outbox (event_id, type, payload)
		SELECT `+eventPayload("transfer.confirmed", "c")+` FROM c
		WHERE c.value > 0 -- zero-value transfers (e.g. address-poisoning spam) are stored but not announced
		ORDER BY c.block_number, c.log_index
		ON CONFLICT (event_id) DO NOTHING`, s.chainID, int16(level))
	if err != nil {
		return 0, fmt.Errorf("emit confirmations: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Rewind removes every block above ancestor after a reorg. Removed transfers are copied to
// orphaned_transfers, and a transfer.reverted event is queued for any that had been confirmed.
func (s *Store) Rewind(ctx context.Context, expected *chain.Header, ancestor uint64) (orphaned int, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockAndCheck(ctx, tx, expected); err != nil {
			return err
		}
		var fin int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT finalized_block FROM sync_state WHERE chain_id = $1), 0)`, s.chainID).Scan(&fin); err != nil {
			return err
		}
		if int64(ancestor) < fin {
			return fmt.Errorf("%w: ancestor %d < finalized %d", ErrBelowFinalized, ancestor, fin)
		}
		if err := tx.QueryRow(ctx, `
			WITH gone AS (
				DELETE FROM transfers WHERE chain_id = $1 AND block_number > $2 RETURNING *
			), audit AS (
				INSERT INTO orphaned_transfers (chain_id, block_number, block_hash, tx_hash, log_index, from_address, to_address, value, was_confirmed)
				SELECT chain_id, block_number, block_hash, tx_hash, log_index, from_address, to_address, value, confirmed_at IS NOT NULL FROM gone
			), events AS (
				INSERT INTO outbox (event_id, type, payload)
				SELECT `+eventPayload("transfer.reverted", "gone")+` FROM gone WHERE gone.confirmed_at IS NOT NULL AND gone.value > 0
				ON CONFLICT (event_id) DO NOTHING
			)
			SELECT count(*) FROM gone`, s.chainID, int64(ancestor)).Scan(&orphaned); err != nil {
			return fmt.Errorf("remove orphaned transfers: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM blocks WHERE chain_id = $1 AND number > $2`, s.chainID, int64(ancestor)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE sync_state SET safe_block = LEAST(safe_block, $2), updated_at = now() WHERE chain_id = $1`, s.chainID, int64(ancestor))
		return err
	})
	return orphaned, err
}

func addr(a common.Address) string { return strings.ToLower(a.Hex()) }

func parseBig(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return new(big.Int)
	}
	return v
}
