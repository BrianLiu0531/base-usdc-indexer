package chain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"math/rand/v2"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/time/rate"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/metrics"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

// RPCConfig configures the JSON-RPC client.
type RPCConfig struct {
	URLs        []string      // tried in order; the client sticks to the last one that worked
	MaxRPS      float64       // client-side rate limit across all endpoints
	MaxAttempts int           // attempts per call, rotating through endpoints
	Timeout     time.Duration // per attempt
}

type endpoint struct {
	label string // scheme://host only, safe to log (URLs often embed API keys)
	url   string
	rpc   *rpc.Client
	eth   *ethclient.Client
}

// RPC is a Client backed by one or more JSON-RPC endpoints, with retries, failover and rate limiting.
type RPC struct {
	endpoints []*endpoint
	preferred atomic.Int32
	limiter   *rate.Limiter
	attempts  int
	timeout   time.Duration
	log       *slog.Logger
	batchSize int // max requests per JSON-RPC batch; providers rate-limit each batch element
}

const headerBatchSize = 100

// DialRPC connects to every configured endpoint.
func DialRPC(ctx context.Context, cfg RPCConfig, log *slog.Logger) (*RPC, error) {
	if len(cfg.URLs) == 0 {
		return nil, errors.New("at least one RPC URL is required")
	}
	c := &RPC{
		limiter:  rate.NewLimiter(rate.Limit(cfg.MaxRPS), max(3, int(cfg.MaxRPS))), // burst covers the 3-element tips batch,
		attempts: max(1, cfg.MaxAttempts),
		timeout:  cfg.Timeout,
		log:      log,
	}
	c.batchSize = min(headerBatchSize, c.limiter.Burst())
	for _, raw := range cfg.URLs {
		rc, err := rpc.DialContext(ctx, raw)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("dial %s: %w", redactURL(raw), scrub(err, raw))
		}
		c.endpoints = append(c.endpoints, &endpoint{label: redactURL(raw), url: raw, rpc: rc, eth: ethclient.NewClient(rc)})
	}
	return c, nil
}

// Close closes every endpoint.
func (c *RPC) Close() {
	for _, ep := range c.endpoints {
		ep.rpc.Close()
	}
}

// do runs fn with retries and exponential backoff, moving to the next endpoint after each failure.
// cost is the number of requests fn makes (batch elements count individually towards provider limits).
func (c *RPC) do(ctx context.Context, method string, cost int, fn func(ctx context.Context, ep *endpoint) error) error {
	var err error
	start := int(c.preferred.Load())
	for attempt := 0; attempt < c.attempts; attempt++ {
		if werr := c.limiter.WaitN(ctx, cost); werr != nil {
			return werr
		}
		i := (start + attempt) % len(c.endpoints)
		ep := c.endpoints[i]

		actx, cancel := context.WithTimeout(ctx, c.timeout)
		began := time.Now()
		err = fn(actx, ep)
		cancel()
		metrics.RPCDuration.WithLabelValues(method).Observe(time.Since(began).Seconds())

		if err == nil {
			c.preferred.Store(int32(i))
			return nil
		}
		err = scrub(err, ep.url)
		switch {
		case errors.Is(err, ErrNotFound):
			return err
		case IsRangeError(err):
			return fmt.Errorf("%s: %w: %v", method, ErrRangeTooLarge, err)
		case ctx.Err() != nil:
			return ctx.Err()
		}
		metrics.RPCErrors.WithLabelValues(method).Inc()
		c.log.Warn("rpc call failed", "method", method, "endpoint", ep.label, "attempt", attempt+1, "err", err)
		if attempt+1 < c.attempts {
			if serr := sleep(ctx, backoff(attempt)); serr != nil {
				return serr
			}
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", method, c.attempts, err)
}

func (c *RPC) ChainID(ctx context.Context) (uint64, error) {
	var id *big.Int
	err := c.do(ctx, "eth_chainId", 1, func(ctx context.Context, ep *endpoint) (err error) {
		id, err = ep.eth.ChainID(ctx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return id.Uint64(), nil
}

type rpcHeader struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
	Timestamp  hexutil.Uint64 `json:"timestamp"`
}

func (h *rpcHeader) header() Header {
	return Header{Number: uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Time: uint64(h.Timestamp)}
}

// getBlocks fetches block headers by tag or number in a single batch request.
// Only the header fields are decoded, which keeps this independent of OP Stack transaction types.
func (c *RPC) getBlocks(ctx context.Context, method string, tags []string) ([]Header, error) {
	out := make([]Header, len(tags))
	err := c.do(ctx, method, len(tags), func(ctx context.Context, ep *endpoint) error {
		results := make([]*rpcHeader, len(tags))
		batch := make([]rpc.BatchElem, len(tags))
		for i, tag := range tags {
			batch[i] = rpc.BatchElem{Method: "eth_getBlockByNumber", Args: []any{tag, false}, Result: &results[i]}
		}
		if err := ep.rpc.BatchCallContext(ctx, batch); err != nil {
			return err
		}
		for i, el := range batch {
			if el.Error != nil {
				return el.Error
			}
			if results[i] == nil {
				return fmt.Errorf("%w: %s", ErrNotFound, tags[i])
			}
			out[i] = results[i].header()
		}
		return nil
	})
	return out, err
}

func (c *RPC) Tips(ctx context.Context) (Tips, error) {
	hs, err := c.getBlocks(ctx, "tips", []string{"latest", "safe", "finalized"})
	if err != nil {
		return Tips{}, err
	}
	return Tips{Latest: hs[0].Number, Safe: hs[1].Number, Finalized: hs[2].Number}, nil
}

func (c *RPC) Header(ctx context.Context, number uint64) (Header, error) {
	hs, err := c.getBlocks(ctx, "eth_getBlockByNumber", []string{hexutil.EncodeUint64(number)})
	if err != nil {
		return Header{}, err
	}
	return hs[0], nil
}

func (c *RPC) Headers(ctx context.Context, numbers []uint64) ([]Header, error) {
	out := make([]Header, 0, len(numbers))
	for start := 0; start < len(numbers); start += c.batchSize {
		chunk := numbers[start:min(len(numbers), start+c.batchSize)]
		tags := make([]string, len(chunk))
		for i, n := range chunk {
			tags[i] = hexutil.EncodeUint64(n)
		}
		hs, err := c.getBlocks(ctx, "eth_getBlockByNumber", tags)
		if err != nil {
			return nil, err
		}
		out = append(out, hs...)
	}
	return out, nil
}

func (c *RPC) Transfers(ctx context.Context, token common.Address, watch []common.Address, from, to uint64) ([]Transfer, error) {
	topics := make([]common.Hash, len(watch))
	for i, a := range watch {
		topics[i] = common.BytesToHash(a.Bytes())
	}
	// Topic filters are ANDed across positions, so "from OR to is watched" needs two queries.
	queries := [][][]common.Hash{
		{{usdc.TransferTopic}, topics},
		{{usdc.TransferTopic}, nil, topics},
	}
	seen := map[string]Transfer{}
	for _, q := range queries {
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(from),
			ToBlock:   new(big.Int).SetUint64(to),
			Addresses: []common.Address{token},
			Topics:    q,
		}
		var logs []rpcLog
		err := c.do(ctx, "eth_getLogs", 1, func(ctx context.Context, ep *endpoint) error {
			logs = nil
			return ep.rpc.CallContext(ctx, &logs, "eth_getLogs", toFilterArg(query))
		})
		if err != nil {
			return nil, err
		}
		for _, l := range logs {
			if t, ok := l.transfer(); ok {
				seen[t.Key()] = t // a transfer between two watched addresses matches both queries
			}
		}
	}
	return sortTransfers(seen), nil
}

func (c *RPC) BalanceOf(ctx context.Context, token, holder common.Address, block uint64) (*big.Int, error) {
	data := append(common.FromHex("0x70a08231"), common.LeftPadBytes(holder.Bytes(), 32)...) // balanceOf(address)
	var out []byte
	err := c.do(ctx, "eth_call", 1, func(ctx context.Context, ep *endpoint) (err error) {
		out, err = ep.eth.CallContract(ctx, ethereum.CallMsg{To: &token, Data: data}, new(big.Int).SetUint64(block))
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(out) != 32 {
		return nil, fmt.Errorf("balanceOf returned %d bytes", len(out))
	}
	return new(big.Int).SetBytes(out), nil
}

// rpcLog decodes only the log fields used here.
type rpcLog struct {
	Topics      []common.Hash  `json:"topics"`
	Data        hexutil.Bytes  `json:"data"`
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	BlockHash   common.Hash    `json:"blockHash"`
	TxHash      common.Hash    `json:"transactionHash"`
	Index       hexutil.Uint   `json:"logIndex"`
	Removed     bool           `json:"removed"`
}

func (l rpcLog) transfer() (Transfer, bool) {
	if l.Removed || len(l.Topics) != 3 || l.Topics[0] != usdc.TransferTopic || len(l.Data) != 32 {
		return Transfer{}, false
	}
	return Transfer{
		BlockNumber: uint64(l.BlockNumber),
		BlockHash:   l.BlockHash,
		TxHash:      l.TxHash,
		LogIndex:    uint(l.Index),
		From:        common.BytesToAddress(l.Topics[1].Bytes()),
		To:          common.BytesToAddress(l.Topics[2].Bytes()),
		Value:       new(big.Int).SetBytes(l.Data),
	}, true
}

func toFilterArg(q ethereum.FilterQuery) map[string]any {
	return map[string]any{
		"address":   q.Addresses,
		"topics":    q.Topics,
		"fromBlock": hexutil.EncodeBig(q.FromBlock),
		"toBlock":   hexutil.EncodeBig(q.ToBlock),
	}
}

func sortTransfers(m map[string]Transfer) []Transfer {
	out := make([]Transfer, 0, len(m))
	for _, t := range m {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockNumber != out[j].BlockNumber {
			return out[i].BlockNumber < out[j].BlockNumber
		}
		return out[i].LogIndex < out[j].LogIndex
	})
	return out
}

// IsRangeError reports whether a node rejected a log query for covering too many blocks or results.
// Providers word this differently, so it matches on the common phrasings. Rate limits are not range errors.
func IsRangeError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRangeTooLarge) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "rate limit") || strings.Contains(msg, "too many requests") {
		return false
	}
	for _, s := range []string{"query returned more than", "too many results", "too many logs", "response size", "exceeds max results"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	// "eth_getLogs is limited to a 200 range", "block range too large", "exceed maximum block range: 5000", ...
	if strings.Contains(msg, "range") {
		for _, s := range []string{"limit", "exceed", "too large", "too wide", "too big", "max", "up to"} {
			if strings.Contains(msg, s) {
				return true
			}
		}
	}
	return false
}

// backoff is exponential with full jitter: a random delay in [0, min(8s, 250ms*2^attempt)].
func backoff(attempt int) time.Duration {
	d := min(8*time.Second, 250*time.Millisecond<<attempt)
	return time.Duration(rand.Int64N(int64(d) + 1))
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// redactURL keeps only the scheme and host, dropping paths and query strings that may hold API keys.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<rpc>"
	}
	return u.Scheme + "://" + u.Host
}

// scrub removes a full endpoint URL from an error message (net/http errors include it).
func scrub(err error, raw string) error {
	if err == nil || !strings.Contains(err.Error(), raw) {
		return err
	}
	return &scrubbedError{msg: strings.ReplaceAll(err.Error(), raw, redactURL(raw)), err: err}
}

type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }
