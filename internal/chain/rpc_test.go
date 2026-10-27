package chain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

func TestIsRangeError(t *testing.T) {
	// Messages returned by real providers
	for msg, want := range map[string]bool{
		"eth_getLogs is limited to a 200 range":                                                     true, // Base public RPC
		"query returned more than 10000 results":                                                    true, // geth / Infura
		"Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range": true, // Alchemy
		"Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range":   true,
		"exceed maximum block range: 5000":                                                          true,
		"block range is too wide":                                                                   true,
		"429 Too Many Requests":                                                                     false,
		"rate limit exceeded, max 25 requests per second over a block range":                        false,
		"header not found":                                                                          false,
	} {
		if got := IsRangeError(errors.New(msg)); got != want {
			t.Errorf("IsRangeError(%q) = %v, want %v", msg, got, want)
		}
	}
}

// rpcServer is a minimal JSON-RPC server that answers single and batch requests with handle.
func rpcServer(t *testing.T, handle func(method string, params []json.RawMessage) (any, error)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		type req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		answer := func(q req) map[string]any {
			res, err := handle(q.Method, q.Params)
			if err != nil {
				return map[string]any{"jsonrpc": "2.0", "id": q.ID, "error": map[string]any{"code": -32000, "message": err.Error()}}
			}
			return map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": res}
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
			var batch []req
			_ = json.Unmarshal(body, &batch)
			out := make([]map[string]any, len(batch))
			for i, q := range batch {
				out[i] = answer(q)
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		var q req
		_ = json.Unmarshal(body, &q)
		_ = json.NewEncoder(w).Encode(answer(q))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, urls ...string) *RPC {
	t.Helper()
	c, err := DialRPC(context.Background(), RPCConfig{URLs: urls, MaxRPS: 1000, MaxAttempts: 3, Timeout: 2 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func hexHash(b byte) string { return common.BytesToHash([]byte{b}).Hex() }

func TestTransfersQueriesBothDirectionsAndMergesDuplicates(t *testing.T) {
	alice := common.HexToAddress("0x00000000000000000000000000000000000a11ce")
	bob := common.HexToAddress("0x0000000000000000000000000000000000000b0b")
	topic := func(a common.Address) string { return common.BytesToHash(a.Bytes()).Hex() }
	log := func(block, index int, from, to common.Address, value int) map[string]any {
		return map[string]any{
			"topics":          []string{usdc.TransferTopic.Hex(), topic(from), topic(to)},
			"data":            fmt.Sprintf("0x%064x", value),
			"blockNumber":     fmt.Sprintf("0x%x", block),
			"blockHash":       hexHash(byte(block)),
			"transactionHash": hexHash(byte(100 + index)),
			"logIndex":        fmt.Sprintf("0x%x", index),
		}
	}
	var filters []map[string]any
	srv := rpcServer(t, func(method string, params []json.RawMessage) (any, error) {
		if method == "eth_blockNumber" {
			return "0x64", nil
		}
		var f map[string]any
		_ = json.Unmarshal(params[0], &f)
		filters = append(filters, f)
		topics := f["topics"].([]any)
		if len(topics) == 2 { // from is watched
			return []any{log(7, 1, alice, bob, 5), log(5, 0, alice, common.Address{9}, 1)}, nil
		}
		return []any{log(7, 1, alice, bob, 5), log(6, 3, common.Address{8}, bob, 2)}, nil // to is watched
	})

	got, err := dial(t, srv.URL).Transfers(context.Background(), common.Address{1}, []common.Address{alice, bob}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].BlockNumber != 5 || got[1].BlockNumber != 6 || got[2].BlockNumber != 7 {
		t.Fatalf("got %+v", got)
	}
	if got[2].From != alice || got[2].To != bob || got[2].Value.Int64() != 5 {
		t.Fatalf("decoded %+v", got[2])
	}
	if len(filters) != 2 || filters[0]["fromBlock"] != "0x1" || filters[0]["toBlock"] != "0xa" {
		t.Fatalf("filters %v", filters)
	}
	// Only the official token contract is queried, so look-alike tokens never match
	if addrs := filters[0]["address"].([]any); len(addrs) != 1 || addrs[0] != (common.Address{1}).Hex() {
		t.Fatalf("address filter %v", filters[0]["address"])
	}
	second := filters[1]["topics"].([]any)
	if second[1] != nil || len(second[2].([]any)) != 2 {
		t.Fatalf("to-filter topics %v", second)
	}
}

func TestIgnoresLogsFromANodeBehindTheRange(t *testing.T) {
	behind := rpcServer(t, func(method string, _ []json.RawMessage) (any, error) {
		if method == "eth_blockNumber" {
			return "0x5", nil // has only reached block 5
		}
		return []any{}, nil // would silently return nothing for blocks 6-10
	})
	_, err := dial(t, behind.URL).Transfers(context.Background(), common.Address{1}, []common.Address{{2}}, 1, 10)
	if !errors.Is(err, ErrNodeBehind) {
		t.Fatalf("got %v, want ErrNodeBehind", err)
	}
}

func TestFailsOverToTheNextEndpoint(t *testing.T) {
	var badCalls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badCalls.Add(1)
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	t.Cleanup(bad.Close)
	good := rpcServer(t, func(method string, params []json.RawMessage) (any, error) {
		var tag string
		_ = json.Unmarshal(params[0], &tag)
		n := map[string]int{"latest": 30, "safe": 20, "finalized": 10}[tag]
		return map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": hexHash(byte(n)), "parentHash": hexHash(byte(n - 1)), "timestamp": "0x1"}, nil
	})

	c := dial(t, bad.URL, good.URL)
	tips, err := c.Tips(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tips != (Tips{Latest: 30, Safe: 20, Finalized: 10}) {
		t.Fatalf("tips %+v", tips)
	}
	// The client sticks with the endpoint that worked
	before := badCalls.Load()
	if _, err := c.Tips(context.Background()); err != nil {
		t.Fatal(err)
	}
	if badCalls.Load() != before {
		t.Fatal("retried the failing endpoint after a success elsewhere")
	}
}

func TestErrorsDoNotLeakAPIKeys(t *testing.T) {
	// Nothing listens here, so the dial error includes the URL
	c := dial(t, "http://127.0.0.1:1/v2/super-secret-key")
	_, err := c.ChainID(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "super-secret-key") {
		t.Fatalf("error leaks the key: %v", err)
	}
}

func TestMissingBlockIsNotFound(t *testing.T) {
	srv := rpcServer(t, func(string, []json.RawMessage) (any, error) { return nil, nil })
	_, err := dial(t, srv.URL).Header(context.Background(), 99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}
