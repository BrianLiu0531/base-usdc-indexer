// Package api serves the indexer's read-only HTTP API.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

type Server struct {
	Store     *store.Store
	Network   usdc.Network
	Watch     []common.Address
	ConfirmAt store.Level
	Metrics   prometheus.Gatherer
	Log       *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /v1/addresses/{address}", s.address)
	mux.HandleFunc("GET /v1/addresses/{address}/transfers", s.transfers)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.Metrics, promhttp.HandlerOpts{}))
	}
	return mux
}

type statusResponse struct {
	Network        string   `json:"network"`
	ChainID        uint64   `json:"chainId"`
	Token          string   `json:"token"`
	Watching       []string `json:"watching"`
	ConfirmAt      string   `json:"confirmAt"`
	LatestBlock    uint64   `json:"latestBlock"`
	IndexedBlock   uint64   `json:"indexedBlock"`
	SafeBlock      uint64   `json:"safeBlock"`
	FinalizedBlock uint64   `json:"finalizedBlock"`
	LagBlocks      uint64   `json:"lagBlocks"`
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	cur, err := s.Store.Cursor(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	st, err := s.Store.State(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := statusResponse{
		Network: s.Network.Name, ChainID: s.Network.ChainID, Token: s.Network.USDC.Hex(), ConfirmAt: s.ConfirmAt.String(),
		LatestBlock: st.LatestSeen, SafeBlock: st.Safe, FinalizedBlock: st.Finalized,
	}
	for _, a := range s.Watch {
		resp.Watching = append(resp.Watching, a.Hex())
	}
	if cur != nil {
		resp.IndexedBlock = cur.Number
		resp.LagBlocks = st.LatestSeen - min(st.LatestSeen, cur.Number)
	}
	writeJSON(w, http.StatusOK, resp)
}

type amount struct {
	Raw string `json:"raw"`  // base units (6 decimals)
	USD string `json:"usdc"` // human-readable
}

func toAmount(v *big.Int) amount { return amount{Raw: v.String(), USD: usdc.FormatAmount(v)} }

type addressResponse struct {
	Address string `json:"address"`
	// Totals over finalized transfers only, so they never change once reported
	Finalized struct {
		Received amount `json:"received"`
		Sent     amount `json:"sent"`
		Count    int    `json:"count"`
	} `json:"finalized"`
	// Balance implied by the opening balance plus all indexed transfers, once reconciliation has run
	IndexedBalance     *amount               `json:"indexedBalance"`
	LastReconciliation *reconciliationResult `json:"lastReconciliation"`
}

type reconciliationResult struct {
	Block     uint64    `json:"block"`
	OK        bool      `json:"ok"`
	Onchain   amount    `json:"onchain"`
	Expected  amount    `json:"expected"`
	CheckedAt time.Time `json:"checkedAt"`
}

func (s *Server) address(w http.ResponseWriter, r *http.Request) {
	a, ok := s.watchedAddress(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	fin, err := s.Store.FlowOf(ctx, a, ^uint64(0), store.Finalized)
	if err != nil {
		s.fail(w, err)
		return
	}
	all, err := s.Store.FlowOf(ctx, a, ^uint64(0), store.Pending)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := addressResponse{Address: a.Hex()}
	resp.Finalized.Received, resp.Finalized.Sent, resp.Finalized.Count = toAmount(fin.Received), toAmount(fin.Sent), fin.Count

	base, err := s.Store.Baseline(ctx, a)
	if err != nil {
		s.fail(w, err)
		return
	}
	if base != nil {
		bal := toAmount(new(big.Int).Add(base.Offset, all.Net()))
		resp.IndexedBalance = &bal
	}
	rec, err := s.Store.LatestReconciliation(ctx, a)
	if err != nil {
		s.fail(w, err)
		return
	}
	if rec != nil {
		resp.LastReconciliation = &reconciliationResult{Block: rec.Block, OK: rec.OK, Onchain: toAmount(rec.Onchain), Expected: toAmount(rec.Expected), CheckedAt: rec.CheckedAt}
	}
	writeJSON(w, http.StatusOK, resp)
}

type transferJSON struct {
	BlockNumber uint64    `json:"blockNumber"`
	BlockHash   string    `json:"blockHash"`
	BlockTime   time.Time `json:"blockTime"`
	TxHash      string    `json:"txHash"`
	LogIndex    int       `json:"logIndex"`
	Direction   string    `json:"direction"` // in, out or self, relative to the requested address
	From        string    `json:"from"`
	To          string    `json:"to"`
	Amount      amount    `json:"amount"`
	Status      string    `json:"status"`
	Confirmed   bool      `json:"confirmed"`
}

type transfersResponse struct {
	Transfers []transferJSON `json:"transfers"`
	Next      *string        `json:"next"` // pass as ?before= for the next page
}

func (s *Server) transfers(w http.ResponseWriter, r *http.Request) {
	a, ok := s.watchedAddress(w, r)
	if !ok {
		return
	}
	page := store.Page{Limit: 25}
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		page.Limit = n
	}
	if v := q.Get("before"); v != "" {
		if _, err := fmt.Sscanf(v, "%d:%d", &page.BeforeBlock, &page.BeforeLog); err != nil {
			writeError(w, http.StatusBadRequest, "before must look like <block>:<logIndex>")
			return
		}
	}
	rows, err := s.Store.ListTransfers(r.Context(), a, page)
	if err != nil {
		s.fail(w, err)
		return
	}
	me := strings.ToLower(a.Hex())
	resp := transfersResponse{Transfers: make([]transferJSON, 0, len(rows))}
	for _, t := range rows {
		dir := "in"
		switch {
		case t.From == me && t.To == me:
			dir = "self"
		case t.From == me:
			dir = "out"
		}
		resp.Transfers = append(resp.Transfers, transferJSON{
			BlockNumber: t.BlockNumber, BlockHash: t.BlockHash, BlockTime: t.BlockTime, TxHash: t.TxHash, LogIndex: t.LogIndex,
			Direction: dir, From: t.From, To: t.To, Amount: toAmount(t.Value), Status: t.Status.String(), Confirmed: t.Confirmed,
		})
	}
	if len(rows) == page.Limit {
		last := rows[len(rows)-1]
		next := fmt.Sprintf("%d:%d", last.BlockNumber, last.LogIndex)
		resp.Next = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

// watchedAddress parses the {address} path value; only watched addresses are indexed.
func (s *Server) watchedAddress(w http.ResponseWriter, r *http.Request) (common.Address, bool) {
	raw := r.PathValue("address")
	if !common.IsHexAddress(raw) {
		writeError(w, http.StatusBadRequest, "invalid address")
		return common.Address{}, false
	}
	a := common.HexToAddress(raw)
	for _, x := range s.Watch {
		if x == a {
			return a, true
		}
	}
	writeError(w, http.StatusNotFound, "address is not watched")
	return common.Address{}, false
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.Log.Error("api error", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
