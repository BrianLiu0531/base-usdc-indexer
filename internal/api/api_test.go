package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/chain/chaintest"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/indexer"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/store/storetest"
	"github.com/BrianLiu0531/base-usdc-indexer/internal/usdc"
)

var (
	alice = common.HexToAddress("0x00000000000000000000000000000000000a11ce")
	carol = common.HexToAddress("0x00000000000000000000000000000000000ca201")
)

func setup(t *testing.T) *httptest.Server {
	t.Helper()
	s := storetest.Open(t, 84532)
	f := chaintest.New(84532, 1)
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 2_500_000})
	f.Mine(chaintest.Spec{From: alice, To: carol, Value: 1_000_000}, chaintest.Spec{From: alice, To: alice, Value: 7})
	f.Mine(chaintest.Spec{From: carol, To: alice, Value: 3})
	f.SetFinality(3, 2)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	network := usdc.Networks["base-sepolia"]
	watch := []common.Address{alice}
	ix := indexer.New(indexer.Config{Token: network.USDC, Watch: watch, StartBlock: 1, ConfirmAt: store.Finalized}, f, s, quiet)
	for range 10 {
		if res, err := ix.Step(context.Background()); err != nil || res.CaughtUp {
			break
		}
	}
	srv := httptest.NewServer((&Server{Store: s, Network: network, Watch: watch, ConfirmAt: store.Finalized, Log: quiet}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string, wantCode int, out any) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != wantCode {
		body, _ := io.ReadAll(res.Body)
		t.Fatalf("GET %s: %d %s", url, res.StatusCode, body)
	}
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStatus(t *testing.T) {
	srv := setup(t)
	var st statusResponse
	get(t, srv.URL+"/v1/status", 200, &st)
	if st.ChainID != 84532 || st.IndexedBlock != 3 || st.FinalizedBlock != 2 || st.SafeBlock != 3 || st.LagBlocks != 0 {
		t.Fatalf("status %+v", st)
	}
}

func TestTransfersArePagedNewestFirst(t *testing.T) {
	srv := setup(t)
	var page transfersResponse
	get(t, srv.URL+"/v1/addresses/"+alice.Hex()+"/transfers?limit=2", 200, &page)
	if len(page.Transfers) != 2 || page.Next == nil {
		t.Fatalf("page 1: %+v", page)
	}
	first := page.Transfers[0]
	if first.BlockNumber != 3 || first.Direction != "in" || first.Amount.USD != "0.000003" || first.Status != "safe" || first.Confirmed {
		t.Fatalf("newest transfer %+v", first)
	}
	if page.Transfers[1].Direction != "self" {
		t.Fatalf("second transfer %+v", page.Transfers[1])
	}

	get(t, srv.URL+"/v1/addresses/"+alice.Hex()+"/transfers?limit=2&before="+*page.Next, 200, &page)
	if len(page.Transfers) != 2 || page.Transfers[0].Direction != "out" || page.Transfers[1].Amount.USD != "2.5" || page.Next == nil {
		t.Fatalf("page 2: %+v", page)
	}
	get(t, srv.URL+"/v1/addresses/"+alice.Hex()+"/transfers?limit=2&before="+*page.Next, 200, &page)
	if len(page.Transfers) != 0 || page.Next != nil {
		t.Fatalf("page 3: %+v", page)
	}
}

func TestAddressSummaryCountsFinalizedOnly(t *testing.T) {
	srv := setup(t)
	var a addressResponse
	get(t, srv.URL+"/v1/addresses/"+alice.Hex(), 200, &a)
	// Blocks 1-2 are final: received 2.5 + 0.000007 (self), sent 1 + 0.000007 (self)
	if a.Finalized.Count != 3 || a.Finalized.Received.Raw != "2500007" || a.Finalized.Sent.Raw != "1000007" {
		t.Fatalf("summary %+v", a.Finalized)
	}
	if a.IndexedBalance != nil || a.LastReconciliation != nil {
		t.Fatal("no balance before the first reconciliation")
	}
}

func TestRejectsBadRequests(t *testing.T) {
	srv := setup(t)
	get(t, srv.URL+"/v1/addresses/not-an-address", 400, nil)
	get(t, srv.URL+"/v1/addresses/"+carol.Hex(), 404, nil) // not watched
	get(t, srv.URL+"/v1/addresses/"+alice.Hex()+"/transfers?limit=1000", 400, nil)
	get(t, srv.URL+"/v1/addresses/"+alice.Hex()+"/transfers?before=yesterday", 400, nil)
	get(t, srv.URL+"/healthz", 200, nil)
}
