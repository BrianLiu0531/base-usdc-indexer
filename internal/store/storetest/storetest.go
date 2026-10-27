// Package storetest opens an isolated store for each test, in its own PostgreSQL schema.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
)

// DefaultURL matches the test database created by docker compose (see docker/init.sql).
const DefaultURL = "postgres://postgres:postgres@localhost:5440/indexer_test?sslmode=disable"

// Open returns a migrated store in a fresh schema that is dropped when the test ends.
// Set TEST_DATABASE_URL to point at another server; the test is skipped if none is reachable.
func Open(t testing.TB, chainID uint64) *store.Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = DefaultURL
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("connect to test database: %v", err)
		}
		t.Skipf("no test database (%v); run `docker compose up -d db`", err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "t_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})

	s, err := store.Open(ctx, url, chainID, store.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}
