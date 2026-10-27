package usdc

import (
	"math/big"
	"testing"
)

func TestFormatAmount(t *testing.T) {
	for raw, want := range map[int64]string{
		0:               "0",
		1:               "0.000001",
		1_000_000:       "1",
		1_234_500:       "1.2345",
		-2_500_000:      "-2.5",
		123_456_789_012: "123456.789012",
	} {
		if got := FormatAmount(big.NewInt(raw)); got != want {
			t.Errorf("FormatAmount(%d) = %q, want %q", raw, got, want)
		}
	}
}

func TestTransferTopic(t *testing.T) {
	if TransferTopic.Hex() != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
		t.Fatal(TransferTopic.Hex())
	}
}
