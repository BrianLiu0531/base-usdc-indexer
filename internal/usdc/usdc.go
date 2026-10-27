// Package usdc holds the per-network USDC contract addresses and amount helpers.
package usdc

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Decimals is the number of decimal places USDC uses on every chain.
const Decimals = 6

// TransferTopic is keccak256("Transfer(address,address,uint256)"), the ERC-20 Transfer event signature.
var TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// Network describes a chain the indexer can follow.
type Network struct {
	Name    string
	ChainID uint64
	USDC    common.Address
}

// Networks lists the supported chains. Addresses are Circle's native USDC deployments.
var Networks = map[string]Network{
	"base":         {Name: "base", ChainID: 8453, USDC: common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")},
	"base-sepolia": {Name: "base-sepolia", ChainID: 84532, USDC: common.HexToAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")},
}

// Lookup returns the network with the given name.
func Lookup(name string) (Network, error) {
	n, ok := Networks[name]
	if !ok {
		names := make([]string, 0, len(Networks))
		for k := range Networks {
			names = append(names, k)
		}
		sort.Strings(names)
		return Network{}, fmt.Errorf("unknown network %q (supported: %s)", name, strings.Join(names, ", "))
	}
	return n, nil
}

// FormatAmount renders a raw token amount (6 decimals) as a decimal string, e.g. 1234500 -> "1.2345".
func FormatAmount(v *big.Int) string {
	if v == nil {
		return "0"
	}
	neg := v.Sign() < 0
	s := new(big.Int).Abs(v).String()
	if len(s) <= Decimals {
		s = strings.Repeat("0", Decimals-len(s)+1) + s
	}
	whole, frac := s[:len(s)-Decimals], strings.TrimRight(s[len(s)-Decimals:], "0")
	out := whole
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}
