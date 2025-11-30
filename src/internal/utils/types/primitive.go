package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
)

type BigInt struct {
	big.Int
}

func (bi *BigInt) UnmarshalJSON(data []byte) error {
	b := bytes.TrimSpace(data)

	if len(b) > 0 && b[0] != '"' {
		s := string(b)
		if _, ok := bi.SetString(s, 10); !ok {
			return fmt.Errorf("invalid BigInt JSON number: %s", s)
		}
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("invalid BigInt JSON string: %w", err)
	}
	if _, ok := bi.SetString(s, 10); !ok {
		return fmt.Errorf("invalid BigInt JSON string: %s", s)
	}

	return nil
}
