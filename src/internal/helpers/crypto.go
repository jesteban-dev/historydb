package helpers

import (
	"crypto/hmac"
	"encoding/hex"
)

func CompareHashes(hash1 string, hash2 string) bool {
	h1, err := hex.DecodeString(hash1)
	if err != nil {
		return false
	}

	h2, err := hex.DecodeString(hash2)
	if err != nil {
		return false
	}

	return hmac.Equal(h1, h2)
}
