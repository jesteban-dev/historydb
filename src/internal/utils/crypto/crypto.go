package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func CheckDataSignature(hash, content []byte) bool {
	contentHash := sha256.Sum256(content)
	return hmac.Equal(hash, contentHash[:])
}

func CompareHashes(hash1, hash2 string) bool {
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
