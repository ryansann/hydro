package index

import (
	"crypto/sha1"
	"encoding/hex"
)

// KeyHashFunc is a hash func that takes a slice of bytes and returns a hash or an error
type KeyHashFunc func(key string) (string, error)

// DefaultHash is a HashFunc that uses the sha1 hashing algorithm
func DefaultHash(key string) (string, error) {
	h := sha1.New()
	_, err := h.Write([]byte(key))
	if err != nil {
		return "", err
	}
	str := hex.EncodeToString(h.Sum(nil))
	return str, nil
}

// NoHash is a HashFunc that returns the key as a string without hashing.
func NoHash(key string) (string, error) {
	return key, nil
}
