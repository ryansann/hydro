package storage

import "io"

// Package storage provides an interface into storing data on disk.
// Implement a storage engine by creating a type that satisfies the Storer interface.

// Storer is the interface that a storage engine implements
type Storer interface {
	Write(key []byte, value []byte) error
	Read(key []byte) ([]byte, error)
	Delete(key []byte) error
	io.Closer
}
