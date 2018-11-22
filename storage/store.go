package storage

import "io"

// Store is the interface that a storage engine implements.
// A storage engine can be as simple as a single file, or more complex,
// like a series of segmented files with background compaction.
// Additionally, there is nothing stipulating that it needs to be a file,
// it could just as easily be a network connection or some other type that implements this interface.
type Store interface {
	// ReadAt is a random access reader that reads bytes from the underlying storage on page starting at offset.
	// Since the storage entry should be length aware, ReadAt does not need to be told
	// how many bytes to read, it will get this information when it decodes the storage entry.
	ReadAt(page int, offset int64) (int, error)
	// Write writes data to the underlying storage
	Write(data []byte) (int, int64, error)
	io.Closer
}
