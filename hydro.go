package hydro

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// Store is the interface that a storage engine implements.
// A storage engine can be as simple as a single file, or more complex,
// like a series of segmented files with background compaction.
// Additionally, there is nothing stipulating that it needs to be a file,
// it could just as easily be a network connection or some other type that implements this interface.
type Store interface {
	// ReadAt is a random access reader that reads bytes from the underlying storage on page starting at offset.
	// Since the storage entry should be length aware, ReadAt does not need to be told
	// how many bytes to read, it will get this information when it decodes the storage entry.
	// It returns the entry and the number of bytes read or an error.
	ReadAt(page int, offset int64) (*pb.Entry, int, error)
	// Write writes data to the underlying storage, returning the page and offset where the data starts.
	// If it can't perform the write operation it returns an error.
	Write(data []byte) (int, int64, error)
	// Close is to cleanup any storage resources
	io.Closer
}

// Indexer is the interface that indexes should implement. An index interacts with a store
// in order to perform CRUD operations. This interface provides CRUD operations through
// the Get, Set, Delete methods. An index also needs to be able to restore any necessary
// inmemory components after a crash, so Restore method must also be implemented by an Index.
type Indexer interface {
	Get(key string) (string, error)
	Set(key, val string) error
	Del(key string) error
	Restore() error
}
