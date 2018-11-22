package hydro

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// OffsetReader defines a behavior needed for decoding, that is reading at a specific offset
// For example, files implement this interface via their ReadAt method.
type OffsetReader interface {
	// ReadAt reads from a source starting from offset into dest.
	// To restrict the number of bytes read into dest, make dest a certain length.
	// It returns the number of bytes read or an error. If an error is returned,
	// you cannot rely on the data read (or not read) into dest.
	ReadAt(dest []byte, offset int64) (int, error)
}

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

// Indexer
type Indexer interface {
}
