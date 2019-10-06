package storage

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// Storer is an interface that a storage engine implements.
// A Storer implementation can be as simple as a single file, or more complex,
// like a series of segmented files with background compaction.
// Additionally, there is nothing stipulating that it needs to be a file,
// it could be a network connection or some other type that implements this interface.
// Storers should only expose virtual memory locations as positions.
// Not exposing physical addresses is important for being able to reorganize entries in storage via
// processes like segment compaction and merging without breaking higher level components like indexes.
type Storer interface {
	// ReadAt defines behavior for a random access reader that reads bytes from the underlying storage at position.
	// Since the storage entry is length aware, ReadAt does not need to be given the number of bytes to read,
	// it will get this information when it decodes the storage entry. It returns the entry or an error.
	ReadAt(position int64) (*pb.Entry, error)
	// Begin returns a forward iterator for iterating over storage entries, starting at the beginning of the storage log.
	Begin() ForwardIterator
	// Append writes data to the tail of the underlying storage, returning the position of the entry or an error.
	Append(e *pb.Entry) (int64, error)
	// Close should clean up any system resources associated with the backing storer, typcially called before exit/shutdown.
	io.Closer
}

// ForwardIterator defines behavior for iterating forward over a store's entries. ForwardIterators are not safe for concurrent use.
// A ForwardIterator can be used to restore an index.
type ForwardIterator interface {
	// Next returns the next entry an error if there was one.
	// io.EOF error is returned once the end of the storage is reached.
	Next() (*pb.Entry, error)
}
