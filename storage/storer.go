package storage

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// Storer is an interface that a storage engine implements.
// A storage engine can be as simple as a single file, or more complex,
// like a series of segmented files with background compaction.
// Additionally, there is nothing stipulating that it needs to be a file,
// it could just as easily be a network connection or some other type that implements this interface.
type Storer interface {
	// ReadAt defines behavior for a random access reader that reads bytes from the underlying storage at segment starting at offset.
	// Since the storage entry should be length aware, ReadAt does not need to be given the number of bytes to read,
	// it will get this information when it decodes the storage entry. It returns the entry and the number of bytes read, or an error if there was one.
	ReadAt(segment int, offset int64) (*pb.Entry, int, error)
	// Begin returns a forward iterator for iterating over storage entries, starting at the beginning of the storage log.
	Begin() ForwardIterator
	// Append writes data to the tail of the underlying storage, returning the segment and offset where the data starts.
	// If it can't perform the append operation it returns an error.
	Append(e *pb.Entry) (int, int64, error)
	// Close should clean up any system resources associated with the backing storer, typcially called before exit/shutdown.
	io.Closer
}

// ForwardIterator defines behavior for iterating forward over a store's entries. ForwardIterators are not safe for concurrent use.
// A ForwardIterator can be used to restore an index.
type ForwardIterator interface {
	// Next returns the next entry, its segment, and its starting offset or an error if there was one.
	// io.EOF error is returned once the end of the storage is reached or if an out of bounds segment and offset were provided as a starting point.
	Next() (*pb.Entry, int, int64, error)
	// Done releases any locks that may have been preventing compaction and merge processes while the iterator is being held.
	// Done must be called after finsihing with a ForwardIterator.
	Done()
}
