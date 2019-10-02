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
	ReadAt(segment int, offset int64) (*pb.Entry, error)
	// Scan reads the entry at segment and offset and returns the entry, the next segment, and the next offset. It returns an error if it can't
	// read the entry or there's a problem with the underlying storage. An io.EOF is returned when there is nothing left to scan.
	Scan(segment int, offset int64) (*pb.Entry, int, int64, error)
	// Append writes data to the tail of the underlying storage, returning the segment and offset where the data starts.
	// If it can't perform the append operation it returns an error.
	Append(e *pb.Entry) (int, int64, error)
	// Close should clean up any system resources associated with the backing storer, typcially called before exit/shutdown.
	io.Closer
}
