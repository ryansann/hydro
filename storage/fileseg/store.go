// Package fileseg implements segmented file based storage with a compaction process.
package fileseg

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
)

const (
	storageDir = "./data"
)

// segment represents an individual storage segment that stores bytes in a file.
type segment struct {
	index     int
	maxSize   int64
	curOffset int64
	// mtx guards file
	mtx  sync.Mutex
	file *os.File
}

// Store provides operations for persisting to a data directory where storage segments are written as files.
// It implements the storage.Storer interface.
type Store struct {
	dir      *os.File
	segments []segment
}

// NewStore returns a new Store object. It accepts OptionFuncs for overriding
// default values for the sync interval and the path of the storage file.
// It returns a Store object or an error.
func NewStore(dir string) (*Store, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get absolute path for dir: %s", dir)
	}

	return &Store{
		segments: make([]segment, 0),
	}, nil
}

// ReadAt ...
func (s *Store) ReadAt(segment int, offset int64) (*pb.Entry, error) {
	return nil, nil
}

// Scan ...
func (s *Store) Scan(segment int, offset int64) (*pb.Entry, int, int64, error) {
	return nil, 0, 0, nil
}

// Append ...
func (s *Store) Append(e *pb.Entry) (int, int64, error) {
	return 0, 0, nil
}

// Close ...
func (s *Store) Close() error {
	return nil
}
