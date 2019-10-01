package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ryansann/hydro/pb"
)

// Store provides operations for persisting to a file and reading data back.
// It implements the storage.Storer interface.
type Store struct {
	mtx       sync.Mutex
	file      *os.File
	curoffset int64
}

// NewStore returns a new Store object. It accepts OptionFuncs for overriding
// default values for the sync interval and the path of the storage file.
// It returns a Store object or an error.
func NewStore(file string) (*Store, error) {
	filename, err := filepath.Abs(file)
	if err != nil {
		return nil, fmt.Errorf("error: could not get absolute path for file: %s, %v", file, err)
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error: could not create/open file: %v", err)
	}

	return &Store{
		file: f,
	}, nil
}

// ReadAt reads and decodes the entry at page, offset. In this case since we are using a single
// file, the page number should be 0. It returns the entry or an error if it could not read the entry at page and offset.
func (s *Store) ReadAt(page int, offset int64) (*pb.Entry, error) {
	e, _, err := pb.Decode(s.file, offset)
	if err != nil {
		return nil, err
	}

	return e, nil
}

// Scan decodes and returns the entry at page, offset along with the next page and next offset to scan from.
// If it is unsuccessful in decoding the entry it returns an error.
func (s *Store) Scan(page int, offset int64) (*pb.Entry, int, int64, error) {
	e, n, err := pb.Decode(s.file, offset)
	if err != nil {
		return nil, 0, 0, err
	}

	// page is always 0, next offset is offset + # bytes decoded
	return e, 0, offset + n, nil
}

// Append appends data to the file and returns the page and staring offset, otherwise it returns an error.
// The page number returned is always 0 since this storer uses a single file.
func (s *Store) Append(e *pb.Entry) (int, int64, error) {
	// encode our entry and its size into bytes
	bytes, err := pb.Encode(e)
	if err != nil {
		return 0, 0, err
	}

	// only allow one write operation at a time
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// write the encoded entry to the file
	n, err := s.file.Write(bytes)
	if err != nil {
		return 0, 0, err
	}

	start := s.curoffset

	s.curoffset += int64(n)

	return 0, start, nil
}

// Close is a wrapper on the underlying file's Close method.
func (s *Store) Close() error {
	return s.file.Close()
}

// Sync is a wrapper on the underlying file's Sync method.
func (s *Store) Sync() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.file.Sync()
}

// cleanup is a utility for testing that closes and removes the file.
func (s *Store) cleanup() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.file.Name())
	if err != nil {
		return err
	}

	return nil
}
