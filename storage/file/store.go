// Package file implements a simple file storage layer for an index to operate on.
// The Store object implements the store.Storer interface, which is used by higher level index constructs.
// Note: in log file an entry is represented as -> <size><data>
// where size is a uint32 encoded in little endian to 4 bytes, storing the length in bytes of data, and data is a pb.Entry marshaled to bytes
// A log file looks like: <size><data><size><data>...<size><data><size><data>
package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
	"github.com/ryansann/hydro/storage"
	"github.com/sirupsen/logrus"
)

// StoreOption is func that modifies the store's configuration options.
type StoreOption func(*options)

type options struct {
	sync time.Duration
}

// SyncInterval overrides the Index default sync interval.
func SyncInterval(dur time.Duration) StoreOption {
	return func(opts *options) {
		opts.sync = dur
	}
}

// Store provides operations for persisting to a file and reading data back.
// It implements the storage.Storer interface.
type Store struct {
	log       *logrus.Logger
	mtx       sync.Mutex
	file      *os.File
	curOffset int64
	sync      time.Duration
	done      chan struct{}
}

// NewStore returns a new Store object or an error.
// It accepts a file and options for overriding default behavior.
func NewStore(log *logrus.Logger, file string, opts ...StoreOption) (*Store, error) {
	// default configuration
	cfg := &options{
		sync: time.Second * 15,
	}

	// override defaults
	for _, opt := range opts {
		opt(cfg)
	}

	filename, err := filepath.Abs(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get absolute path for file: %s", file)
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "could not create/open file")
	}

	s := &Store{
		log:  log,
		file: f,
		sync: cfg.sync,
		done: make(chan struct{}),
	}

	go s.syncLoop()

	return s, nil
}

// ReadAt reads and decodes the entry at segment, offset. In this case since we are using a single
// file, the segment number should be 0. It returns the entry or an error if it could not read the entry at segment and offset.
func (s *Store) ReadAt(segment int, offset int64) (*pb.Entry, int, error) {
	return pb.Decode(s.file, offset)
}

// Begin returns an iterator to the beginning of the storage log.
func (s *Store) Begin() storage.ForwardIterator {
	return &iterator{s: s}
}

// Append appends data to the file and returns the segment and staring offset, otherwise it returns an error.
// The segment number returned is always 0 since this storer uses a single file.
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

	start := s.curOffset

	s.curOffset += int64(n)

	return 0, start, nil
}

// Close calls the underlying files Close method and stops the sync process.
func (s *Store) Close() error {
	defer func() {
		_ = s.file.Close()
		close(s.done)
	}()

	s.done <- struct{}{}

	return nil
}

// syncLoop syncs the file contents to disk every s.sync interval,
// it exits when it receives a signal on the s.done channel.
func (s *Store) syncLoop() {
	for {
		select {
		case <-time.After(s.sync):
			s.mtx.Lock()
			err := s.file.Sync()
			s.mtx.Unlock()
			if err != nil {
				// TODO: change to log
				fmt.Println(err)
			}
		case <-s.done:
			return
		}
	}
}

// Cleanup is a utility for testing that closes and removes the file.
// NOTE: This should be unexported in the future.
func (s *Store) Cleanup() error {
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
