// Package file implements a simple file storage layer for an index to operate on.
// The Store object implements the store.Storer interface, which is used by higher level index constructs.
// Note: in log file an entry is represented as -> <size><data>
// where size is a uint32 encoded in little endian to 4 bytes, storing the length in bytes of data, and data is a pb.Entry marshaled to bytes
// A log file looks like: <size><data><size><data>...<size><data><size><data>
package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
	"github.com/ryansann/hydro/storage"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
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
	log *logrus.Logger

	// nextPos is the next position for an entry
	nextPos *atomic.Int64
	// lastOffset points the last physical byte offset of file
	lastOffset *atomic.Int64
	// sync determines how often file is synced to persistent storage
	sync time.Duration

	// fmtx guards file
	fmtx sync.Mutex
	file *os.File

	// pmtx guards pageTable
	pmtx sync.RWMutex
	// pageTable maps virtual entry positions to physical offsets in file
	pageTable map[int64]int64

	done chan struct{}
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
		log:        log,
		nextPos:    atomic.NewInt64(0),
		lastOffset: atomic.NewInt64(0),
		sync:       cfg.sync,
		file:       f,
		pageTable:  make(map[int64]int64, 0),
		done:       make(chan struct{}),
	}

	err = s.restorePageTable()
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize page table")
	}

	go s.syncLoop()

	return s, nil
}

// restorePageTable iterates over entries to rebuild the store's pageTable.
func (s *Store) restorePageTable() error {
	// lock page table during restore process
	s.pmtx.Lock()
	defer s.pmtx.Unlock()

	it := &iterator{s: s}

	var iterr error
	var done bool

	for {
		e, offset, nextOff, err := it.next()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}

			iterr = err
			break
		}

		s.pageTable[e.Position] = offset

		s.nextPos.Store(e.Position + 1)
		s.lastOffset.Store(nextOff)
	}

	if !done {
		return iterr
	}

	s.log.Tracef("restored page table: %v", s.pageTable)

	return nil
}

// ReadAt reads and decodes the entry at position. It returns the entry or an error.
func (s *Store) ReadAt(position int64) (*pb.Entry, error) {
	// look up physical address for position
	s.pmtx.RLock()
	offset, ok := s.pageTable[position]
	s.pmtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no entry found at position: %v", position)
	}

	s.log.Debugf("position %v has physical offset %v", position, offset)

	// decode using physical address
	e, _, err := pb.Decode(s.file, offset)
	if err != nil {
		return nil, err
	}

	return e, nil
}

// readAt reads and decodes the entry at physical offset in the store's file.
// It returns the entry or an error if it could not read the entry at offset.
func (s *Store) readAt(offset int64) (*pb.Entry, int, error) {
	return pb.Decode(s.file, offset)
}

// Begin returns an iterator to the beginning of the storage log.
func (s *Store) Begin() storage.ForwardIterator {
	return &iterator{s: s}
}

// Append writes to the tail of the storage file and returns the written entry's position or an error.
func (s *Store) Append(e *pb.Entry) (int64, error) {
	position := s.nextPos.Load()
	s.log.Debugf("appending entry at position: %v", position)

	e.Position = position
	np := s.nextPos.Add(1) // atomic increment on position counter

	s.log.Debugf("next position: %v", np)

	// encode our entry and its size into bytes
	bytes, err := pb.Encode(e)
	if err != nil {
		return 0, err
	}

	// write the encoded entry to the file, only one write at a time
	s.fmtx.Lock()
	n, err := s.file.Write(bytes)
	s.fmtx.Unlock()
	if err != nil {
		return 0, err
	}

	// update page table
	s.pmtx.Lock()
	s.pageTable[e.Position] = s.lastOffset.Load()
	s.pmtx.Unlock()

	// update last offset with number of bytes written
	_ = s.lastOffset.Add(int64(n))

	// return virtual address
	return e.Position, nil
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
// NOTE: this isn't strictly necessary, see: https://stackoverflow.com/questions/10862375/when-to-flush-a-file-in-go
func (s *Store) syncLoop() {
	for {
		select {
		case <-time.After(s.sync):
			s.fmtx.Lock()
			err := s.file.Sync()
			s.fmtx.Unlock()
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
