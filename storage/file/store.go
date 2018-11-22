package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ryansann/hydro/pb"
)

type options struct {
	sync time.Duration
	file string
}

// OptionFunc is func that modifies the server's configuration options
type OptionFunc func(*options)

// SyncInterval overrides the Index default sync interval
func SyncInterval(dur time.Duration) OptionFunc {
	return func(opts *options) {
		opts.sync = dur
	}
}

// StorageFile overrides the Index default storage file
func StorageFile(path string) OptionFunc {
	return func(opts *options) {
		opts.file = path
	}
}

// Store provides operations for persisting to a file and reading data back
type Store struct {
	mtx       sync.RWMutex
	sync      time.Duration
	file      *os.File
	curoffset int64
	stop      chan struct{}
	done      chan struct{}
}

// NewStore returns a new Store object. It accepts OptionFuncs for overriding
// default values for the sync interval and the path of the storage file. It returns
// a Store object or an error.
func NewStore(opts ...OptionFunc) (*Store, error) {
	cfg := &options{
		sync: time.Second * 30,
		file: "./data",
	}

	for _, opt := range opts {
		opt(cfg)
	}

	filename, err := filepath.Abs(cfg.file)
	if err != nil {
		return nil, fmt.Errorf("error: could not get absolute path for file: %s, %v", cfg.file, err)
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error: could not create/open file: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})

	go syncloop(cfg.sync, f, stop, done)

	return &Store{
		mtx:       sync.RWMutex{},
		sync:      cfg.sync,
		file:      f,
		curoffset: 0,
		stop:      stop,
		done:      done,
	}, nil
}

// ReadAt reads and decodes the entry at offset, in this case since we are using a single
// file, the page number should be 0. It returns the entry and the number of bytes decoded or an error.
func (s *Store) ReadAt(page int, offset int64) (*pb.Entry, int, error) {
	e, n, err := pb.Decode(s.file, offset)
	if err != nil {
		return nil, 0, err
	}

	return e, n, nil
}

// Write writes data to the store and returns the page and staring offset or an error.
func (s *Store) Write(e *pb.Entry) (int, int64, error) {
	// encode our entry and its size into bytes
	bytes, err := pb.Encode(e)
	if err != nil {
		return 0, 0, err
	}

	// write the encoded entry to the file
	n, err := s.file.Write(bytes)
	if err != nil {
		return 0, 0, err
	}

	start := s.curoffset

	s.curoffset += int64(n)

	return 0, start, nil
}

// Close stops the sync loop and closes the file resource. It blocks until this completes.
func (s *Store) Close() error {
	// signal sync loop to stop
	close(s.stop)

	// wait for sync loop to exit
	<-s.done

	// close the file
	err := s.file.Close()
	if err != nil {
		return err
	}

	return nil
}

// syncloop is intended to be run as a background go routine that flushes data to disk every interval
func syncloop(interval time.Duration, f *os.File, stop <-chan struct{}, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-time.After(interval):
			err := f.Sync()
			if err != nil {
				fmt.Println(err)
			}
		case <-stop:
			return
		}
	}
}

// cleanup is a utility for testing that closes and removes the storage log file
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
