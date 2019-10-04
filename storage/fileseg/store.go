// Package fileseg implements segmented file based storage with a compaction process.
package fileseg

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
	"github.com/ryansann/hydro/storage"
)

// StoreOption is func that modifies the store's configuration options.
type StoreOption func(*options)

type options struct {
	sync        time.Duration
	segmentSize int
	compaction  bool
}

// SyncInterval overrides the Index default sync interval.
func SyncInterval(dur time.Duration) StoreOption {
	return func(opts *options) {
		opts.sync = dur
	}
}

// SegmentSize sets the max size for a given storage segment.
func SegmentSize(n int) StoreOption {
	return func(opts *options) {
		opts.segmentSize = n
	}
}

// Compaction turns segment compaction on or off, default is on.
func Compaction(enabled bool) StoreOption {
	return func(opts *options) {
		opts.compaction = enabled
	}
}

// Store provides operations for persisting to a data directory where storage segments are written as files.
// It implements the storage.Storer interface.
type Store struct {
	dirPath     string
	segmentSize int
	segments    []segment
	// mtx should be held when iterating or compacting previous segments.
	mtx sync.Mutex
}

// NewStore returns a new Store object or an error.
// It accepts a data directory and options for overriding default behavior.
func NewStore(dir string, opts ...StoreOption) (*Store, error) {
	// default configuration
	cfg := &options{
		sync:        15 * time.Second,
		compaction:  true,
		segmentSize: 1 << 16, // 65536 bytes
	}

	// override defaults
	for _, opt := range opts {
		opt(cfg)
	}

	// get fully qualified path to dir
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get absolute path for dir: %s", dir)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) { // doesn't exist error
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create dir: %s", path)
		}
	} else if err != nil { // other error
		return nil, errors.Wrapf(err, "could not get info on dir: %s", path)
	}

	s := &Store{
		dirPath:     path,
		segmentSize: cfg.segmentSize,
	}

	err = s.initSegments()
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize storage segments")
	}

	return s, nil
}

// initSegments initializes the storage segments from directory at path.
// If there are no files in the directory, it creates the initial segment.
// It returns an error if there was one.
func (s *Store) initSegments() error {
	files, err := ioutil.ReadDir(s.dirPath)
	if err != nil {
		return err
	}

	// if there are no existing segments, create the first and return
	if len(files) == 0 {
		seg, err := newSegment(strings.Join([]string{s.dirPath, uuid.New().String()}, "/"), 0, s.segmentSize)
		if err != nil {
			return err
		}

		s.segments = append(s.segments, *seg)

		return nil
	}

	// if there are existing segments we need to initialize data structures
	segments := make([]segment, len(files))
	for _, file := range files {
		fpath := strings.Join([]string{s.dirPath, file.Name()}, "/")

		f, err := os.OpenFile(fpath, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return errors.Wrapf(err, "could not create/open file: %s", fpath)
		}

		seg, err := initSegment(f)
		if err != nil {
			return errors.Wrapf(err, "could not initialize segment from: %s", fpath)
		}

		segments[seg.index] = *seg
	}

	s.segments = segments

	return nil
}

// ReadAt reads the entry from segment starting at offset, returning an error if there was one.
func (s *Store) ReadAt(segment int, offset int64) (*pb.Entry, int, error) {
	if segment > len(s.segments) {
		return nil, 0, fmt.Errorf("segment %v does not exist", segment)
	}

	return s.segments[segment].readAt(offset)
}

// Begin returns an iterator to the beginning of the storage log, and locks the log from writes while iterating.
func (s *Store) Begin() storage.ForwardIterator {
	s.mtx.Lock()
	return &iterator{
		s: s,
	}
}

// Append appends data a segment and returns the segment and staring offset, otherwise it returns an error.
func (s *Store) Append(e *pb.Entry) (int, int64, error) {
	idx, offset, err := s.segments[len(s.segments)-1].append(e)
	if err != nil && err == errSegmentFull {
		fpath := strings.Join([]string{s.dirPath, uuid.New().String()}, "/")
		seg, err := newSegment(fpath, len(s.segments), s.segmentSize)
		if err != nil {
			return 0, 0, err
		}

		s.segments = append(s.segments, *seg)

		return seg.append(e)
	} else if err != nil {
		return 0, 0, err
	}

	return idx, offset, nil
}

// Close closes all underlying segment files and stops background processes.
func (s *Store) Close() error {
	// acquire the lock for the store, no in progress iterators or background processe during close.
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, seg := range s.segments {
		_ = seg.file.Close()
	}

	return nil
}
