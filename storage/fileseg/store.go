// Package fileseg implements segmented file based storage with a segmentation and compaction strategy.
// The Store object implements the Storer interface. It allows for compaction by virtualizing addresses through the use of hierarchical page tables.
// Hierarchical page tables comes with some storage overhead since we are maintaining multiple page tables instead of just one.
// The store maintains a page table with [<virtual-address> -> <segment-index>], with each segment maintaining its own
// page table with [<virtual-address> -> <offset>]. Each segment is backed by a file in a specified data directory.
// The Store orchestrates reads and writes to the various segments.
// The Store's page tables need to fit in memory.
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
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
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
	log *logrus.Logger

	// dirPath is where data files are stored
	dirPath string
	// nextPos is the next position for an entry
	nextPos *atomic.Int64
	// segmentSize is the size in bytes for a storage segment
	segmentSize int

	// pmtx guards pageTable
	pmtx sync.RWMutex
	// pageTable maps virtual addresses (positions) to segment indexes, part of the physical address.
	pageTable map[int64]int

	// smtx guards segments
	smtx sync.Mutex
	// segments maps segment indexes to the corresponding segment
	segments map[int]segment
}

// NewStore returns a new Store object or an error.
// It accepts a data directory and options for overriding default behavior.
func NewStore(log *logrus.Logger, dir string, opts ...StoreOption) (*Store, error) {
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

	// create the data directory if it does not exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create dir: %s", path)
		}
	} else if err != nil { // other error
		return nil, errors.Wrapf(err, "could not get info on dir: %s", path)
	}

	s := &Store{
		log:         log,
		dirPath:     path,
		nextPos:     atomic.NewInt64(0),
		segmentSize: cfg.segmentSize,
		pageTable:   make(map[int64]int, 0),
	}

	// initialize the store and its underlying segments
	err = s.init()
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize storage segments")
	}

	return s, nil
}

// int initializes the store and its segments from data inside of s.dirPath
// If there are no files in the directory, it creates the initial segment
// An error is returned if initialization can't be completed.
func (s *Store) init() error {
	// get file info from data dir
	files, err := ioutil.ReadDir(s.dirPath)
	if err != nil {
		return err
	}

	// if there are no existing segments, create the first and return
	if len(files) == 0 {
		s.log.Debug("no existing storage files found, initializing")

		// create a new segment at path with index and size
		seg, err := newSegment(s.getNewFilePath(), 0, s.segmentSize)
		if err != nil {
			return err
		}

		// initialize segments map
		s.segments[seg.index] = *seg // dereference non nil segment

		return nil
	}

	segments := make(map[int]segment, 0)

	// if there are existing segment files we need to initialize segments and restore the store's page table
	for _, file := range files {
		fpath := s.getFilePath(file.Name())

		// open data file
		f, err := os.OpenFile(fpath, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return errors.Wrapf(err, "could not create/open file: %s", fpath)
		}

		// initialize segment from data file
		seg, err := initSegment(f)
		if err != nil {
			return errors.Wrapf(err, "could not initialize segment from: %s", fpath)
		}

		s.log.Debugf("initialized segement: %+v", *seg)

		// set the segment
		segments[seg.index] = *seg // dereference non nil segment
	}

	// set the segments map to the initialized segments map
	s.segments = segments

	return nil
}

// ReadAt looks up the entries physical address in the page table.
// It returns the entry at that physical address or an error.
func (s *Store) ReadAt(position int64) (*pb.Entry, error) {
	// look up physical address for position
	s.pmtx.RLock()
	segment, ok := s.pageTable[position]
	s.pmtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no entry found at position: %v", position)
	}

	// look up the entry by its physical address
	e, _, err := s.readAt(segment, position)

	return e, err
}

// readAt takes a physical segment and virtual position returns the entry from segment that has that position, or an error.
func (s *Store) readAt(segment int, position int64) (*pb.Entry, int, error) {
	// make sure the segment is valid
	if segment > len(s.segments) {
		return nil, 0, fmt.Errorf("segment %v does not exist", segment)
	}

	// lookup the entry by its physical offset in the correct segment.
	return s.segments[segment].readAt(position)
}

// Begin returns a forward iterator to the beginning of the storage entries.
func (s *Store) Begin() storage.ForwardIterator {
	return &iterator{
		s: s,
	}
}

// Append appends data to a storeage segment and returns its position or an error.
func (s *Store) Append(e *pb.Entry) (int64, error) {
	e.Position = s.nextPos
	_ = atomic.AddInt64(&s.nextPos, 1) // increment position counter

	// append to segment, create a new segment and append there if current segment is full
	loc, err := s.segments[len(s.segments)-1].append(e)
	if err != nil && err == errSegmentFull {
		if err == errSegmentFull {
			return s.appendNewSegment(e)
		}

		// unsuccessful append
		return 0, err
	}

	// update page table, no new segment created
	s.pmtx.Lock()
	defer s.pmtx.Unlock()
	s.pageTable[e.Position] = loc

	return e.Position, nil
}

// appendNewSegment creates a new segment and append entry to it, returning the position or an error.
func (s *Store) appendNewSegment(e *pb.Entry) (int64, error) {
	seg, err := newSegment(s.getNewFilePath(), len(s.segments), s.segmentSize)
	if err != nil {
		return 0, err
	}

	s.log.Debugf("segment %v full, new segment created", len(s.segments)-1)

	s.segments = append(s.segments, *seg)

	// run compaction on old segments when a new one is created
	go s.compact()

	loc, err := seg.append(e)
	if err != nil {
		return 0, err
	}

	// update page table
	s.pmtx.Lock()
	defer s.pmtx.Unlock()
	s.pageTable[e.Position] = loc

	return e.Position, nil
}

// Close closes all underlying segment files and stops background processes.
func (s *Store) Close() error {
	// acquire the lock for the store, no in progress iterators or background processe during close.
	for _, seg := range s.segments {
		_ = seg.file.Close()
	}

	return nil
}

// compact rewrites previous segments with the entries that affect the current index state only.
// Compaction removes entries that are no longer needed for maintaining an index, and thus reduce storage space.
// Compaction writes to new segment files, removing previous ones after completed.
// For example:
// {write, hello, world}{delete, hello}{write, hello, worldagain}
// can be compacted to:
// {write, hello, worldagain}
func (s *Store) compact() {

}

// getFilePath returns the full path of the file with name.
func (s *Store) getFilePath(name string) string {
	return strings.Join([]string{s.dirPath, name}, "/")
}

// getNewFilePath returns the full path for a new data file.
func (s *Store) getNewFilePath() string {
	return s.dirPath + "/" + uuid.New().String() + ".data"
}
