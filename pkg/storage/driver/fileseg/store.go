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
	"io"
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
	// segments stores references to segments
	segments []*segment
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

		// initialize segments list
		s.segments = append(s.segments, seg)

		return nil
	}

	return s.restore(files)
}

// restore restores segments and the store's page table from existing files in the storage directory.
func (s *Store) restore(files []os.FileInfo) error {
	var nextPos int64
	segments := make([]*segment, len(files))

	// if there are existing segment files we need to initialize segments and restore the store's page table
	for _, file := range files {
		fpath := s.getFilePath(file.Name())

		// initialize segment from data file
		seg, positions, err := initSegment(fpath)
		if err != nil {
			return errors.Wrapf(err, "could not initialize segment from: %s", fpath)
		}

		s.log.Debugf("initialized segement: %+v", *seg)

		// set the segment
		segments[seg.index] = seg

		// initialize page table, track next position
		for _, pos := range positions {
			// position -> segment index
			s.pageTable[pos] = seg.index

			if pos > nextPos {
				nextPos = pos
			}
		}
	}

	// set the segments list to the initialized segments list
	s.segments = segments

	// update nextPos to the maximum position seen + 1
	s.nextPos.Store(nextPos + 1)

	s.log.Tracef("restored page table: %v", s.pageTable)

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

	// look up the entry by its position in its physical segment
	return s.readAt(segment, position)
}

// readAt takes a physical segment and virtual position returns the entry from segment that has that position, or an error.
func (s *Store) readAt(segment int, position int64) (*pb.Entry, error) {
	// make sure the segment is valid
	if segment > len(s.segments) {
		return nil, fmt.Errorf("segment %v does not exist", segment)
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
	position := s.nextPos.Load()
	s.log.Debugf("appending entry at position: %v", position)

	e.Position = position
	np := s.nextPos.Add(1) // increment position counter

	s.log.Debugf("next position: %v", np)

	// segment index for most recent segment, i.e. segment to append to
	index := len(s.segments) - 1

	// append to segment, create a new segment and append there if current segment is full
	err := s.segments[index].append(e)
	if err != nil {
		if err == errSegmentFull {
			return s.appendNewSegment(e)
		}

		// unsuccessful append
		return 0, err
	}

	// update page table, no new segment created
	s.pmtx.Lock()
	defer s.pmtx.Unlock()
	s.pageTable[e.Position] = index

	return e.Position, nil
}

// appendNewSegment creates a new segment and append entry to it, returning the position or an error.
func (s *Store) appendNewSegment(e *pb.Entry) (int64, error) {
	seg, err := newSegment(s.getNewFilePath(), len(s.segments), s.segmentSize)
	if err != nil {
		return 0, err
	}

	s.log.Debugf("segment %v full, new segment created", len(s.segments)-1)

	// append the new segment to segment list
	s.smtx.Lock()
	s.segments = append(s.segments, seg)
	s.smtx.Unlock()

	// run compaction on old segments when a new one is created
	go s.compact()

	err = seg.append(e)
	if err != nil {
		return 0, err
	}

	// update page table
	s.pmtx.Lock()
	defer s.pmtx.Unlock()
	s.pageTable[e.Position] = seg.index

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
// Compaction removes entries that are no longer needed for maintaining an index, and thus reduces storage space.
// Compaction writes to new segment files, removing previous ones after completed.
// A segment with entries:
// {write, hello, world, 0}{delete, hello, 1}{write, hello, worldagain, 2}
// can be compacted to:
// {write, hello, worldagain, 2}
// NOTE: this is a relatively naive compaction algorithm.
// - it is triggered every time a new segment is created due to its capacity being reached
// - it compacts all previous segments, which isn't necessary. Can be improved by compacting only the most recent segment whose capacity was reached.
// - it does not compact across segments
// - it does not merge segments and reindex
func (s *Store) compact() {
	// if there's one or zero segments, there's nothing to compact
	if len(s.segments) <= 1 {
		return
	}

	s.log.Debug("starting compaction process")

	// iterate over every segment but the last
	for i := 0; i < len(s.segments)-1; i++ {
		seg := s.segments[i]

		s.log.Debugf("compacting segment: %v", i)

		// create a new segment
		compactedSeg, err := newSegment(s.getNewFilePath(), i, seg.capacity)
		if err != nil {
			s.log.Errorf("could not create new segment during compaction process: %v", err)
			return
		}

		it := &segmentIterator{seg, seg.startOffset.Load()}

		var iterr error
		var done bool
		entries := make(map[string]*pb.Entry, 0)

		for {
			e, _, _, err := it.next()
			if err != nil {
				if err == io.EOF { // finished iterating over segment file
					done = true
					break
				}

				iterr = err
				break
			}

			switch e.GetType() {
			case pb.EntryType_WRITE:
				entries[e.GetKey()] = e
			case pb.EntryType_DELETE:
				delete(entries, e.GetKey())
			}
		}

		if !done {
			s.log.Errorf("error iterating over segment during compaction: %v, %v", i, iterr)
			return
		}

		s.log.Debugf("finished iterating over segment: %v", i)

		// serialize compacted entries to new segment
		for _, e := range entries {
			err := compactedSeg.append(e)
			if err != nil {
				s.log.Errorf("could not append entry with position: %v to compacted segment with index: %v", e.GetPosition(), seg.index)
				return
			}
		}

		// overwrite existing segment
		s.smtx.Lock()
		s.segments[seg.index] = compactedSeg
		s.smtx.Unlock()

		// log how many bytes we saved through compaction
		sinfo, serr := seg.file.Stat()
		cinfo, cerr := compactedSeg.file.Stat()
		if cerr == nil && serr == nil {
			s.log.Infof("compaction removed %v bytes for segment: %v", sinfo.Size()-cinfo.Size(), i)
		}

		// cleanup old segment
		err = seg.remove()
		if err != nil {
			s.log.Errorf("could not remove old segment file: %v", err)
		}

		s.log.Infof("segment %v successfully compacted", i)
	}
}

// getFilePath returns the full path of the file with name.
func (s *Store) getFilePath(name string) string {
	return strings.Join([]string{s.dirPath, name}, "/")
}

// getNewFilePath returns the full path for a new data file.
func (s *Store) getNewFilePath() string {
	return s.dirPath + "/" + uuid.New().String() + ".data"
}
