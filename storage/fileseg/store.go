// Package fileseg implements segmented file based storage with a compaction process.
package fileseg

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
)

// StoreOption is func that modifies the store's configuration options.
type StoreOption func(*options)

type options struct {
	sync        time.Duration
	segmentSize int64
	compaction  bool
}

// SyncInterval overrides the Index default sync interval.
func SyncInterval(dur time.Duration) StoreOption {
	return func(opts *options) {
		opts.sync = dur
	}
}

// SegmentSize sets the max size for a given storage segment.
func SegmentSize(n int64) StoreOption {
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

// segment represents an individual storage segment that stores bytes in a file.
type segment struct {
	index     int
	curOffset int64
	// mtx guards file
	mtx  *sync.Mutex
	file *os.File
}

// Store provides operations for persisting to a data directory where storage segments are written as files.
// It implements the storage.Storer interface.
type Store struct {
	segmentSize int64
	cur         *segment
	prev        []segment
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
		segmentSize: cfg.segmentSize,
	}

	err = s.initSegments(path)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize storage segments")
	}

	return s, nil
}

// initSegments initializes the storage segments from directory at path.
// If there are no files in the directory, it creates the initial segment.
// It returns an error if there was one.
func (s *Store) initSegments(path string) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	// if there are no existing segments, create the first and return
	if len(files) == 0 {
		seg, err := newSegment(strings.Join([]string{path, uuid.New().String()}, "/"), 0)
		if err != nil {
			return err
		}

		s.cur = seg

		return nil
	}

	// if there are existing segments we need to initialize data structures
	segments := make([]segment, len(files))
	for _, file := range files {
		fpath := strings.Join([]string{path, file.Name()}, "/")

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

	s.prev = segments[:len(segments)-1]
	s.cur = &segments[len(segments)-1]

	return nil
}

// newSegment creates a new file with name, writes the segment info and returns the segment or an error.
func newSegment(path string, index int) (*segment, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "could not create/open file")
	}

	ibytes, err := pb.EncodeInfo(&pb.SegmentInfo{Index: int64(index)})
	if err != nil {
		return nil, err
	}

	n, err := f.Write(ibytes)
	if err != nil {
		return nil, errors.Wrapf(err, "could not write segment info to file: %s", path)
	}

	return &segment{
		index:     index,
		curOffset: int64(n),
		mtx:       &sync.Mutex{},
		file:      f,
	}, nil
}

// initSegment reads an existing file and creates its corresponding segment object.
// It returns an error if it cannot create the segment.
func initSegment(f *os.File) (*segment, error) {
	info, _, err := pb.DecodeInfo(f, 0)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "could not get file info")
	}

	return &segment{
		index:     int(info.GetIndex()),
		curOffset: fi.Size() - 1, // TODO: is this correct?
		mtx:       &sync.Mutex{},
		file:      f,
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
