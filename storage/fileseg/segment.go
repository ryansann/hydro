package fileseg

import (
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
)

// segment represents an individual storage segment that stores bytes in a file.
type segment struct {
	index       int
	capacity    int   // capacity is the number of bytes a segment can hold, a given segment will not exceed capacity.
	startOffset int64 // startOffset is the offset where entry records start, e.g. after segment info
	lastOffset  int64
	// mtx guards file
	mtx  *sync.Mutex
	file *os.File
}

// newSegment creates a new file with name, writes the segment info and returns the segment or an error.
func newSegment(path string, index int, capacity int) (*segment, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "could not create/open file")
	}

	info := &pb.SegmentInfo{Index: int64(index), Capacity: int64(capacity)}

	ibytes, err := pb.EncodeInfo(info)
	if err != nil {
		return nil, err
	}

	n, err := f.Write(ibytes)
	if err != nil {
		return nil, errors.Wrapf(err, "could not write segment info to file: %s", path)
	}

	return &segment{
		index:       index,
		capacity:    capacity,
		startOffset: int64(n),
		lastOffset:  int64(n),
		mtx:         &sync.Mutex{},
		file:        f,
	}, nil
}

// initSegment reads an existing file and creates its corresponding segment object.
// It returns an error if it cannot create the segment.
func initSegment(f *os.File) (*segment, error) {
	info, n, err := pb.DecodeInfo(f, 0)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "could not get file info")
	}

	return &segment{
		index:       int(info.Index),
		capacity:    int(info.Capacity),
		startOffset: n,
		lastOffset:  fi.Size() - 1,
		mtx:         &sync.Mutex{},
		file:        f,
	}, nil
}

// readAt reads the entry in segment starting at offset, it returns an error if there was one.
func (s *segment) readAt(offset int64) (*pb.Entry, int, error) {
	e, n, err := pb.Decode(s.file, offset)
	if err != nil {
		return nil, 0, err
	}

	return e, n, nil
}

var (
	errSegmentFull = errors.New("segment full")
)

// append appends an entry to the segment and returns the segment number and starting offset, otherwise it returns an error.
func (s *segment) append(e *pb.Entry) (int, int64, error) {
	// encode our entry and its size into bytes
	bytes, err := pb.Encode(e)
	if err != nil {
		return 0, 0, err
	}

	if (len(bytes) + int(s.lastOffset)) > s.capacity {
		return 0, 0, errSegmentFull
	}

	// lock file when writing
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// write the encoded entry to the file
	n, err := s.file.Write(bytes)
	if err != nil {
		return 0, 0, err
	}

	start := s.lastOffset

	s.lastOffset += int64(n)

	return s.index, start, nil
}
