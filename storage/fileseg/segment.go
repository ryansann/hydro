package fileseg

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/pb"
	"go.uber.org/atomic"
)

// segment represents an individual storage segment that stores bytes in a file.
type segment struct {
	// index is the segment's index in the store's list of segments
	index int
	// capacity is the number of bytes a segment can hold, a given segment will not exceed capacity
	capacity int

	// startOffset is the offset where entry records start, e.g. after segment info
	startOffset *atomic.Int64
	// lastOffset is the offset where append operations can be made
	lastOffset *atomic.Int64

	// pmtx guards pageTable
	pmtx *sync.RWMutex
	// pageTable maps virtual addresses to physical byte offsets in file
	pageTable map[int64]int64

	// mtx guards file
	fmtx *sync.Mutex
	// file is the segment's underlying persistence mechanism
	file *os.File
}

// newSegment creates a new file at path, writes the segment info and returns the segment object or an error.
func newSegment(path string, index int, capacity int) (*segment, error) {
	// create file
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "could not create/open file")
	}

	// create segment info object
	info := &pb.SegmentInfo{Index: int64(index), Capacity: int64(capacity)}

	// encode segment info
	ibytes, err := pb.EncodeInfo(info)
	if err != nil {
		return nil, err
	}

	// write the segment info bytes to the beginning of the segment file
	n, err := f.Write(ibytes)
	if err != nil {
		return nil, errors.Wrapf(err, "could not write segment info to file: %s", path)
	}

	// create a variable for n64 for nostalgia's sake
	n64 := int64(n)

	return &segment{
		index:       index,
		capacity:    capacity,
		startOffset: atomic.NewInt64(n64),
		lastOffset:  atomic.NewInt64(n64),
		pmtx:        &sync.RWMutex{},
		pageTable:   make(map[int64]int64, 0),
		fmtx:        &sync.Mutex{},
		file:        f,
	}, nil
}

// initSegment reads an existing file and creates the corresponding segment object.
// It returns the segment object and the list of positions found in the segment file's entries.
// It uses the encoded SegmentInfo at the beginning of the file for segment sepcific metadata.
// It returns an error if it cannot create the segment object.
func initSegment(f *os.File) (*segment, []int64, error) {
	// decode the segment info starting at offset 0 of file
	info, n, err := pb.DecodeInfo(f, 0)
	if err != nil {
		return nil, nil, err
	}

	// get the file info for the size
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get file info")
	}

	// initialize segment object
	seg := &segment{
		index:       int(info.Index),
		capacity:    int(info.Capacity),
		startOffset: atomic.NewInt64(n),
		lastOffset:  atomic.NewInt64(fi.Size() - 1),
		pmtx:        &sync.RWMutex{},
		pageTable:   make(map[int64]int64, 0),
		fmtx:        &sync.Mutex{},
		file:        f,
	}

	// restore the segment page table
	positions, err := seg.restore()
	if err != nil {
		return nil, nil, err
	}

	// return the segments and entry positions
	return seg, positions, nil
}

// restore uses a segment iterator to iterate over the entries in the segment file to rebuild the page table.
// it returns a list of entry positions found in the segment file or an error.
func (s *segment) restore() ([]int64, error) {
	// lock the page table during restore process
	s.pmtx.Lock()
	defer s.pmtx.Unlock()

	// create the segment iterator starting at the first entry
	it := segmentIterator{s, s.startOffset.Load()}

	var iterr error
	var done bool
	var positions []int64

	for {
		// get the next entry
		e, offset, nextOff, err := it.next()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}

			iterr = err
			break
		}

		// map virtual address to physical offset in page table
		s.pageTable[e.Position] = offset

		// add position to list
		positions = append(positions, e.Position)

		s.lastOffset.Store(nextOff)
	}

	if !done {
		return nil, iterr
	}

	return positions, nil
}

// readAt reads the entry at the physical address corresponding to position, or an error.
func (s *segment) readAt(position int64) (*pb.Entry, error) {
	// look up physical offset for position in page table
	s.pmtx.RLock()
	offset, ok := s.pageTable[position]
	s.pmtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("no entry found in segment: %v at position: %v", s.index, position)
	}

	// read the entry using the physical offset
	entry, _, err := s.readAtOffset(offset)

	return entry, err
}

// readAtOffset reads the entry at its physical address in the segment file,
// returning the entry and the number of bytes read or an error.
func (s *segment) readAtOffset(offset int64) (*pb.Entry, int, error) {
	return pb.Decode(s.file, offset)
}

var (
	// errSegmentFull is returned by append when an append operation would cause the segment to exceed its capacity
	errSegmentFull = errors.New("segment full")
)

// location represents the physical location of an entry
type location struct {
	segment int
	offset  int64
}

// append appends an entry to the segment file returning an error if unsuccessful.
// NOTE: e.Position should be set before calling append.
func (s *segment) append(e *pb.Entry) error {
	// encode our entry and its size into bytes
	bytes, err := pb.Encode(e)
	if err != nil {
		return err
	}

	// if written, entry would cause segment to exceed its capacity
	if (len(bytes) + int(s.lastOffset.Load())) > s.capacity {
		return errSegmentFull
	}

	// lock file when writing
	s.fmtx.Lock()
	defer s.fmtx.Unlock()

	// write the encoded entry to the file
	n, err := s.file.Write(bytes)
	if err != nil {
		return err
	}

	// get reference to entries offset
	offset := s.lastOffset.Load()

	// update segment's last offset atomically
	_ = s.lastOffset.Add(int64(n))

	return nil
}
