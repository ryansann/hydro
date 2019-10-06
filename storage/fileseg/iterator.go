package fileseg

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// iterator can be used to iterate over entries in the storage layer,
// it implements the store.ForwardIterator interface.
type iterator struct {
	s *Store
	// segment is the segment index
	segment int
	// curIt is a segment iterator fot the current segment
	curIt *segmentIterator
}

// Next returns the next entry or an error.
func (i *iterator) Next() (*pb.Entry, error) {
	e, _, _, err := i.next()
	return e, err
}

// next returns the next entry, its segment, and its offset or an error if there was one.
func (i *iterator) next() (*pb.Entry, location, location, error) {
	if i.segment > len(i.s.segments)-1 {
		return nil, location{}, location{}, io.EOF
	}

	// skip the segment info when reading, default beginning to startOffset
	if i.offset == 0 {
		i.offset = i.s.segments[i.segment].startOffset
	}

	// if the offset is the last offset or more, go to the next segment
	if i.offset >= i.s.segments[i.segment].lastOffset {
		// return an io.EOF error if there is not another segment
		if i.segment == len(i.s.segments)-1 {
			return nil, location{}, location{}, io.EOF
		}

		i.segment++
		i.offset = i.s.segments[i.segment].startOffset
	}

	e, n, err := i.s.segments[i.segment].readAt(i.offset)
	if err != nil {
		return nil, location{}, location{}, err
	}

	curOffset := i.offset

	// update offset for next call
	i.offset += int64(n)

	return e, location{i.segment, curOffset}, location{i.segment, curOffset}, nil
}

// segmentIterator provides functionality for iterating over the entries in a segment file.
// segmentIterator is not safe for concurrent use.
type segmentIterator struct {
	s      *segment
	offset int64
}

// next returns the next entry, its offset, and the next entry's offset or an error.
func (i *segmentIterator) next() (*pb.Entry, int64, int64, error) {
	e, n, err := i.s.readAtOffset(i.offset)
	if err != nil {
		return nil, 0, 0, err
	}

	offset := i.offset

	i.offset += int64(n)

	return e, offset, i.offset, nil
}
