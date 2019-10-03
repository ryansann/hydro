package fileseg

import (
	"io"

	"github.com/ryansann/hydro/pb"
)

// Iterator can be used to iterate over entries in the storage layer,
// it implements the store.ForwardIterator interface.
type Iterator struct {
	s       *Store
	segment int
	offset  int64
}

// Next returns the next entry, its segment, and its offset or an error if there was one.
func (i *Iterator) Next() (*pb.Entry, int, int64, error) {
	if i.segment > len(i.s.segments)-1 {
		return nil, 0, 0, io.EOF
	}

	// skip the segment info when reading, default beginning to startOffset
	if i.offset == 0 {
		i.offset = i.s.segments[i.segment].startOffset
	}

	// if the offset is the last offset or more, go to the next segment
	if i.offset >= i.s.segments[i.segment].lastOffset {
		// return an io.EOF error if there is not another segment
		if i.segment == len(i.s.segments)-1 {
			return nil, 0, 0, io.EOF
		}

		i.segment++
	}

	e, n, err := i.s.segments[i.segment].readAt(i.offset)
	if err != nil {
		return nil, 0, 0, err
	}

	offset := i.offset

	// update offset for next call
	i.offset += n

	return e, i.segment, offset, nil
}
