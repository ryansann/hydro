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
	return i.next()
}

// next returns the next entry, its segment, and the next segment or an error if there was one.
func (i *iterator) next() (*pb.Entry, error) {
	// if the segment index is greater than the number of segments, we reached the end
	if i.segment > len(i.s.segments)-1 {
		return nil, io.EOF
	}

	// initialize curIt
	if i.curIt == nil {
		seg := i.s.segments[i.segment]
		i.curIt = &segmentIterator{seg, seg.startOffset.Load()}
	}

	e, _, _, err := i.curIt.next()
	if err != nil {
		// if we reach the end of the segment go to the next
		if err == io.EOF {
			i.segment++
			i.curIt = nil
			return i.next()
		}

		// other error while reading from segment
		return nil, err
	}

	// we got an entry from segment without an error
	return e, nil
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
