package file

import "github.com/ryansann/hydro/pb"

// iterator provides functionality for iterating over entries in the store.
type iterator struct {
	s      *Store
	offset int64
}

// Next returns the next entry or an error.
func (i *iterator) Next() (*pb.Entry, error) {
	e, _, _, err := i.next()
	return e, err
}

// next returns the next entry, its physical offset, and the next physical offset, or an error.
func (i *iterator) next() (*pb.Entry, int64, int64, error) {
	e, n, err := i.s.readAt(i.offset)
	if err != nil {
		return nil, 0, 0, err
	}

	offset := i.offset

	i.offset += int64(n)

	return e, offset, i.offset, nil
}
