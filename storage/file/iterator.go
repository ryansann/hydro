package file

import "github.com/ryansann/hydro/pb"

// iterator provides functionality for iterating over entries in the store.
type iterator struct {
	offset int64
	s      *Store
}

// Next returns the next entry its segment (always 0), and its offset or an error if there was one.
func (i *iterator) Next() (*pb.Entry, int, int64, error) {
	e, n, err := i.s.ReadAt(0, i.offset)
	if err != nil {
		return nil, 0, 0, err
	}

	offset := i.offset

	i.offset += int64(n)

	return e, 0, offset, nil
}

// Done is a noop since there is background processes afecting storage.
func (i *iterator) Done() {}
