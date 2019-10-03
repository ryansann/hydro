package file

import "github.com/ryansann/hydro/pb"

// Iterator provides functionality for iterating over entries in the store.
type Iterator struct {
	offset int64
	s      *Store
}

// Next returns the next entry its segment (always 0), and its offset or an error if there was one.
func (i *Iterator) Next() (*pb.Entry, int, int64, error) {
	return nil, 0, 0, nil
}
