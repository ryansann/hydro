package hash

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// Hash Index with no segmentation / compaction
// - uses single append only log (can grow boundlessly w/ no segmentation / compaction)
// - keeps entire keyset in memory in keymap
// - keymap maps a keys hash to its offset in the storage file
// - read -> lookup key in keymap, find it's offset and seek to that location and read it's value
// - write -> append entry to log, update location in keymap
// - delete -> remove entry from keymap

// Func is a hash func that takes a slice of bytes and returns a hash or an error
type Func func(key []byte) (string, error)

// DefaultHash is a default hashing algorithm
func DefaultHash(key []byte) (string, error) {
	return "", nil
}

// Index is a hash index implementation
type Index struct {
	mtx    sync.RWMutex
	keymap map[string]int
	log    *os.File
	hash   Func
}

// New returns a new hash index
func New(dir string, hash Func) (*Index, error) {
	logpath, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("could not get absolute path for dir: %s %v", dir, err)
	}

	filename, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("could not generate logfile name: %v", err)
	}

	f, err := os.Create(logpath + "/" + filename.String())
	if err != nil {
		return nil, fmt.Errorf("could not create log file: %v", err)
	}

	return &Index{
		mtx:    sync.RWMutex{},
		keymap: make(map[string]int),
		log:    f,
		hash:   hash,
	}, nil
}

// Write appends the key, value to the log and updates the keymap
func (i *Index) Write(key []byte, value []byte) error {
	return nil
}

// Read looksup the key in the log and returns the value or an error if it wasn't found
func (i *Index) Read(key []byte) ([]byte, error) {
	return []byte{}, nil
}

// Delete removes the key from the keymap
func (i *Index) Delete(key []byte) error {
	return nil
}
