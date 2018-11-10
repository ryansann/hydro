package hash

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/google/uuid"
	"github.com/ryansann/hydro/storage/hash/pb"
)

// Hash Index with no segmentation / compaction
// - uses single append only log (can grow boundlessly w/ no segmentation / compaction)
// - keeps entire keyset in memory in keymap
// - keymap maps a keys hash to its offset in the storage file
// - read -> lookup key in keymap, find it's offset and seek to that location and read it's value
// - write -> append entry to log, update location in keymap
// - delete -> remove entry from keymap

// HashFunc is a hash func that takes a slice of bytes and returns a hash or an error
type HashFunc func(key []byte) (string, error)

// DefaultHash is a HashFunc that uses the md5 hashing algorithm
func DefaultHash(key []byte) (string, error) {
	h := md5.New()

	_, err := h.Write(key)
	if err != nil {
		return "", err
	}

	res := h.Sum(nil)
	return string(res), nil
}

// NoHash is a HashFunc that returns the key as a string without hashing
func NoHash(key []byte) (string, error) {
	return string(key), nil
}

// Index is a hash index implementation
type Index struct {
	mtx    sync.RWMutex
	keymap map[string]int
	log    *os.File
	offset int
	hash   HashFunc
}

// New returns a new hash index
func New(dir string, hash HashFunc) (*Index, error) {
	logpath, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("could not get absolute path for dir: %s %v", dir, err)
	}

	filename, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("could not generate logfile name: %v", err)
	}

	file := logpath + "/" + filename.String()

	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not create/open log file: %v", err)
	}

	return &Index{
		mtx:    sync.RWMutex{},
		keymap: make(map[string]int),
		log:    f,
		offset: 0,
		hash:   hash,
	}, nil
}

// Write appends the key, value to the log and updates the keymap
func (i *Index) Write(key []byte, value []byte) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	// get the key's hash
	hash, err := i.hash(key)
	if err != nil {
		return err
	}

	// create our log entry
	entry := &pb.LogEntry{
		Key:   hash,
		Value: value,
	}

	// marshal the data into bytes
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("could not marshal log entry: %v", err)
	}

	// write the data to the file
	nbytes, err := i.log.Write(data)
	if err != nil {
		return fmt.Errorf("could not write log entry: %v", err)
	}

	// store the offset where we started writing
	i.keymap[hash] = i.offset

	// update the offset with the number of bytes written
	i.offset += nbytes

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
