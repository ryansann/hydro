package hash

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
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
// - write -> append write entry to log, update location in keymap
// - delete -> append delete entry to log, remove entry from keymap

// Log Entry: <size:int32-little-endian><data:protobuf-encoded>

// HashFunc is a hash func that takes a slice of bytes and returns a hash or an error
type HashFunc func(key []byte) (string, error)

// DefaultHash is a HashFunc that uses the sha1 hashing algorithm
func DefaultHash(key []byte) (string, error) {
	h := sha1.New()

	_, err := h.Write(key)
	if err != nil {
		return "", err
	}

	str := hex.EncodeToString(h.Sum(nil))
	return str, nil
}

// NoHash is a HashFunc that returns the key as a string without hashing
func NoHash(key []byte) (string, error) {
	return string(key), nil
}

// Index is a hash index implementation
type Index struct {
	mtx    sync.RWMutex
	keymap map[string]int64
	log    *os.File
	offset int64
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
		keymap: make(map[string]int64),
		log:    f,
		offset: 0,
		hash:   hash,
	}, nil
}

// Write appends the key, value to the log and updates the keymap
func (i *Index) Write(key []byte, val []byte) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	// get the key's hash
	hash, err := i.hash(key)
	if err != nil {
		return err
	}

	// create our log entry contents
	entry := &pb.LogEntry{
		Type:  pb.EntryType_WRITE,
		Key:   hash,
		Value: val,
	}

	// marshal the data into protobuf format
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("could not marshal log entry: %v", err)
	}

	// write the size of the data to a new buffer
	sizebytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebytes, uint32(len(data)))
	buf := bytes.NewBuffer(sizebytes)

	// write the data to the buffer
	_, err = buf.Write(data)
	if err != nil {
		return fmt.Errorf("could not write data to buffer: %v", err)
	}

	// write the data to the file
	n, err := i.log.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("could not write log entry: %v", err)
	}

	// store the offset where we started writing
	i.keymap[hash] = i.offset

	// update the offset with the number of bytes written
	i.offset += int64(n)

	// write contents to stable storage
	err = i.log.Sync()
	if err != nil {
		return err
	}

	return nil
}

// Read looksup the key in the log and returns the value or an error if it wasn't found
func (i *Index) Read(key []byte) ([]byte, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	// get the key's hash
	hash, err := i.hash(key)
	if err != nil {
		return nil, err
	}

	offset, ok := i.keymap[hash]
	if !ok {
		return nil, fmt.Errorf("did not find key: %s in index", string(key))
	}

	sizebytes := make([]byte, 4) // data size is int32 (4 bytes)
	_, err = i.log.ReadAt(sizebytes, offset)
	if err != nil {
		return nil, fmt.Errorf("could not read size at offset: %v, %v", offset, err)
	}

	size := binary.LittleEndian.Uint32(sizebytes)

	data := make([]byte, size)
	_, err = i.log.ReadAt(data, offset+4) // add 4 bytes since we read uint32 size already
	if err != nil {
		return nil, fmt.Errorf("could not read data from file: %v", err)
	}

	var entry pb.LogEntry
	err = proto.Unmarshal(data, &entry)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal data into log entry: %v", err)
	}

	return entry.GetValue(), nil
}

// Delete removes the key from the keymap
func (i *Index) Delete(key []byte) error {
	return nil
}

// Restore parses the log stored on disk and restores the inmemory keymap
func (i *Index) Restore() error {
	return nil
}

// Close implements the io.Closer interface, it closes the log file
func (i *Index) Close() error {
	err := i.log.Close()
	if err != nil {
		return err
	}

	return nil
}

// cleanup is a utility for testing that closes and removes the storage log file
func (i *Index) cleanup() error {
	err := i.Close()
	if err != nil {
		return err
	}

	err = os.Remove(i.log.Name())
	if err != nil {
		return err
	}

	return nil
}
