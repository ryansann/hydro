package hash

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ryansann/hydro/storage/hash/pb"
)

// Hash Index with no segmentation / compaction
// - uses single append only log (can grow boundlessly with no segmentation / compaction)
// - keeps entire keyset in an in-memory map (Index.keymap)
// - keymap maps a keys hash to its corresponding log entry's offset in the storage file
// - starts a background process to ensure log file is synced to disk every sync interval
//
// Note: in log file an entry is represented as -> <size><data>
// where size is a uint32 encoded in little endian to 4 bytes, storing the length in bytes of data,
// and data is a pb.LogEntry marshaled to bytes
// A log file looks like: <size><data><size><data>...<size><data><size><data>
// No compaction implies that keys can and will appear multiple times, the most recent entry will reflect the actual state in the keymap
//
// Operations:
// - read -> lookup key in keymap, find it's offset, read data size at offset, read data from offset+4, get value from data
// - write -> create a log entry pb object, marshal it to bytes (wire format), get its size in bytes,
// store its size and data bytes in log file as an entry, update or insert key/offset into keymap
// - delete -> write a delete log entry (for restore purposes), remove key from keymap
// - restore -> start from beginning of storage file, read all log entries, continuously updating keymap, by the end the keymap will
// reflect the state the index was in when it was closed or it crashed (assuming no data was corrupted during crash)
// - close -> closes the storage log file

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

type options struct {
	sync    time.Duration
	file    string
	hash    HashFunc
	restore bool
}

// OptionFunc is func that modifies the server's configuration options
type OptionFunc func(*options)

// SyncInterval overrides the Index default sync interval
func SyncInterval(dur time.Duration) OptionFunc {
	return func(opts *options) {
		opts.sync = dur
	}
}

// StorageFile overrides the Index default storage file
func StorageFile(name string) OptionFunc {
	return func(opts *options) {
		opts.file = name
	}
}

// SetHashFunc overrides the Index default hashing func
func SetHashFunc(hash HashFunc) OptionFunc {
	return func(opts *options) {
		opts.hash = hash
	}
}

// NoRestore tells the Index not to restore the keymap from the storage log if it exists already
func NoRestore() OptionFunc {
	return func(opts *options) {
		opts.restore = false
	}
}

// Index is a hash index implementation
type Index struct {
	mtx    sync.RWMutex
	keymap map[string]int64
	log    *os.File
	offset int64
	hash   HashFunc
	done   chan struct{}
}

// NewIndex accepts a variadic number of option funcs for configuration.
// It returns a configured Hash Index ready to start running operations.
func NewIndex(opts ...OptionFunc) (*Index, error) {
	// default config
	cfg := &options{
		sync:    time.Second * 30,
		file:    "./data",
		hash:    NoHash,
		restore: true,
	}

	canrestore := false

	for _, opt := range opts {
		opt(cfg)
	}

	filename, err := filepath.Abs(cfg.file)
	if err != nil {
		return nil, fmt.Errorf("could not get absolute path for dir: %s %v", cfg.file, err)
	}

	// if the file already exists we can restore the index state from it
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		canrestore = true
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not create/open log file: %v", err)
	}

	done := make(chan struct{})

	go syncloop(cfg.sync, f, done)

	i := &Index{
		mtx:    sync.RWMutex{},
		keymap: make(map[string]int64),
		log:    f,
		offset: 0,
		hash:   cfg.hash,
		done:   done,
	}

	if cfg.restore && canrestore {
		err := i.Restore()
		if err != nil {
			return nil, err
		}
	}

	return i, nil
}

// Write appends the key, value to the log and updates the keymap
func (i *Index) Write(key []byte, val []byte) error {
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

	i.mtx.Lock()
	defer i.mtx.Unlock()

	// append entry to storage log
	n, err := i.append(entry)
	if err != nil {
		return err
	}

	// store the offset where we started writing
	i.keymap[hash] = i.offset

	// update the offset with the number of bytes written
	i.offset += n

	return nil
}

// Read looksup the key in the log and returns the value or an error if it wasn't found
func (i *Index) Read(key []byte) ([]byte, error) {
	// get the key's hash
	hash, err := i.hash(key)
	if err != nil {
		return nil, err
	}

	i.mtx.RLock()
	defer i.mtx.RUnlock()

	// get the offset from the map, if we don't find it we don't have the key
	offset, ok := i.keymap[hash]
	if !ok {
		return nil, fmt.Errorf("did not find key: %s in index", string(key))
	}

	e, _, err := i.read(offset)
	if err != nil {
		return nil, err
	}

	// return the value for the key
	return e.GetValue(), nil
}

// Delete removes the key from the keymap
func (i *Index) Delete(key []byte) error {
	// get the key's hash
	hash, err := i.hash(key)
	if err != nil {
		return err
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	// check if we have a key to delete, if we don't it's an error
	_, ok := i.keymap[hash]
	if !ok {
		return fmt.Errorf("nothing to delete for key: %s", string(key))
	}

	// create our log entry contents
	entry := &pb.LogEntry{
		Type:  pb.EntryType_DELETE,
		Key:   hash,
		Value: []byte{},
	}

	// append data to storage log
	n, err := i.append(entry)
	if err != nil {
		return err
	}

	// remove key from map
	delete(i.keymap, hash)

	// update the offset with the number of bytes written
	i.offset += n

	return nil
}

// Restore reads the storage log and restores the inmemory keymap
func (i *Index) Restore() error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var offset int64
	var readerr error
	done := false

	for {
		// read entries from file until we encounter an eof or an unexpected error
		e, n, err := i.read(offset)
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}
			readerr = err
			break
		}

		// only add it to keymap if it was a write operation, deletes won't be added
		if e.GetType() == pb.EntryType_WRITE {
			i.keymap[e.GetKey()] = offset
		}

		offset += int64(n) // add bytes read to offset
	}

	// if we encountered an unexpected error, return it
	if !done {
		return readerr
	}

	// set the offset to the end of the file
	i.offset = offset

	return nil
}

// Close implements the io.Closer interface, it closes the log file
func (i *Index) Close() error {
	defer close(i.done)

	// signal sync to stop
	i.done <- struct{}{}

	err := i.log.Close()
	if err != nil {
		return err
	}

	return nil
}

// append writes a log entry to the storage log and returns how many bytes it wrote or an error
func (i *Index) append(entry *pb.LogEntry) (int64, error) {
	// marshal the data into protobuf format
	data, err := proto.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("could not marshal log entry: %v", err)
	}

	// write the size of the data to a new buffer
	sizebytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebytes, uint32(len(data)))
	buf := bytes.NewBuffer(sizebytes)

	// write the data to the buffer
	_, err = buf.Write(data)
	if err != nil {
		return 0, fmt.Errorf("could not write data to buffer: %v", err)
	}

	// write the data to the file
	n, err := i.log.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("could not write log entry: %v", err)
	}

	return int64(n), nil
}

// read reads the log at offset and returns the entry's value or an error
func (i *Index) read(offset int64) (*pb.LogEntry, int, error) {
	// read the bytes storing the size of the data
	sizebytes := make([]byte, 4) // stored as uint32 (4 bytes)
	_, err := i.log.ReadAt(sizebytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// convert the bytes to a uint32
	size := binary.LittleEndian.Uint32(sizebytes)

	// read the data (protobuf) bytes
	data := make([]byte, size)
	_, err = i.log.ReadAt(data, offset+4) // add 4 bytes since we read uint32 size already
	if err != nil {
		return nil, 0, err // keep error as is so we can detect io.EOF
	}

	// unmarshal the data bytes into LogEntry data structure
	var entry pb.LogEntry
	err = proto.Unmarshal(data, &entry)
	if err != nil {
		return nil, 0, fmt.Errorf("could not unmarshal data into log entry: %v", err)
	}

	return &entry, 4 + len(data), nil
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

// syncloop is intended to be run as a background go routine that flushes data to disk every interval
func syncloop(interval time.Duration, f *os.File, done <-chan struct{}) {
	for {
		select {
		case <-time.After(interval):
			err := f.Sync()
			if err != nil {
				fmt.Println(err)
			}
		case <-done:
			return
		}
	}
}
