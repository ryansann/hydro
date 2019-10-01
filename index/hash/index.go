package hash

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ryansann/hydro/pb"
	"github.com/ryansann/hydro/storage"
)

// Hash Index with no segmentation / compaction
// - uses single append only log (can grow boundlessly with no segmentation / compaction)
// - keeps entire keyset in an in-memory map (Index.keys)
// - keys maps a keys hash to its corresponding log entry's offset in the storage file
// - starts a background process to ensure log file is synced to disk every sync interval
//
// Note: in log file an entry is represented as -> <size><data>
// where size is a uint32 encoded in little endian to 4 bytes, storing the length in bytes of data,
// and data is a pb.LogEntry marshaled to bytes
// A log file looks like: <size><data><size><data>...<size><data><size><data>
// No compaction implies that keys can and will appear multiple times, the most recent entry will reflect the actual state in the keys
//
// Operations:
// - read -> lookup key in keys, find it's offset, read data size at offset, read data from offset+4, get value from data
// - write -> create a log entry pb object, marshal it to bytes (wire format), get its size in bytes,
// store its size and data bytes in log file as an entry, update or insert key/offset into keys
// - delete -> write a delete log entry (for restore purposes), remove key from keys
// - restore -> start from beginning of storage file, read all log entries, continuously updating keys, by the end the keys will
// reflect the state the index was in when it was closed or it crashed (assuming no data was corrupted during crash)
// - close -> closes the storage log file

// KeyHashFunc is a hash func that takes a slice of bytes and returns a hash or an error
type KeyHashFunc func(key []byte) (string, error)

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
	hash    KeyHashFunc
	restore bool
}

// IndexOption is func that modifies the server's configuration options
type IndexOption func(*options)

// SyncInterval overrides the Index default sync interval
func SyncInterval(dur time.Duration) IndexOption {
	return func(opts *options) {
		opts.sync = dur
	}
}

// SetHashFunc overrides the Index default hashing func
func SetHashFunc(hash KeyHashFunc) IndexOption {
	return func(opts *options) {
		opts.hash = hash
	}
}

// NoRestore tells the Index not to restore the keys from the storage log if it exists already
func NoRestore() IndexOption {
	return func(opts *options) {
		opts.restore = false
	}
}

type entryLocation struct {
	page   int
	offset int64
}

// Index is a hash index implementation
type Index struct {
	log storage.Storer

	// mtx guards keys
	mtx  sync.RWMutex
	keys map[string]entryLocation

	hash KeyHashFunc
	done chan struct{}
}

// NewIndex accepts a variadic number of option funcs for configuration.
// It returns a configured Hash Index ready to start running operations.
func NewIndex(store storage.Storer, opts ...IndexOption) (*Index, error) {
	// default config
	cfg := &options{
		sync:    time.Second * 30,
		hash:    NoHash,
		restore: true,
	}

	canrestore := false

	for _, opt := range opts {
		opt(cfg)
	}

	done := make(chan struct{})

	go syncLoop(cfg.sync, store, done)

	i := &Index{
		log:  store,
		keys: make(map[string]entryLocation, 0),
		hash: cfg.hash,
		done: done,
	}

	if cfg.restore && canrestore {
		err := i.Restore()
		if err != nil {
			return nil, err
		}
	}

	return i, nil
}

// Set sets key to val, it returns an error if the operation is unsuccessful.
func (i *Index) Set(key string, val string) error {
	kbytes, vbytes := []byte(key), []byte(val)

	// get the key's hash
	hash, err := i.hash(kbytes)
	if err != nil {
		return err
	}

	// create our log entry contents
	entry := &pb.Entry{
		Type:  pb.EntryType_WRITE,
		Key:   hash,
		Value: vbytes,
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	// append entry to storage log
	pg, off, err := i.log.Append(entry)
	if err != nil {
		return err
	}

	// store the offset where we started writing
	i.keys[hash] = entryLocation{page: pg, offset: off}

	return nil
}

// Get gets the value at key in the log and returns the value or an error if it wasn't found.
func (i *Index) Get(key string) (string, error) {
	// get the key's hash
	hash, err := i.hash([]byte(key))
	if err != nil {
		return "", err
	}

	i.mtx.RLock()
	defer i.mtx.RUnlock()

	// get the offset from the map, if we don't find it we don't have the key
	loc, ok := i.keys[hash]
	if !ok {
		return "", fmt.Errorf("did not find key: %s in index", string(key))
	}

	e, err := i.log.ReadAt(loc.page, loc.offset)
	if err != nil {
		return "", err
	}

	// return the value for the key
	return string(e.GetValue()), nil
}

// Del removes the key from the keys.
func (i *Index) Del(key string) error {
	// get the key's hash
	hash, err := i.hash([]byte(key))
	if err != nil {
		return err
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	// check if we have a key to delete, if we don't it's an error
	_, ok := i.keys[hash]
	if !ok {
		return fmt.Errorf("nothing to delete for key: %s", string(key))
	}

	// create our log entry contents
	entry := &pb.Entry{
		Type:  pb.EntryType_DELETE,
		Key:   hash,
		Value: []byte{},
	}

	// append deletion entry to storage log
	_, _, err = i.log.Append(entry)
	if err != nil {
		return err
	}

	// remove key from map
	i.mtx.Lock()
	defer i.mtx.Unlock()
	delete(i.keys, hash)

	return nil
}

// Restore reads the storage log and restores the inmemory keys
func (i *Index) Restore() error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var page int
	var offset int64
	var readerr error
	done := false

	for {
		// read entries from file until we encounter an eof or an unexpected error
		e, nextPage, nextOffset, err := i.log.Scan(page, offset)
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}

			readerr = err
			break
		}

		// add the key if we encounter a write entry, delete it if we encounter a delete entry.
		switch e.GetType() {
		case pb.EntryType_WRITE:
			i.mtx.Lock()
			i.keys[e.GetKey()] = entryLocation{page, offset}
			i.mtx.Unlock()
		case pb.EntryType_DELETE:
			i.mtx.Lock()
			delete(i.keys, e.GetKey())
			i.mtx.Unlock()
		}

		page, offset = nextPage, nextOffset
	}

	// if we encountered an unexpected error, return it
	if !done {
		return readerr
	}

	return nil
}

// sync is intended to be run as a background go routine that flushes data to disk every interval
func syncLoop(interval time.Duration, s storage.Storer, done <-chan struct{}) {
	for {
		select {
		case <-time.After(interval):
			err := s.Sync()
			if err != nil {
				fmt.Println(err)
			}
		case <-done:
			return
		}
	}
}
