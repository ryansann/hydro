// Package hash implements a simple storage agnostic hash index for a key value store.
//
// - no segmentation / compaction - implies that keys can and will appear multiple times, the most recent entry will reflect the actual state in the keys
// - uses single append only log (can grow boundlessly with no segmentation / compaction)
// - keeps entire keyset in an in-memory map (keys)
// - keys maps a keys hash to its corresponding log entry's location in the underlying paginated storage
//
// Operations:
// - get
// - set
// - del
// - restore -> restore the keys map from the commit log
package hash

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/ryansann/hydro/pb"
	"github.com/ryansann/hydro/storage"
	"github.com/sirupsen/logrus"
)

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

// NoHash is a HashFunc that returns the key as a string without hashing.
func NoHash(key []byte) (string, error) {
	return string(key), nil
}

type options struct {
	hash    KeyHashFunc
	restore bool
}

// IndexOption is func that modifies the index configuration options.
type IndexOption func(*options)

// SetHashFunc overrides the Index default hashing func.
func SetHashFunc(hash KeyHashFunc) IndexOption {
	return func(opts *options) {
		opts.hash = hash
	}
}

// Restore tells the Index whether or not to restore the keys from the storage log if it exists already.
func Restore(v bool) IndexOption {
	return func(opts *options) {
		opts.restore = v
	}
}

type entryLocation struct {
	segment int
	offset  int64
}

// Index is a hash index implementation
type Index struct {
	log *logrus.Logger

	store storage.Storer

	// mtx guards keys
	mtx  sync.RWMutex
	keys map[string]entryLocation

	hash KeyHashFunc
}

// NewIndex accepts a variadic number of option funcs for configuration.
// It returns a configured Hash Index ready to start running operations.
func NewIndex(log *logrus.Logger, store storage.Storer, opts ...IndexOption) (*Index, error) {
	// default config
	cfg := &options{
		hash:    NoHash,
		restore: true,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	i := &Index{
		log:   log,
		store: store,
		keys:  make(map[string]entryLocation, 0),
		hash:  cfg.hash,
	}

	if cfg.restore {
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

	i.log.Debugf("setting entry: %+v", *entry)

	i.mtx.Lock()
	defer i.mtx.Unlock()

	// append entry to storage log
	seg, off, err := i.store.Append(entry)
	if err != nil {
		return err
	}

	// store the offset where we started writing
	i.keys[hash] = entryLocation{segment: seg, offset: off}

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

	e, _, err := i.store.ReadAt(loc.segment, loc.offset)
	if err != nil {
		return "", err
	}

	i.log.Debugf("got entry with key: %s", hash)

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

	i.log.Debugf("setting entry: %+v", *entry)

	// append deletion entry to storage log
	_, _, err = i.store.Append(entry)
	if err != nil {
		return err
	}

	// remove key from map
	delete(i.keys, hash)

	return nil
}

// Restore reads the storage log and restores the inmemory keys
func (i *Index) Restore() error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var iterr error
	done := false

	// get an iterator pointing to the beginning of the commit log
	it := i.store.Begin()
	defer it.Done()

	for {
		// read entries from file until we encounter an eof or an unexpected error
		e, seg, off, err := it.Next()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}

			iterr = err
			break
		}

		i.log.Debugf("iterator yielded entry: %+v", *e)

		// add the key if we encounter a write entry, delete it if we encounter a delete entry.
		switch e.GetType() {
		case pb.EntryType_WRITE:
			i.keys[e.GetKey()] = entryLocation{seg, off}
		case pb.EntryType_DELETE:
			delete(i.keys, e.GetKey())
		}
	}

	// if we encountered an unexpected error, return it
	if !done {
		return iterr
	}

	i.log.Info("index restored")

	return nil
}
