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

	"github.com/pkg/errors"
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
	hash KeyHashFunc
}

// IndexOption is func that modifies the index configuration options.
type IndexOption func(*options)

// SetHashFunc overrides the Index default hashing func.
func SetHashFunc(hash KeyHashFunc) IndexOption {
	return func(opts *options) {
		opts.hash = hash
	}
}

type entryLocation struct {
	segment int
	offset  int64
}

// Index is a hash index implementation
type Index struct {
	log *logrus.Logger

	// store is the underlying storage layer
	store storage.Storer

	// mtx guards keys
	mtx sync.RWMutex
	// keys maps keys to their virtual storage address
	keys map[string]int64

	hash KeyHashFunc
}

// NewIndex accepts a variadic number of option funcs for configuration.
// It returns a configured Hash Index ready to start running operations.
func NewIndex(log *logrus.Logger, store storage.Storer, opts ...IndexOption) (*Index, error) {
	// default config
	cfg := &options{
		hash: NoHash,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	i := &Index{
		log:   log,
		store: store,
		keys:  make(map[string]int64, 0),
		hash:  cfg.hash,
	}

	err := i.restore()
	if err != nil {
		return nil, errors.Wrap(err, "could not restore index")
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

	// append entry to storage log
	pos, err := i.store.Append(entry)
	if err != nil {
		return err
	}

	// store the offset where we started writing
	i.mtx.Lock()
	defer i.mtx.Unlock()
	i.keys[hash] = pos

	return nil
}

// Get gets the value at key in the log and returns the value or an error if it wasn't found.
func (i *Index) Get(key string) (string, error) {
	// get the key's hash
	hash, err := i.hash([]byte(key))
	if err != nil {
		return "", err
	}

	// get the position from the map, if we don't find it we don't have the key
	i.mtx.RLock()
	pos, ok := i.keys[hash]
	i.mtx.RUnlock()
	if !ok {
		return "", fmt.Errorf("did not find key: %s in index", string(key))
	}

	i.log.Debugf("reading entry at position: %v", pos)

	e, err := i.store.ReadAt(pos)
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

	// check if we have a key to delete, if we don't it's an error
	i.mtx.Lock()
	defer i.mtx.Unlock()
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
	_, err = i.store.Append(entry)
	if err != nil {
		return err
	}

	// remove key from map, mtx already locked
	delete(i.keys, hash)

	return nil
}

// restore reads the storage log and restores the inmemory keys
func (i *Index) restore() error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	// get an iterator pointing to the beginning of the commit log
	it := i.store.Begin()

	var iterr error
	var done bool

	for {
		// read entries from file until we encounter an eof or an unexpected error
		e, err := it.Next()
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
			i.keys[e.GetKey()] = e.Position
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
