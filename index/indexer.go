package index

// Indexer is the interface that indexes should implement. An index interacts with a store
// in order to perform CRUD operations. This interface provides CRUD operations through
// the Get, Set, Delete methods. An index also needs to be able to restore any necessary
// inmemory components after a crash, so Restore method must also be implemented by an Index.
type Indexer interface {
	// Get returns a value for the corresponding key, returning an error if the operation was unsuccessful
	Get(key string) (string, error)
	// Set sets key to val, returning an error if the operation was unsuccessful
	Set(key, val string) error
	// Del removes key and its corresponding value, returning an error if the operation was unsuccessful
	Del(key string) error
	// Restore rebuilds the index using the underlying storage, returning an error if the operation was unsuccessful.
	Restore() error
}
