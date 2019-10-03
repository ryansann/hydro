package index

// Indexer is the interface that indexes should implement. An index interacts with a store
// in order to perform CRUD operations. This interface provides CRUD operations through
// the Get, Set, Delete methods. An index also needs to be able to restore any necessary
// inmemory components after a crash, so Restore method must also be implemented by an Index.
type Indexer interface { // stutter :/
	Get(key string) (string, error)
	Set(key, val string) error
	Del(key string) error
	Restore() error
}
