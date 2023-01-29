package index

type Indexer interface {
	Get(key string) (int64, error)
	Set(key string, offset int64) error
	Del(key string) (int64, error)
}
