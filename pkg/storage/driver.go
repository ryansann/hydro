package storage

type Driver interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Iterate(func(key string, value []byte) error) error
	Enable() error
	Close() error
}
