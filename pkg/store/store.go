package store

type WriteOptions struct {
	ContentType string
	TTL         int64
}

type Store interface {
	GetInternalStore() interface{}

	Get(key string) ([]byte, error)
	Set(key string, value []byte, options *WriteOptions) error

	Delete(key string) error
	DeleteAll(pattern string) error

	Exists(key string) (bool, error)
	Expire(key string, ttl int64) error

	Scan(prefix string, skip int, limit int, fn func(key string, val []byte)) error
	Count(prefix string) int

	Close() error
}
