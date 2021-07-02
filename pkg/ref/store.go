package ref

import "fmt"

type ReflogReader interface {
	Read() (*Reflog, error)
	Close() error
}

type Store interface {
	SetWithLog(key string, val []byte, log *Reflog) error
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Delete(key string) error
	Filter(prefix string) (m map[string][]byte, err error)
	FilterKey(prefix string) (keys []string, err error)
	Rename(oldKey, newKey string) (err error)
	Copy(srcKey, dstKey string) (err error)
	LogReader(key string) (ReflogReader, error)
}

var ErrKeyNotFound = fmt.Errorf("key not found")
