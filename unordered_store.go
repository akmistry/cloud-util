package cloud

import (
	"github.com/docker/libkv/store"
)

type WriteOptions = store.WriteOptions

// Subset of libkv/store.Store
type UnorderedStore interface {
	Get(key string) (*store.KVPair, error)
	Exists(key string) (bool, error)
	Put(key string, value []byte, options *store.WriteOptions) error
	Delete(key string) error
}
type AtomicUnorderedStore interface {
	UnorderedStore

	AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error)
	AtomicDelete(key string, previous *store.KVPair) (bool, error)
}
