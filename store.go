package cloud

import (
	"io"

	"github.com/docker/libkv/store"
)

// Errors mirrored from libkv
var (
	ErrKeyNotFound      = store.ErrKeyNotFound
	ErrKeyExists        = store.ErrKeyExists
	ErrKeyModified      = store.ErrKeyModified
	ErrCallNotSupported = store.ErrCallNotSupported
)

// Types mirrored from libkv
type (
	KVPair       = store.KVPair
	WriteOptions = store.WriteOptions
	LockOptions  = store.LockOptions
	Locker       = store.Locker
)

// Subset of libkv/store.Store
type UnorderedStore interface {
	Get(key string) (*KVPair, error)
	// TODO: Potentially get rid of this function?
	Exists(key string) (bool, error)
	Put(key string, value []byte, options *WriteOptions) error
	Delete(key string) error
}

type AtomicUnorderedStore interface {
	UnorderedStore

	AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error)
	AtomicDelete(key string, previous *KVPair) (bool, error)
}

type OrderedStore interface {
	UnorderedStore

	ListKeys(start string) ([]string, error)
}

func DoStoreClose(s UnorderedStore) error {
	type libkvCloser interface {
		Close()
	}

	if c, ok := s.(io.Closer); ok {
		return c.Close()
	} else if c, ok := s.(libkvCloser); ok {
		c.Close()
	}
	return nil
}
