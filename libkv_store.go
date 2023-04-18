package cloud

import (
	"github.com/docker/libkv/store"
)

type libkvStoreAdapter struct {
	UnorderedStore
}

var _ = (store.Store)((*libkvStoreAdapter)(nil))

func GetLibkvStore(s UnorderedStore) store.Store {
	return &libkvStoreAdapter{s}
}

func (a *libkvStoreAdapter) Close() {
	DoStoreClose(a.UnorderedStore)
}

func (a *libkvStoreAdapter) AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error) {
	if as, ok := a.UnorderedStore.(AtomicUnorderedStore); ok {
		return as.AtomicPut(key, value, previous, options)
	}
	return false, nil, ErrCallNotSupported
}

func (a *libkvStoreAdapter) AtomicDelete(key string, previous *KVPair) (bool, error) {
	if as, ok := a.UnorderedStore.(AtomicUnorderedStore); ok {
		return as.AtomicDelete(key, previous)
	}
	return false, ErrCallNotSupported
}

func (*libkvStoreAdapter) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*libkvStoreAdapter) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*libkvStoreAdapter) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

func (*libkvStoreAdapter) List(directory string) ([]*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*libkvStoreAdapter) DeleteTree(directory string) error {
	return store.ErrCallNotSupported
}
