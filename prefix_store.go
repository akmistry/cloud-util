package cloud

import (
  "fmt"
	"strings"

	"github.com/akmistry/go-util/badgerkv"
	"github.com/docker/libkv/store"
)

type PrefixStore struct {
	s      store.Store
	prefix string
}

var _ = (store.Store)((*PrefixStore)(nil))

func NewPrefixStore(s store.Store, prefix string) *PrefixStore {
	return &PrefixStore{
		s:      s,
		prefix: prefix,
	}
}

func (s *PrefixStore) makeKey(key string) string {
  k := s.prefix + key
  fmt.Println("Making key: ", k)
  return k
}

func (s *PrefixStore) Close() {
	// No-op, since it doesn't make sense to "close" a prefix.
}

func (s *PrefixStore) Get(key string) (*store.KVPair, error) {
	return s.s.Get(s.makeKey(key))
}

func (s *PrefixStore) Exists(key string) (bool, error) {
	return s.s.Exists(s.makeKey(key))
}

func (s *PrefixStore) Put(key string, value []byte, options *store.WriteOptions) error {
	return s.s.Put(s.makeKey(key), value, options)
}

func (s *PrefixStore) Delete(key string) error {
	return s.s.Delete(s.makeKey(key))
}

func (s *PrefixStore) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return s.s.AtomicPut(s.makeKey(key), value, previous, options)
}

func (s *PrefixStore) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return s.s.AtomicDelete(s.makeKey(key), previous)
}

func (s *PrefixStore) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return s.s.Watch(s.makeKey(key), stopCh)
}

func (*PrefixStore) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *PrefixStore) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return s.s.NewLock(s.makeKey(key), options)
}

func (*PrefixStore) List(directory string) ([]*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*PrefixStore) DeleteTree(directory string) error {
	return store.ErrCallNotSupported
}

func (s *PrefixStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(badgerkv.Lister)
	if !ok {
		return nil, store.ErrCallNotSupported
	}

	startKey := s.makeKey(start)
	keys, err := lister.ListKeys(startKey)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		if !strings.HasPrefix(k, s.prefix) {
			return keys[:i], nil
		}
		keys[i] = strings.TrimPrefix(k, s.prefix)
	}
	return keys, nil
}
