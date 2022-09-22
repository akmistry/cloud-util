package cloud

import (
	"time"

	"github.com/akmistry/go-util/badgerkv"
	"github.com/docker/libkv/store"
)

type DelayStore struct {
	s     store.Store
	delay time.Duration
}

var _ = (store.Store)((*DelayStore)(nil))

func NewDelayStore(s store.Store, delay time.Duration) *DelayStore {
	return &DelayStore{
		s:     s,
		delay: delay,
	}
}

func (s *DelayStore) Close() {
	s.s.Close()
}

func (s *DelayStore) Get(key string) (*store.KVPair, error) {
	time.Sleep(s.delay)
	return s.s.Get(key)
}

func (s *DelayStore) Exists(key string) (bool, error) {
	time.Sleep(s.delay)
	return s.s.Exists(key)
}

func (s *DelayStore) Put(key string, value []byte, options *store.WriteOptions) error {
	time.Sleep(s.delay)
	return s.s.Put(key, value, options)
}

func (s *DelayStore) Delete(key string) error {
	time.Sleep(s.delay)
	return s.s.Delete(key)
}

func (s *DelayStore) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	time.Sleep(s.delay)
	return s.s.AtomicPut(key, value, previous, options)
}

func (s *DelayStore) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	time.Sleep(s.delay)
	return s.s.AtomicDelete(key, previous)
}

func (s *DelayStore) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	time.Sleep(s.delay)
	return s.s.Watch(key, stopCh)
}

func (*DelayStore) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *DelayStore) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	time.Sleep(s.delay)
	return s.s.NewLock(key, options)
}

func (*DelayStore) List(directory string) ([]*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*DelayStore) DeleteTree(directory string) error {
	return store.ErrCallNotSupported
}

func (s *DelayStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(badgerkv.Lister)
	if !ok {
		return nil, store.ErrCallNotSupported
	}
	time.Sleep(s.delay)
	return lister.ListKeys(start)
}
