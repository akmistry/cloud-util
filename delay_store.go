package cloud

import (
	"time"
)

type DelayStore struct {
	s     UnorderedStore
	delay time.Duration
}

var _ = (UnorderedStore)((*DelayStore)(nil))

func NewDelayStore(s UnorderedStore, delay time.Duration) *DelayStore {
	return &DelayStore{
		s:     s,
		delay: delay,
	}
}

func (s *DelayStore) Close() {
	DoStoreClose(s.s)
}

func (s *DelayStore) Get(key string) (*KVPair, error) {
	time.Sleep(s.delay)
	return s.s.Get(key)
}

func (s *DelayStore) Exists(key string) (bool, error) {
	time.Sleep(s.delay)
	return s.s.Exists(key)
}

func (s *DelayStore) Put(key string, value []byte, options *WriteOptions) error {
	time.Sleep(s.delay)
	return s.s.Put(key, value, options)
}

func (s *DelayStore) Delete(key string) error {
	time.Sleep(s.delay)
	return s.s.Delete(key)
}

func (s *DelayStore) AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		time.Sleep(s.delay)
		return as.AtomicPut(key, value, previous, options)
	}
	return false, nil, ErrCallNotSupported
}

func (s *DelayStore) AtomicDelete(key string, previous *KVPair) (bool, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		time.Sleep(s.delay)
		return as.AtomicDelete(key, previous)
	}
	return false, ErrCallNotSupported
}

func (s *DelayStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(OrderedStore)
	if !ok {
		return nil, ErrCallNotSupported
	}
	time.Sleep(s.delay)
	return lister.ListKeys(start)
}
