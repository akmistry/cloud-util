package store_util

import (
	"time"

	"github.com/akmistry/cloud-util"
)

type DelayStore struct {
	s     cloud.UnorderedStore
	delay time.Duration
}

var _ = (cloud.UnorderedStore)((*DelayStore)(nil))

func NewDelayStore(s cloud.UnorderedStore, delay time.Duration) *DelayStore {
	return &DelayStore{
		s:     s,
		delay: delay,
	}
}

func (s *DelayStore) Close() {
	cloud.DoStoreClose(s.s)
}

func (s *DelayStore) Get(key string) (*cloud.KVPair, error) {
	time.Sleep(s.delay)
	return s.s.Get(key)
}

func (s *DelayStore) Exists(key string) (bool, error) {
	time.Sleep(s.delay)
	return s.s.Exists(key)
}

func (s *DelayStore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	time.Sleep(s.delay)
	return s.s.Put(key, value, options)
}

func (s *DelayStore) Delete(key string) error {
	time.Sleep(s.delay)
	return s.s.Delete(key)
}

func (s *DelayStore) AtomicPut(key string, value []byte, previous *cloud.KVPair, options *cloud.WriteOptions) (bool, *cloud.KVPair, error) {
	if as, ok := s.s.(cloud.AtomicUnorderedStore); ok {
		time.Sleep(s.delay)
		return as.AtomicPut(key, value, previous, options)
	}
	return false, nil, cloud.ErrCallNotSupported
}

func (s *DelayStore) AtomicDelete(key string, previous *cloud.KVPair) (bool, error) {
	if as, ok := s.s.(cloud.AtomicUnorderedStore); ok {
		time.Sleep(s.delay)
		return as.AtomicDelete(key, previous)
	}
	return false, cloud.ErrCallNotSupported
}

func (s *DelayStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(cloud.OrderedStore)
	if !ok {
		return nil, cloud.ErrCallNotSupported
	}
	time.Sleep(s.delay)
	return lister.ListKeys(start)
}
