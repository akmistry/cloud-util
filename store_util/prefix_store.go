package store_util

import (
	"strings"

	"github.com/akmistry/cloud-util"
)

type PrefixStore struct {
	s      cloud.UnorderedStore
	prefix string
}

var _ = (cloud.UnorderedStore)((*PrefixStore)(nil))

func NewPrefixStore(s cloud.UnorderedStore, prefix string) *PrefixStore {
	return &PrefixStore{
		s:      s,
		prefix: prefix,
	}
}

func (s *PrefixStore) makeKey(key string) string {
	return s.prefix + key
}

func (s *PrefixStore) Get(key string) (*cloud.KVPair, error) {
	return s.s.Get(s.makeKey(key))
}

func (s *PrefixStore) Exists(key string) (bool, error) {
	return s.s.Exists(s.makeKey(key))
}

func (s *PrefixStore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	return s.s.Put(s.makeKey(key), value, options)
}

func (s *PrefixStore) Delete(key string) error {
	return s.s.Delete(s.makeKey(key))
}

func (s *PrefixStore) AtomicPut(key string, value []byte, previous *cloud.KVPair, options *cloud.WriteOptions) (bool, *cloud.KVPair, error) {
	if as, ok := s.s.(cloud.AtomicUnorderedStore); ok {
		return as.AtomicPut(s.makeKey(key), value, previous, options)
	}
	return false, nil, cloud.ErrCallNotSupported
}

func (s *PrefixStore) AtomicDelete(key string, previous *cloud.KVPair) (bool, error) {
	if as, ok := s.s.(cloud.AtomicUnorderedStore); ok {
		return as.AtomicDelete(s.makeKey(key), previous)
	}
	return false, cloud.ErrCallNotSupported
}

func (s *PrefixStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(cloud.OrderedStore)
	if !ok {
		return nil, cloud.ErrCallNotSupported
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
