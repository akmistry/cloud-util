package cloud

import (
	"fmt"
	"strings"
)

type PrefixStore struct {
	s      UnorderedStore
	prefix string
}

var _ = (UnorderedStore)((*PrefixStore)(nil))

func NewPrefixStore(s UnorderedStore, prefix string) *PrefixStore {
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

func (s *PrefixStore) Get(key string) (*KVPair, error) {
	return s.s.Get(s.makeKey(key))
}

func (s *PrefixStore) Exists(key string) (bool, error) {
	return s.s.Exists(s.makeKey(key))
}

func (s *PrefixStore) Put(key string, value []byte, options *WriteOptions) error {
	return s.s.Put(s.makeKey(key), value, options)
}

func (s *PrefixStore) Delete(key string) error {
	return s.s.Delete(s.makeKey(key))
}

func (s *PrefixStore) AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		return as.AtomicPut(s.makeKey(key), value, previous, options)
	}
	return false, nil, ErrCallNotSupported
}

func (s *PrefixStore) AtomicDelete(key string, previous *KVPair) (bool, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		return as.AtomicDelete(s.makeKey(key), previous)
	}
	return false, ErrCallNotSupported
}

func (s *PrefixStore) ListKeys(start string) ([]string, error) {
	lister, ok := s.s.(OrderedStore)
	if !ok {
		return nil, ErrCallNotSupported
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
