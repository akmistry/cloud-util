package local

import (
	"bytes"
	"strings"
	"sync"

	"github.com/google/btree"

	"github.com/akmistry/cloud-util"
)

type memItem struct {
	key   string
	value []byte
}

func memItemLessFunc(a, b *memItem) bool {
	return strings.Compare(a.key, b.key) < 0
}

type InMemoryStore struct {
	t    *btree.BTreeG[*memItem]
	lock sync.Mutex
}

var _ = (cloud.OrderedStore)((*InMemoryStore)(nil))
var _ = (cloud.AtomicUnorderedStore)((*InMemoryStore)(nil))

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		t: btree.NewG(4, memItemLessFunc),
	}
}

func (s *InMemoryStore) Get(key string) (*cloud.KVPair, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: key,
	}
	item, ok := s.t.Get(keyItem)
	if !ok {
		return nil, cloud.ErrKeyNotFound
	}
	return &cloud.KVPair{
		Key:   key,
		Value: bytes.Clone(item.value),
	}, nil
}

func (s *InMemoryStore) Exists(key string) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: key,
	}
	return s.t.Has(keyItem), nil
}

func (s *InMemoryStore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	insertItem := &memItem{
		key:   key,
		value: bytes.Clone(value),
	}
	s.t.ReplaceOrInsert(insertItem)
	return nil
}

func (s *InMemoryStore) Delete(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: key,
	}
	s.t.Delete(keyItem)
	return nil
}

func (s *InMemoryStore) AtomicPut(key string, value []byte, previous *cloud.KVPair, options *cloud.WriteOptions) (bool, *cloud.KVPair, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: key,
	}
	item, ok := s.t.Get(keyItem)
	if !ok {
		if previous != nil {
			return false, nil, cloud.ErrKeyNotFound
		}
	} else if previous == nil {
		return false, nil, cloud.ErrKeyExists
	} else if !bytes.Equal(item.value, previous.Value) {
		return false, nil, cloud.ErrKeyModified
	}

	insertItem := &memItem{
		key:   key,
		value: bytes.Clone(value),
	}
	s.t.ReplaceOrInsert(insertItem)

	updated := &cloud.KVPair{
		Key:   key,
		Value: value,
	}
	return true, updated, nil
}

func (s *InMemoryStore) AtomicDelete(key string, previous *cloud.KVPair) (bool, error) {
	if previous == nil {
		return false, cloud.ErrPreviousNotSpecified
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: key,
	}
	item, ok := s.t.Get(keyItem)
	if !ok {
		return false, cloud.ErrKeyNotFound
	} else if !bytes.Equal(item.value, previous.Value) {
		return false, cloud.ErrKeyModified
	}

	s.t.Delete(keyItem)
	return true, nil
}

func (s *InMemoryStore) ListKeys(start string) ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyItem := &memItem{
		key: start,
	}

	var keys []string
	s.t.AscendGreaterOrEqual(keyItem, func(item *memItem) bool {
		keys = append(keys, item.key)
		return len(keys) < 16
	})
	return keys, nil
}
