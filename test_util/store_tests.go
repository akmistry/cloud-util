package test_util

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/akmistry/cloud-util"
)

const (
	NumTestItems = 1000
)

func checkExists(t *testing.T, s cloud.UnorderedStore, key string, value []byte) {
	t.Helper()

	exists, err := s.Exists(key)
	if err != nil {
		t.Errorf("Exists(%s) error = %v", key, err)
	} else if !exists {
		t.Errorf("Exists(%s) returned false", key)
	}

	pair, err := s.Get(key)
	if err != nil {
		t.Errorf("Get(%s) error = %v", key, err)
	} else if pair == nil {
		t.Errorf("Get(%s) returned nil", key)
	}
	if pair.Key != key {
		t.Errorf("Get(%s).Key %s != expected %s", key, pair.Key, key)
	}
	if !bytes.Equal(pair.Value, value) {
		t.Errorf("Get(%s).Value %v != expected %v", key, pair.Value, value)
	}
}

func checkNotExists(t *testing.T, s cloud.UnorderedStore, key string) {
	t.Helper()

	exists, err := s.Exists(key)
	if err != nil {
		t.Errorf("Exists(%s) error = %v", key, err)
	} else if exists {
		t.Errorf("Exists(%s) returned true", key)
	}

	pair, err := s.Get(key)
	if err != cloud.ErrKeyNotFound {
		t.Errorf("Get(%s) error %v != expected cloud.ErrKeyNotFound", key, err)
	}
	if pair != nil {
		t.Errorf("Get(%s) returned non-nil", key)
	}
}

func PopulateTestItems(t *testing.T, s cloud.UnorderedStore, count int) ([]string, map[string][]byte) {
	kv := make(map[string][]byte)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%020d", rand.Int())
		value := []byte(key)
		kv[key] = value

		err := s.Put(key, value, nil)
		if err != nil {
			t.Errorf("Put(%s) error = %v", key, err)
		}
	}
	sortedKeys := make([]string, 0, len(kv))
	for key := range kv {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	return sortedKeys, kv
}

func TestUnorderedStore(t *testing.T, s cloud.UnorderedStore) {
	// Generate and insert items
	sortedKeys, kv := PopulateTestItems(t, s, NumTestItems)

	// Check for existence and get
	for key, value := range kv {
		checkExists(t, s, key, value)
	}

	// Erase the first (in sorted order) 50% of elements
	for i := 0; i < (len(sortedKeys) / 2); i++ {
		err := s.Delete(sortedKeys[i])
		if err != nil {
			t.Errorf("Delete(%s) error = %v", sortedKeys[i], err)
		}
		delete(kv, sortedKeys[i])
	}

	// Check for existence and get
	for i := 0; i < len(sortedKeys); i++ {
		if i < (len(sortedKeys) / 2) {
			checkNotExists(t, s, sortedKeys[i])
		} else {
			checkExists(t, s, sortedKeys[i], []byte(sortedKeys[i]))
		}
	}
}

func TestListKeys(t *testing.T, s cloud.OrderedStore) {
	const ListIterations = 1000

	// Generate and insert items
	sortedKeys, _ := PopulateTestItems(t, s, NumTestItems)

	for i := 0; i < ListIterations; i++ {
		startIndex := rand.Intn(len(sortedKeys))
		startKey := sortedKeys[startIndex]
		keys, err := s.ListKeys(startKey)
		if err != nil {
			t.Errorf("ListKeys(%s) error = %v", startKey, err)
		}
		if startIndex == (len(sortedKeys) - 1) {
			if len(keys) != 1 {
				t.Errorf("ListKeys(%s) len(keys) %d != expected 1", startKey, len(keys))
			}
		} else if len(keys) < 2 {
			t.Errorf("ListKeys(%s) len(keys) %d < expected 2 or more", startKey, len(keys))
		}

		for ki, k := range keys {
			if k != sortedKeys[startIndex+ki] {
				t.Errorf("key %s != sortedKeys[startIndex+ki] %s", k, sortedKeys[startIndex+ki])
			}
		}
	}
}
