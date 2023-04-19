package store_util

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/local"
	"github.com/akmistry/cloud-util/test_util"
)

func mapKeysKeysHelper(t *testing.T, s cloud.OrderedStore, expectedKeys []string, start, end string, workers int) {
	foundKeys := make(map[string]bool, len(expectedKeys))

	var lock sync.Mutex
	err := MapKeys(s, start, end, func(key string) {
		lock.Lock()
		defer lock.Unlock()

		if foundKeys[key] {
			t.Errorf("Duplicate key %s", key)
		}
		foundKeys[key] = true
	}, workers)
	if err != nil {
		t.Errorf("MapKeys err = %v", err)
	}

	t.Logf("Iterated %d/%d keys", len(foundKeys), len(expectedKeys))

	for _, k := range expectedKeys {
		if start != "" && k < start {
			if foundKeys[k] {
				t.Errorf("Found key %s before start %s", k, start)
			}
			continue
		} else if end != "" && k >= end {
			if foundKeys[k] {
				t.Errorf("Found key %s after end %s", k, end)
			}
			continue
		} else if !foundKeys[k] {
			t.Errorf("Key %s not interated", k)
		}
	}
}

func TestMapKeys_All(t *testing.T) {
	const NumTestItems = 1000

	s := local.NewInMemoryStore()
	sortedKeys, _ := test_util.PopulateTestItems(t, s, NumTestItems)

	mapKeysKeysHelper(t, s, sortedKeys, "", "", -1)
	mapKeysKeysHelper(t, s, sortedKeys, "", "", 0)
	mapKeysKeysHelper(t, s, sortedKeys, "", "", 1)
	mapKeysKeysHelper(t, s, sortedKeys, "", "", 2)
	mapKeysKeysHelper(t, s, sortedKeys, "", "", 3)
}

func mapKeysRangeHelper(t *testing.T, s cloud.OrderedStore, sortedKeys []string, workers int) {
	const Iterations = 10

	for i := 0; i < Iterations; i++ {
		startIndex := rand.Intn(len(sortedKeys))
		endOffset := rand.Intn(len(sortedKeys) - startIndex)

		startKey := sortedKeys[startIndex]
		endKey := sortedKeys[startIndex+endOffset]

		mapKeysKeysHelper(t, s, sortedKeys, startKey, endKey, workers)
	}
}

func TestMapKeys_Range(t *testing.T) {
	const NumTestItems = 1000

	s := local.NewInMemoryStore()
	sortedKeys, _ := test_util.PopulateTestItems(t, s, NumTestItems)

	mapKeysRangeHelper(t, s, sortedKeys, -1)
	mapKeysRangeHelper(t, s, sortedKeys, 0)
	mapKeysRangeHelper(t, s, sortedKeys, 1)
	mapKeysRangeHelper(t, s, sortedKeys, 2)
	mapKeysRangeHelper(t, s, sortedKeys, 3)
}
