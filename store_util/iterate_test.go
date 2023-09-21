package store_util

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/local"
	"github.com/akmistry/cloud-util/test_util"
)

func iterateKeysKeysHelper(t *testing.T, s cloud.OrderedStore, expectedKeys []string, start, end string) {
	t.Helper()

	count := 0
	err := IterateKeys(s, start, func(key string) bool {
		if end != "" && strings.Compare(key, end) > 0 {
			return false
		}
		if count >= len(expectedKeys) {
			t.Errorf("Extra key %s", key)
			return false
		}
		if key != expectedKeys[count] {
			t.Errorf("Key %s != expected %s", key, expectedKeys[count])
			return false
		}
		count++
		return true
	})
	if err != nil {
		t.Errorf("IterateKeys err = %v", err)
	}

	t.Logf("Iterated %d/%d keys", count, len(expectedKeys))
}

func TestIterateKeys_All(t *testing.T) {
	const NumTestItems = 1000

	s := local.NewInMemoryStore()
	sortedKeys, _ := test_util.PopulateTestItems(t, s, NumTestItems)

	iterateKeysKeysHelper(t, s, sortedKeys, "", "")
}

func TestIterateKeys_Range(t *testing.T) {
	const NumTestItems = 1000
	const Iterations = 10

	s := local.NewInMemoryStore()
	sortedKeys, _ := test_util.PopulateTestItems(t, s, NumTestItems)

	for i := 0; i < Iterations; i++ {
		startIndex := rand.Intn(len(sortedKeys))
		endOffset := rand.Intn(len(sortedKeys) - startIndex)

		startKey := sortedKeys[startIndex]
		endKey := sortedKeys[startIndex+endOffset]

		iterateKeysKeysHelper(t, s, sortedKeys[startIndex:startIndex+endOffset+1], startKey, endKey)
	}
}
