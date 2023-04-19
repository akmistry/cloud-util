package local

import (
	"testing"

	"github.com/akmistry/cloud-util/test_util"
)

func TestInMemoryStore(t *testing.T) {
	s := NewInMemoryStore()
	test_util.TestUnorderedStore(t, s)

	// Test assumes stores are empty to start
	s = NewInMemoryStore()
	test_util.TestListKeys(t, s)
}
