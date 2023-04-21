package cloud

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type StoreFunc func(path string) (UnorderedStore, error)
type BlobStoreFunc func(path string) (BlobStore, error)

var (
	ErrUnrecognisedScheme = errors.New("cloud: unrecognised store scheme")
	ErrInvalidFormat      = errors.New("cloud: invalid store path format")

	storeSchemeMap     = make(map[string]StoreFunc)
	blobStoreSchemeMap = make(map[string]BlobStoreFunc)
	schemeLock         sync.Mutex
)

func RegisterStoreScheme(scheme string, fn StoreFunc) {
	schemeLock.Lock()
	storeSchemeMap[scheme] = fn
	schemeLock.Unlock()
}

func RegisterBlobStoreScheme(scheme string, fn BlobStoreFunc) {
	schemeLock.Lock()
	blobStoreSchemeMap[scheme] = fn
	schemeLock.Unlock()
}

func OpenUnorderedStore(path string) (UnorderedStore, error) {
	i := strings.IndexByte(path, ':')
	if i < 0 {
		return nil, ErrInvalidFormat
	}
	scheme := path[:i]

	schemeLock.Lock()
	fn := storeSchemeMap[scheme]
	schemeLock.Unlock()

	if fn == nil {
		return nil, ErrUnrecognisedScheme
	}
	return fn(path)
}

func OpenAtomicOrderedStore(path string) (AtomicOrderedStore, error) {
	us, err := OpenUnorderedStore(path)
	if err != nil {
		return nil, err
	}

	aos, ok := us.(AtomicOrderedStore)
	if !ok {
		return nil, fmt.Errorf("cloud: '%s' does not implement AtomicOrderedStore", path)
	}
	return aos, nil
}

func OpenBlobStore(path string) (BlobStore, error) {
	i := strings.IndexByte(path, ':')
	if i < 0 {
		return nil, ErrInvalidFormat
	}
	scheme := path[:i]

	schemeLock.Lock()
	fn := blobStoreSchemeMap[scheme]
	schemeLock.Unlock()

	if fn == nil {
		return nil, ErrUnrecognisedScheme
	}
	return fn(path)
}
