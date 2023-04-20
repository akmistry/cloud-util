package cloud

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type StoreFunc func(path string) (UnorderedStore, error)

var (
	ErrUnrecognisedScheme = errors.New("cloud: unrecognised store scheme")
	ErrInvalidFormat      = errors.New("cloud: invalid store path format")

	storeSchemeMap map[string]StoreFunc
	schemeLock     sync.Mutex
)

func RegisterStoreScheme(scheme string, fn StoreFunc) {
	schemeLock.Lock()
	defer schemeLock.Unlock()

	if storeSchemeMap == nil {
		storeSchemeMap = make(map[string]StoreFunc)
	}
	storeSchemeMap[scheme] = fn
}

func OpenUnorderedStore(path string) (UnorderedStore, error) {
	i := strings.IndexByte(path, ':')
	if i < 0 {
		return nil, ErrInvalidFormat
	}
	scheme := path[:i]

	schemeLock.Lock()
	defer schemeLock.Unlock()

	fn := storeSchemeMap[scheme]
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
