package cloud

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"

	"github.com/docker/libkv/store"
)

type ScrambleFunc func(string) string

type ScrambledKeyStore struct {
	s       store.Store
	keyFunc ScrambleFunc
}

var _ = (store.Store)((*ScrambledKeyStore)(nil))

func NewScrambledKeyStore(s store.Store, keyFunc ScrambleFunc) *ScrambledKeyStore {
	return &ScrambledKeyStore{
		s:       s,
		keyFunc: keyFunc,
	}
}

func NewHMacSha256KeyStore(s store.Store, salt []byte) *ScrambledKeyStore {
	saltCopy := append([]byte(nil), salt...)
	keyFunc := func(key string) string {
		mac := hmac.New(sha256.New, saltCopy)
		_, err := mac.Write([]byte(key))
		if err != nil {
			// Don't expect this to happen.
			panic(err)
		}
		var sum [sha256.Size]byte
		return base64.RawURLEncoding.EncodeToString(mac.Sum(sum[:0]))
	}
	return NewScrambledKeyStore(s, keyFunc)
}

func (s *ScrambledKeyStore) makeKey(key string) string {
	return s.keyFunc(key)
}

func (s *ScrambledKeyStore) Close() {
	s.s.Close()
}

func (s *ScrambledKeyStore) Get(key string) (*store.KVPair, error) {
	return s.s.Get(s.makeKey(key))
}

func (s *ScrambledKeyStore) Exists(key string) (bool, error) {
	return s.s.Exists(s.makeKey(key))
}

func (s *ScrambledKeyStore) Put(key string, value []byte, options *store.WriteOptions) error {
	return s.s.Put(s.makeKey(key), value, options)
}

func (s *ScrambledKeyStore) Delete(key string) error {
	return s.s.Delete(s.makeKey(key))
}

func (s *ScrambledKeyStore) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return s.s.AtomicPut(s.makeKey(key), value, previous, options)
}

func (s *ScrambledKeyStore) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return s.s.AtomicDelete(s.makeKey(key), previous)
}

func (s *ScrambledKeyStore) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return s.s.Watch(s.makeKey(key), stopCh)
}

func (*ScrambledKeyStore) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *ScrambledKeyStore) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return s.s.NewLock(s.makeKey(key), options)
}

func (*ScrambledKeyStore) List(directory string) ([]*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (*ScrambledKeyStore) DeleteTree(directory string) error {
	return store.ErrCallNotSupported
}

func (s *ScrambledKeyStore) ListKeys(start string) ([]string, error) {
	// Doesn't make sense if keys are scrambled
	return nil, store.ErrCallNotSupported
}
