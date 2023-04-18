package cloud

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

type ScrambleFunc func(string) string

type ScrambledKeyStore struct {
	s       UnorderedStore
	keyFunc ScrambleFunc
}

var _ = (UnorderedStore)((*ScrambledKeyStore)(nil))

func NewScrambledKeyStore(s UnorderedStore, keyFunc ScrambleFunc) *ScrambledKeyStore {
	return &ScrambledKeyStore{
		s:       s,
		keyFunc: keyFunc,
	}
}

func NewHMacSha256KeyStore(s UnorderedStore, salt []byte) *ScrambledKeyStore {
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

func (s *ScrambledKeyStore) Get(key string) (*KVPair, error) {
	return s.s.Get(s.makeKey(key))
}

func (s *ScrambledKeyStore) Exists(key string) (bool, error) {
	return s.s.Exists(s.makeKey(key))
}

func (s *ScrambledKeyStore) Put(key string, value []byte, options *WriteOptions) error {
	return s.s.Put(s.makeKey(key), value, options)
}

func (s *ScrambledKeyStore) Delete(key string) error {
	return s.s.Delete(s.makeKey(key))
}

func (s *ScrambledKeyStore) AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		return as.AtomicPut(s.makeKey(key), value, previous, options)
	}
	return false, nil, ErrCallNotSupported
}

func (s *ScrambledKeyStore) AtomicDelete(key string, previous *KVPair) (bool, error) {
	if as, ok := s.s.(AtomicUnorderedStore); ok {
		return as.AtomicDelete(s.makeKey(key), previous)
	}
	return false, ErrCallNotSupported
}
