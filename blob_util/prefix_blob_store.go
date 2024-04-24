package blob_util

import (
	"github.com/akmistry/cloud-util"
)

type PrefixBlobStore struct {
	bs     cloud.BlobStore
	prefix string
}

var _ = (cloud.BlobStore)((*PrefixBlobStore)(nil))

func NewPrefixBlobStore(bs cloud.BlobStore, prefix string) *PrefixBlobStore {
	return &PrefixBlobStore{
		bs:     bs,
		prefix: prefix,
	}
}

func (s *PrefixBlobStore) makeKey(key string) string {
	return s.prefix + key
}

func (s *PrefixBlobStore) Size(key string) (int64, error) {
	return s.bs.Size(s.makeKey(key))
}

func (s *PrefixBlobStore) Get(key string) (cloud.GetReader, error) {
	return s.bs.Get(s.makeKey(key))
}

func (s *PrefixBlobStore) Put(key string) (cloud.PutWriter, error) {
	return s.bs.Put(s.makeKey(key))
}

func (s *PrefixBlobStore) Delete(key string) error {
	return s.bs.Delete(s.makeKey(key))
}
