package cloud

type PrefixBlobStore struct {
	bs     BlobStore
	prefix string
}

var _ = (BlobStore)((*PrefixBlobStore)(nil))

func NewPrefixBlobStore(bs BlobStore, prefix string) *PrefixBlobStore {
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

func (s *PrefixBlobStore) Get(key string) (GetReader, error) {
	return s.bs.Get(s.makeKey(key))
}

func (s *PrefixBlobStore) Put(key string) (PutWriter, error) {
	return s.bs.Put(s.makeKey(key))
}

func (s *PrefixBlobStore) Delete(key string) error {
	return s.bs.Delete(s.makeKey(key))
}
