package local

import (
	"os"
	"path/filepath"

	"github.com/akmistry/cloud-util"
)

type DirBlobStore struct {
	dir string
}

var _ = (cloud.BlobStore)((*DirBlobStore)(nil))

func NewDirBlobStore(dir string) (*DirBlobStore, error) {
	err := os.MkdirAll(dir, 0750)
	if err != nil {
		return nil, err
	}

	return &DirBlobStore{
		dir: dir,
	}, nil
}

func (s *DirBlobStore) makeFilePath(key string) string {
	return filepath.Join(s.dir, key)
}

func (s *DirBlobStore) Size(key string) (int64, error) {
	path := s.makeFilePath(key)
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type fileBlobReader struct {
	*os.File
	size int64
}

func (r *fileBlobReader) Size() int64 {
	return r.size
}

func (s *DirBlobStore) Get(key string) (cloud.GetReader, error) {
	path := s.makeFilePath(key)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &fileBlobReader{
		File: f,
		size: fi.Size(),
	}, nil

}

type fileBlobWriter struct {
	*os.File
	path string
}

func (w *fileBlobWriter) Close() error {
	err := w.File.Close()
	if err == nil {
		w.File = nil
	}
	return err
}

func (w *fileBlobWriter) Cancel() error {
	if w.File == nil {
		return nil
	}
	err := w.File.Close()
	if err != nil {
		return err
	}
	return os.Remove(w.path)
}

func (s *DirBlobStore) Put(key string) (cloud.PutWriter, error) {
	// TODO: Make semantics atomic
	path := s.makeFilePath(key)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	return &fileBlobWriter{
		File: f,
		path: path,
	}, nil
}

func (s *DirBlobStore) Delete(key string) error {
	path := s.makeFilePath(key)
	return os.Remove(path)
}
