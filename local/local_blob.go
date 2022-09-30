package local

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/akmistry/cloud-util"
)

const (
	// Use distinct prefixes for temp and actual blob files, so that old temp
	// files can be clean up.
	tempFilePrefix = "temp-"
	blobPrefix     = "blob-"
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

	// Erase temp files
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if path == dir {
			return nil
		} else if d.IsDir() {
			return fs.SkipDir
		} else if err != nil {
			log.Printf("Error walking path %s: %v", path, err)
			return nil
		}

		if !strings.HasPrefix(d.Name(), tempFilePrefix) {
			return nil
		}

		err = os.Remove(path)
		if err != nil {
			// Non-fatal, since unreferenced temp files only consume space and don't
			// affect semantics.
			log.Printf("Error removing old temp file %s: %v", path, err)
		}
		return nil
	})

	return &DirBlobStore{
		dir: dir,
	}, nil
}

func (s *DirBlobStore) makeFilePath(key string) string {
	// TODO: Consider encoding key to avoid issues with keys containing path
	// elements such as '/', '.', and '..'
	return filepath.Join(s.dir, blobPrefix+key)
}

func (s *DirBlobStore) Size(key string) (int64, error) {
	path := s.makeFilePath(key)
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, os.ErrNotExist
	} else if err != nil {
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
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	} else if err != nil {
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
	defer os.Remove(w.File.Name())

	err := w.File.Close()
	if err != nil {
		return err
	}
	err = os.Rename(w.File.Name(), w.path)
	if err != nil {
		return err
	}
	w.File = nil
	return nil
}

func (w *fileBlobWriter) Cancel() error {
	if w.File == nil {
		// Writer has been closed successfully
		return nil
	}
	err := w.File.Close()
	if err != nil {
		return err
	}
	return os.Remove(w.File.Name())
}

func (s *DirBlobStore) Put(key string) (cloud.PutWriter, error) {
	f, err := os.CreateTemp(s.dir, tempFilePrefix+"*")
	if err != nil {
		return nil, err
	}

	return &fileBlobWriter{
		File: f,
		path: s.makeFilePath(key),
	}, nil
}

func (s *DirBlobStore) Delete(key string) error {
	path := s.makeFilePath(key)
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	return err
}

func (s *DirBlobStore) List() ([]string, error) {
	dirents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	entries := make([]string, 0, len(dirents))
	for _, d := range dirents {
		if !strings.HasPrefix(d.Name(), blobPrefix) {
			continue
		}
		entries = append(entries, strings.TrimPrefix(d.Name(), blobPrefix))
	}
	return entries, nil
}
