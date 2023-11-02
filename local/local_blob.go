package local

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/akmistry/cloud-util"
)

const (
	// Use distinct prefixes for temp and actual blob files, so that old temp
	// files can be clean up.
	tempFilePrefix = "t-"
	blobPrefix     = "b-"

	dirScheme = "file"
)

func init() {
	cloud.RegisterBlobStoreScheme(dirScheme, openDirBlobStore)
}

// URL format: file://<dir>[/], where dir is either an absolute or relative path.
// Example:
//
//	file:///home/myhome/mydir -> /home/myhome/mydir (note the 3 /'s)
//	file://mydir -> ./mydir (relative to current directory)
func openDirBlobStore(path string) (cloud.BlobStore, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	p := u.EscapedPath()
	if u.Scheme != dirScheme {
		return nil, fmt.Errorf("cloud/local: unexpected scheme: %s", u.Scheme)
	} else if p[0] != '/' {
		return nil, fmt.Errorf("cloud/local: invalid path: %s", p)
	}

	name := p
	if u.Host != "" {
		name = filepath.Join(u.Host, name)
	}

	return NewDirBlobStore(name)
}

type DirBlobStore struct {
	dir string
}

var _ = (cloud.BlobStore)((*DirBlobStore)(nil))

func NewDirBlobStore(dir string) (*DirBlobStore, error) {
	err := os.MkdirAll(dir, 0750)
	if err != nil {
		return nil, err
	}

	// Intentionally don't delete any existsing temp files. They could be created
	// by another process sharing the same store.

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
	if errors.Is(err, fs.ErrNotExist) {
		return 0, fs.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type fileBlobReader struct {
	*os.File
	size int64

	mmaps [][]byte
}

func (r *fileBlobReader) Close() error {
	for _, m := range r.mmaps {
		err := unix.Munmap(m)
		if err != nil {
			log.Printf("cloud/local: Munmap error: %v", err)
		}
	}
	r.mmaps = nil
	return r.File.Close()
}

func (r *fileBlobReader) Size() int64 {
	return r.size
}

func (r *fileBlobReader) MemMap(offset int64, length int) ([]byte, error) {
	mmap, err := unix.Mmap(int(r.File.Fd()), offset, length, unix.PROT_READ, unix.MAP_SHARED)
	if len(mmap) > 0 {
		r.mmaps = append(r.mmaps, mmap)
	}
	return mmap, err
}

func (s *DirBlobStore) Get(key string) (cloud.GetReader, error) {
	path := s.makeFilePath(key)
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fs.ErrNotExist
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

	err := w.File.Sync()
	if err != nil {
		// Close the file on sync error to avoid an FD leak
		w.File.Close()
		return err
	}
	err = w.File.Close()
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
	if errors.Is(err, fs.ErrNotExist) {
		return fs.ErrNotExist
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
