package cache

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akmistry/cloud-util"
)

type StagedBlobUploader struct {
	pendingDir   string
	completedDir string
	backing      cloud.BlobStore
	pendingBlobs map[string]bool
	lock         sync.Mutex
}

func NewStagedBlobUploader(bs cloud.BlobStore, dir string) (*StagedBlobUploader, error) {
	pendingDir := filepath.Join(dir, "pending")
	completedDir := filepath.Join(dir, "pending")
	err := os.MkdirAll(pendingDir, 0755)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(completedDir, 0755)
	if err != nil {
		return nil, err
	}

	dirents, err := os.ReadDir(pendingDir)
	if err != nil {
		return nil, err
	}
	pendingBlobs := make(map[string]bool, len(dirents))
	for _, e := range dirents {
		if strings.HasPrefix(e.Name(), "blob-temp") {
			continue
		}
		pendingBlobs[e.Name()] = true
	}

	u := &StagedBlobUploader{
		pendingDir:   pendingDir,
		completedDir: completedDir,
		backing:      bs,
		pendingBlobs: pendingBlobs,
	}
	for key := range u.pendingBlobs {
		go u.doBlobUpload(key)
	}
	return u, nil
}

func (u *StagedBlobUploader) doBlobUpload(key string) {
	startTime := time.Now()
	defer func() {
		log.Printf("Uploaded %s in %s", key, time.Since(startTime))
	}()
	log.Printf("Uploading %s", key)

	pendingName := filepath.Join(u.pendingDir, key)
	f, err := os.Open(pendingName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w, err := u.backing.Put(key)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(w, f)
	if err != nil {
		w.Cancel()
		panic(err)
	}
	err = w.Close()
	if err != nil {
		w.Cancel()
		panic(err)
	}
	err = os.Rename(pendingName, filepath.Join(u.completedDir, key))
	if err != nil {
		panic(err)
	}
}

func (u *StagedBlobUploader) findStagedBlob(key string) string {
	completedName := filepath.Join(u.completedDir, key)
	_, err := os.Stat(completedName)
	if err == nil {
		return completedName
	}
	pendingName := filepath.Join(u.pendingDir, key)
	_, err = os.Stat(pendingName)
	if err == nil {
		return pendingName
	}
	return ""
}

func (u *StagedBlobUploader) Size(key string) (int64, error) {
	fname := u.findStagedBlob(key)
	if fname != "" {
		fi, err := os.Stat(fname)
		if err == nil {
			return fi.Size(), nil
		}
		log.Printf("Unable to stat %s: %v", fname, err)
		// Fallback
	}

	return u.backing.Size(key)
}

func (u *StagedBlobUploader) List() ([]string, error) {
	// Pass-through
	l, ok := u.backing.(cloud.Lister)
	if !ok {
		return nil, errors.New("List() unimplemented")
	}
	return l.List()
}

func (u *StagedBlobUploader) Delete(key string) error {
	fname := u.findStagedBlob(key)
	if fname != "" {
		err := os.Remove(fname)
		if err != nil {
			log.Printf("Unable to delete %s: %v", fname, err)
		}
	}

	return u.backing.Delete(key)
}

type fileReader struct {
	*os.File
	size int64
}

func (r *fileReader) Size() int64 {
	return r.size
}

func (u *StagedBlobUploader) Get(key string) (cloud.GetReader, error) {
	fname := u.findStagedBlob(key)
	if fname != "" {
		f, err := os.Open(fname)
		if err == nil {
			fi, err := f.Stat()
			if err == nil {
				return &fileReader{File: f, size: fi.Size()}, nil
			}
			log.Printf("Unable to stat for reading %s: %v", fname, err)
			f.Close()
		} else {
			log.Printf("Unable to open for reading %s: %v", fname, err)
		}
	}
	return u.backing.Get(key)
}

type pendingWriter struct {
	u   *StagedBlobUploader
	f   *os.File
	key string
}

func (w *pendingWriter) Write(b []byte) (int, error) {
	return w.f.Write(b)
}

func (w *pendingWriter) Close() (err error) {
	defer func() {
		if err != nil {
			removeErr := os.Remove(w.f.Name())
			if removeErr != nil {
				log.Printf("Unable to remove temp file %s on close: %v",
					w.f.Name(), removeErr)
			}
		}
	}()

	err = w.f.Close()
	if err != nil {
		return
	}

	completedName := filepath.Join(w.u.completedDir, w.key)
	err = os.Rename(w.f.Name(), completedName)
	if err != nil {
		return
	}
	go w.u.doBlobUpload(w.key)
	return
}

func (w *pendingWriter) Cancel() error {
	err := w.f.Close()
	if err != nil {
		log.Printf("Unable to close %s on cancel: %v", w.f.Name(), err)
	}
	err = os.Remove(w.f.Name())
	if err != nil {
		log.Printf("Unable to remove %s on cancel: %v", w.f.Name(), err)
	}
	return nil
}

func (u *StagedBlobUploader) Put(key string) (cloud.PutWriter, error) {
	// TODO: Check blob does not already exist
	f, err := os.CreateTemp(u.pendingDir, "blob-temp*")
	if err != nil {
		return nil, err
	}
	w := &pendingWriter{
		u:   u,
		f:   f,
		key: key,
	}
	return w, nil
}
