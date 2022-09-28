package cache

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
)

const (
	tempPrefix      = "temp-"
	pendingPrefix   = "pending-"
	completedPrefix = "completed-"

	maxActiveUploads = 2
)

type StagedBlobUploader struct {
	dir          string
	backing      cloud.BlobStore
	pendingBlobs map[string]bool
	lock         sync.Mutex

	activeUploads *semaphore.Weighted
}

func NewStagedBlobUploader(bs cloud.BlobStore, dir string) (*StagedBlobUploader, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	dirents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	pendingBlobs := make(map[string]bool, len(dirents))
	for _, e := range dirents {
		if strings.HasPrefix(e.Name(), tempPrefix) {
			log.Printf("Deleting temp file %s", e.Name())
			err = os.Remove(filepath.Join(dir, e.Name()))
			if err != nil {
				log.Printf("Error removing temp file %s: %v", e.Name(), err)
			}
			continue
		} else if !strings.HasPrefix(e.Name(), pendingPrefix) {
			continue
		}
		key := strings.TrimPrefix(e.Name(), pendingPrefix)
		pendingBlobs[key] = true
	}

	u := &StagedBlobUploader{
		dir:           dir,
		backing:       bs,
		pendingBlobs:  pendingBlobs,
		activeUploads: semaphore.NewWeighted(maxActiveUploads),
	}
	for key := range u.pendingBlobs {
		go u.doBlobUpload(key)
	}
	return u, nil
}

func (u *StagedBlobUploader) makePendingName(key string) string {
	return filepath.Join(u.dir, pendingPrefix+key)
}

func (u *StagedBlobUploader) makeCompletedName(key string) string {
	return filepath.Join(u.dir, completedPrefix+key)
}

func (u *StagedBlobUploader) doBlobUpload(key string) {
	u.activeUploads.Acquire(context.Background(), 1)
	defer u.activeUploads.Release(1)

	startTime := time.Now()
	defer func() {
		log.Printf("Uploaded %s in %s", key, time.Since(startTime))
	}()
	log.Printf("Uploading %s", key)

	backoffs := 0
	retry := func(err error) {
		u.activeUploads.Release(1)
		if backoffs > 8 {
			backoffs = 8
		}
		retryTime := time.Duration(rand.Int63n(int64(time.Second << backoffs)))
		backoffs++
		log.Printf("Upload of %s failed with error %v, retyring after %s", key, err, retryTime)
		time.Sleep(retryTime)
		u.activeUploads.Acquire(context.Background(), 1)
	}

	for {
		pendingName := u.makePendingName(key)
		f, err := os.Open(pendingName)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			panic(err)
		}

		size, err := u.backing.Size(key)
		if err == nil {
			if size == fi.Size() {
				err = os.Rename(pendingName, u.makeCompletedName(key))
				if err != nil {
					panic(err)
				}
				return
			}
			log.Printf("Uploaded blob %s size %d != pending size %d", key, size, fi.Size())
			log.Print("Deleting blob and re-uploading")
			err = u.backing.Delete(key)
			if err != nil {
				retry(err)
				continue
			}
		} else if err != os.ErrNotExist {
			retry(err)
			continue
		}

		w, err := u.backing.Put(key)
		if err != nil {
			retry(err)
			continue
		}
		_, err = io.Copy(w, f)
		if err != nil {
			w.Cancel()
			retry(err)
			continue
		}
		err = w.Close()
		if err != nil {
			retry(err)
			continue
		}
		err = os.Rename(pendingName, u.makeCompletedName(key))
		if err != nil {
			panic(err)
		}
		return
	}
}

func (u *StagedBlobUploader) findStagedBlob(key string) string {
	completedName := u.makeCompletedName(key)
	_, err := os.Stat(completedName)
	if err == nil {
		return completedName
	}
	pendingName := u.makePendingName(key)
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
	list, err := l.List()
	if err != nil {
		return list, err
	}

	dirents, err := os.ReadDir(u.dir)
	if err != nil {
		return nil, err
	}
	for _, e := range dirents {
		var key string
		if strings.HasPrefix(e.Name(), pendingPrefix) {
			key = strings.TrimPrefix(e.Name(), pendingPrefix)
		} else if strings.HasPrefix(e.Name(), completedPrefix) {
			key = strings.TrimPrefix(e.Name(), completedPrefix)
		} else {
			continue
		}

		hasKey := false
		for _, k := range list {
			if k == key {
				hasKey = true
				break
			}
		}
		if hasKey {
			continue
		}
		list = append(list, key)
	}
	return list, nil
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

	err = os.Rename(w.f.Name(), w.u.makePendingName(w.key))
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
	f, err := os.CreateTemp(u.dir, tempPrefix+"*")
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
