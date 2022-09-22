package gcp

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"os"

	"cloud.google.com/go/storage"
	prom "github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
)

const (
	maxPendingFetches = 512
	contentSizeKey    = "cloudutil-content-size"
)

type GcsStore struct {
	client       *storage.Client
	bucketHandle *storage.BucketHandle
	bucket       string
	pendingSema  *semaphore.Weighted

	requestsCounter *prom.CounterVec
}

func NewGcsStore(bucket string) *GcsStore {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}

	rc := prom.NewCounterVec(prom.CounterOpts{
		Name:        "gcp_storage_requests_total",
		Help:        "Number of GCP cloud storage requests",
		ConstLabels: prom.Labels{"bucket": bucket},
	}, []string{"method"})
	prom.MustRegister(rc)

	return &GcsStore{
		client:          client,
		bucketHandle:    client.Bucket(bucket),
		bucket:          bucket,
		pendingSema:     semaphore.NewWeighted(maxPendingFetches),
		requestsCounter: rc,
	}
}

func (s *GcsStore) Size(key string) (int64, error) {
	s.requestsCounter.WithLabelValues("size").Add(1)

	s.pendingSema.Acquire(context.Background(), 1)
	defer s.pendingSema.Release(1)

	obj := s.bucketHandle.Object(key)
	attrs, err := obj.Attrs(context.TODO())
	if err == storage.ErrObjectNotExist {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}

	return attrs.Size, nil
}

type getReader struct {
	s    *GcsStore
	key  string
	size int64
}

func createRange(off, len int64) string {
	return fmt.Sprintf("bytes=%d-%d", off, off+len-1)
}

func (r *getReader) Size() int64 {
	return r.size
}

func (r *getReader) ReadAt(b []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	r.s.requestsCounter.WithLabelValues("get").Add(1)

	r.s.pendingSema.Acquire(context.Background(), 1)
	defer r.s.pendingSema.Release(1)

	obj := r.s.bucketHandle.Object(r.key)
	reader, err := obj.NewRangeReader(context.TODO(), off, int64(len(b)))
	if err == storage.ErrObjectNotExist {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}

	defer reader.Close()
	n := 0
	for n < len(b) {
		bytesRead, err := reader.Read(b[n:])
		n += bytesRead
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s *GcsStore) Get(key string) (cloud.GetReader, error) {
	size, err := s.Size(key)
	if err != nil {
		return nil, err
	}
	r := &getReader{s: s, key: key, size: size}
	return r, nil
}

type putWriter struct {
	name   string
	s      *GcsStore
	w      *storage.Writer
	closed bool
	size   int64
	hasher hash.Hash
	err    error
}

func (w *putWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	w.hasher.Write(b)

	n, err := w.w.Write(b)
	if err != nil {
		w.err = fmt.Errorf("error writing %d bytes: %v", len(b), err)
	}
	w.size += int64(n)
	return n, err
}

func (w *putWriter) Close() error {
	if w.closed {
		return w.err
	}

	if w.err != nil {
		w.Cancel()
		if w.err == nil {
			panic("unexpected w.err == nil")
		}
		return w.err
	}
	w.closed = true

	w.s.pendingSema.Release(1)
	err := w.w.Close()
	if err != nil {
		w.err = fmt.Errorf("GCS close error: %v", err)
		return w.err
	}

	// Verify size and md5 hash to make sure blob was written correctly.
	attrs, err := w.s.bucketHandle.Object(w.name).Attrs(context.TODO())
	if err != nil {
		w.s.Delete(w.name)
		w.err = fmt.Errorf("GCS put validation error: %v", err)
		return w.err
	} else if attrs.Size != w.size {
		w.s.Delete(w.name)
		w.err = fmt.Errorf("GCS put size %d != blob size %d", w.size, attrs.Size)
		return w.err
	} else if bytes.Compare(attrs.MD5, w.hasher.Sum(nil)) != 0 {
		w.s.Delete(w.name)
		w.err = fmt.Errorf("GCS put hash %x != blob hash %x", w.hasher.Sum(nil), attrs.MD5)
		return w.err
	}

	// TODO: Other validation?
	return nil
}

func (w *putWriter) Cancel() error {
	if w.closed {
		return w.err
	}
	w.closed = true

	w.s.pendingSema.Release(1)
	// TODO: ErrCanceled?
	w.w.CloseWithError(io.ErrClosedPipe)
	w.w = nil
	w.err = fmt.Errorf("GCP blob put canceled after write error: %v", w.err)

	// TODO: Wait for request completion, and delete the object if success.
	return nil
}

func (s *GcsStore) Put(key string) (cloud.PutWriter, error) {
	s.requestsCounter.WithLabelValues("put").Add(1)

	s.pendingSema.Acquire(context.Background(), 1)

	obj := s.bucketHandle.Object(key).If(storage.Conditions{DoesNotExist: true})
	writer := obj.NewWriter(context.TODO())
	writer.ChunkSize = 0
	writer.ContentType = "application/octet-stream"
	writer.CacheControl = "no-transform"

	return &putWriter{name: key, s: s, w: writer, hasher: md5.New()}, nil
}

func (s *GcsStore) Delete(key string) error {
	s.requestsCounter.WithLabelValues("delete").Add(1)

	s.pendingSema.Acquire(context.Background(), 1)
	defer s.pendingSema.Release(1)

	obj := s.bucketHandle.Object(key)
	err := obj.Delete(context.TODO())
	if err == storage.ErrObjectNotExist {
		return nil
	}

	return err
}
