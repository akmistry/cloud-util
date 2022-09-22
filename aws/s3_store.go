package aws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go"
	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
)

const maxPendingFetches = 512

type S3Store struct {
	client      *minio.Client
	bucket      string
	pendingSema *semaphore.Weighted
}

func NewDefaultS3Store(bucket string) *S3Store {
	return NewS3Store(*Endpoint, *AccessId, *AccessSecret, *Region, bucket)
}

func NewS3Store(endpoint, accessKey, secret, region, bucket string) *S3Store {
	client, err := minio.New(endpoint, accessKey, secret, false)
	if err != nil {
		panic(err)
	}
	client.SetCustomTransport(&http.Transport{
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	})
	return &S3Store{
		client:      client,
		bucket:      bucket,
		pendingSema: semaphore.NewWeighted(maxPendingFetches),
	}
}

func (s *S3Store) Size(hash string) (int64, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	defer s.pendingSema.Release(1)

	info, err := s.client.StatObject(s.bucket, hash)
	if errResp, ok := err.(minio.ErrorResponse); ok && errResp.Code == "NoSuchKey" {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	return info.Size, nil
}

type getReader struct {
	s    *S3Store
	hash string
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

	r.s.pendingSema.Acquire(context.Background(), 1)
	defer r.s.pendingSema.Release(1)

	obj, err := r.s.client.GetObject(r.s.bucket, r.hash)
	if errResp, ok := err.(minio.ErrorResponse); ok && errResp.Code == "NoSuchKey" {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	defer obj.Close()
	return obj.ReadAt(b, off)
}

func (s *S3Store) Get(hash string) (cloud.GetReader, error) {
	size, err := s.Size(hash)
	if err != nil {
		return nil, err
	}
	r := &getReader{s: s, hash: hash, size: size}
	return r, nil
}

type putWriter struct {
	s      *S3Store
	pw     *io.PipeWriter
	result chan result
	closed bool
}

func (w *putWriter) Write(b []byte) (int, error) {
	n, err := w.pw.Write(b)
	if err != nil {
		err = fmt.Errorf("error writing %d bytes: %v", len(b), err)
	}
	return n, err
}

func (w *putWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.pw.Close()

	res := <-w.result
	if res.err != nil {
		return res.err
	}
	return nil
}

func (w *putWriter) Cancel() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// TODO: ErrCanceled?
	w.pw.CloseWithError(io.ErrClosedPipe)
	w.pw = nil

	// TODO: Wait for request completion, and delete the object if success.
	return nil
}

type result struct {
	n   int64
	err error
}

func (s *S3Store) Put(hash string) (cloud.PutWriter, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	r, pw := io.Pipe()
	writer := &putWriter{s: s, pw: pw, result: make(chan result, 1)}
	go func() {
		defer close(writer.result)
		defer s.pendingSema.Release(1)

		n, err := s.client.PutObject(s.bucket, hash, r, "application/octet-stream")
		writer.result <- result{n: n, err: err}
	}()
	return writer, nil
}

func (s *S3Store) Delete(key string) error {
	panic("unimplemented")
	return nil
}
