package aws

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
)

var flagS3DisableTls = flag.Bool("s3-disable-tls", false, "Disable TLS for S3")

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
	transport := &http.Transport{
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}
	options := &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secret, ""),
		Secure:    !*flagS3DisableTls,
		Transport: transport,
	}
	client, err := minio.New(endpoint, options)
	if err != nil {
		panic(err)
	}
	return &S3Store{
		client:      client,
		bucket:      bucket,
		pendingSema: semaphore.NewWeighted(maxPendingFetches),
	}
}

func (s *S3Store) Size(name string) (int64, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	defer s.pendingSema.Release(1)

	info, err := s.client.StatObject(context.TODO(), s.bucket, name, minio.StatObjectOptions{})
	if errResp, ok := err.(minio.ErrorResponse); ok && errResp.Code == "NoSuchKey" {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	return info.Size, nil
}

type getReader struct {
	s    *S3Store
	name string
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

	obj, err := r.s.client.GetObject(context.TODO(), r.s.bucket, r.name, minio.GetObjectOptions{})
	if errResp, ok := err.(minio.ErrorResponse); ok && errResp.Code == "NoSuchKey" {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	defer obj.Close()
	return obj.ReadAt(b, off)
}

func (r *getReader) Close() error {
	return nil
}

func (s *S3Store) Get(name string) (cloud.GetReader, error) {
	size, err := s.Size(name)
	if err != nil {
		return nil, err
	}
	r := &getReader{s: s, name: name, size: size}
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

func (s *S3Store) Put(name string) (cloud.PutWriter, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	r, pw := io.Pipe()
	writer := &putWriter{s: s, pw: pw, result: make(chan result, 1)}
	go func() {
		defer r.Close()
		defer close(writer.result)
		defer s.pendingSema.Release(1)

		opts := minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		}
		info, err := s.client.PutObject(context.TODO(), s.bucket, name, r, -1, opts)
		if err != nil {
			log.Print("Error putting object: ", err)
		}
		writer.result <- result{n: info.Size, err: err}
	}()
	return writer, nil
}

func (s *S3Store) Delete(name string) error {
	return s.client.RemoveObject(context.TODO(), s.bucket, name, minio.RemoveObjectOptions{})
}

func (s *S3Store) List() ([]string, error) {
	names := make([]string, 0)
	iter := s.client.ListObjects(context.TODO(), s.bucket, minio.ListObjectsOptions{})
	for info := range iter {
		names = append(names, info.Key)
	}
	return names, nil
}
