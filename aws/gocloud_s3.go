package aws

import (
	"context"
	"fmt"
	"io"
	"log"
	//"net/http"
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/util"
)

//var flagS3DisableTls = flag.Bool("s3-disable-tls", false, "Disable TLS for S3")

//const maxPendingFetches = 512

type GCS3Store struct {
	session     *session.Session
	bucket      *blob.Bucket
	pendingSema *semaphore.Weighted
}

var (
	ErrWriteCanceled = errors.New("cloud: blob write canceled")
)

func NewDefaultGCS3Store(bucket string) *GCS3Store {
	return NewGCS3Store(*Endpoint, *AccessId, *AccessSecret, *Region, bucket)
}

func NewGCS3Store(endpoint, accessKey, secret, region, bucket string) *GCS3Store {
	/*
		transport := &http.Transport{
			MaxIdleConnsPerHost:   1024,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
		}
	*/

	session, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secret, ""),
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(*flagS3DisableTls),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	// Create a *blob.Bucket.
	blobBucket, err := s3blob.OpenBucket(context.TODO(), session, bucket, nil)
	if err != nil {
		panic(err)
	}

	return &GCS3Store{
		session:     session,
		bucket:      blobBucket,
		pendingSema: semaphore.NewWeighted(maxPendingFetches),
	}
}

func (s *GCS3Store) Size(name string) (int64, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	defer s.pendingSema.Release(1)

	attr, err := s.bucket.Attributes(context.TODO(), name)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}

	return attr.Size, nil
}

type goCloudGetReader struct {
	s    *GCS3Store
	name string
	size int64
}

/*
func createRange(off, len int64) string {
	return fmt.Sprintf("bytes=%d-%d", off, off+len-1)
}
*/

func (r *goCloudGetReader) Size() int64 {
	return r.size
}

func (r *goCloudGetReader) ReadAt(b []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	oldLen := len(b)
	if int64(len(b)) > (r.size - off) {
		b = b[:int(r.size-off)]
	}

	r.s.pendingSema.Acquire(context.Background(), 1)
	defer r.s.pendingSema.Release(1)

	rr, err := r.s.bucket.NewRangeReader(context.TODO(), r.name, off, int64(len(b)), nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return 0, os.ErrNotExist
	} else if err != nil {
		return 0, err
	}
	defer rr.Close()
	n, err := io.ReadFull(rr, b)
	if err == nil && n < oldLen {
		err = io.EOF
	}
	return n, err
}

func (r *goCloudGetReader) Close() error {
	return nil
}

func (s *GCS3Store) Get(name string) (cloud.GetReader, error) {
	size, err := s.Size(name)
	if err != nil {
		return nil, err
	}
	r := &goCloudGetReader{s: s, name: name, size: size}
	return r, nil
}

type goCloudPutWriter struct {
	s      *GCS3Store
	result chan result
	closed bool

	buf *util.StagingBuffer
}

func (w *goCloudPutWriter) Write(b []byte) (int, error) {
	n, err := w.buf.Write(b)
	if err != nil {
		err = fmt.Errorf("error writing %d bytes: %v", len(b), err)
	}
	return n, err
}

func (w *goCloudPutWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.buf.Close()

	res := <-w.result
	if res.err != nil {
		return res.err
	}
	return nil
}

func (w *goCloudPutWriter) Cancel() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.buf.CloseWithError(ErrWriteCanceled)

	// TODO: Wait for request completion, and delete the object if success.
	return nil
}

/*
type result struct {
	n   int64
	err error
}
*/

func (s *GCS3Store) Put(name string) (cloud.PutWriter, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	buf, err := util.NewStagingBuffer("")
	if err != nil {
		return nil, err
	}
	writer := &goCloudPutWriter{s: s, result: make(chan result, 1), buf: buf}
	r, _ := buf.Reader()
	go func() {
		defer r.Close()
		defer close(writer.result)
		defer s.pendingSema.Release(1)

		var n int64
		var err error
		var w io.WriteCloser
		for {
			r.Reset()

			ctx, cf := context.WithCancel(context.TODO())
			w, err = s.bucket.NewWriter(ctx, name, nil)
			if err == nil {
				n, err = io.Copy(w, r)
				if err != nil {
					cf()
				}
				err = w.Close()
			}
			cf()
			if err == ErrWriteCanceled {
				log.Print("Write canceled for object: ", name)
			}
			if err == nil || err == ErrWriteCanceled {
				break
			}
			log.Print("Error putting object: ", err)
			time.Sleep(time.Second)
		}

		writer.result <- result{n: n, err: err}
	}()
	return writer, nil
}

func (s *GCS3Store) Delete(name string) error {
	return s.bucket.Delete(context.TODO(), name)
}

func (s *GCS3Store) List() ([]string, error) {
	names := make([]string, 0)

	iter := s.bucket.List(nil)
	for {
		obj, err := iter.Next(context.TODO())
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		names = append(names, obj.Key)
	}
	return names, nil
}
