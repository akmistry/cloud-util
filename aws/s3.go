package aws

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/semaphore"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/util"
)

var (
	ErrWriteCanceled = errors.New("cloud: blob write canceled")

	flagS3DisableTls = flag.Bool("s3-disable-tls", false, "Disable TLS for S3")
)

const (
	maxPendingFetches = 512

	s3Scheme = "s3"
)

func init() {
	cloud.RegisterBlobStoreScheme(s3Scheme, openS3BlobStore)
}

// URL format: s3://<bucket_name>
func openS3BlobStore(path string) (cloud.BlobStore, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != s3Scheme {
		return nil, fmt.Errorf("cloud/aws.s3: unexpected scheme: %s", u.Scheme)
	} else if u.Host == "" {
		return nil, fmt.Errorf("cloud/aws.s3: empty bucket name")
	} else if u.Path != "" {
		return nil, fmt.Errorf("cloud/aws.s3: path must be empty")
	}

	return NewDefaultS3Store(u.Host), nil
}

type S3Store struct {
	client      *s3.Client
	bucket      *blob.Bucket
	pendingSema *semaphore.Weighted
}

func NewDefaultS3Store(bucket string) *S3Store {
	return NewS3Store("", "", "", "", bucket)
}

func NewS3Store(endpoint, accessKey, secret, region, bucket string) *S3Store {
	var optFns []func(*config.LoadOptions) error
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}
	if endpoint != "" {
		optFns = append(optFns, config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			})))
	}
	if accessKey != "" && secret != "" {
		optFns = append(optFns, config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     accessKey,
				SecretAccessKey: secret,
			},
		}))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
	if err != nil {
		panic(err)
	}

	//cfg := &aws.Config{
	//	DisableSSL:       aws.Bool(*flagS3DisableTls),
	//	S3ForcePathStyle: aws.Bool(true),
	//}

	client := s3.NewFromConfig(cfg, func(opts *s3.Options) {
		opts.UsePathStyle = true
	})

	blobBucket, err := s3blob.OpenBucketV2(context.TODO(), client, bucket, nil)
	if err != nil {
		panic(err)
	}

	return &S3Store{
		client:      client,
		bucket:      blobBucket,
		pendingSema: semaphore.NewWeighted(maxPendingFetches),
	}
}

func (s *S3Store) Size(name string) (int64, error) {
	for {
		attr, err := s.bucket.Attributes(context.TODO(), name)
		if err == nil {
			return attr.Size, nil
		} else if gcerrors.Code(err) == gcerrors.NotFound {
			return 0, os.ErrNotExist
		}
		log.Printf("cloud-s3: error sizing blob %s: %v", name, err)
		time.Sleep(time.Second)
	}
}

type s3GetReader struct {
	s    *S3Store
	name string
	size int64
}

func (r *s3GetReader) Size() int64 {
	return r.size
}

func (r *s3GetReader) ReadAt(b []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	oldLen := len(b)
	if int64(len(b)) > (r.size - off) {
		b = b[:int(r.size-off)]
	}

	r.s.pendingSema.Acquire(context.Background(), 1)
	defer r.s.pendingSema.Release(1)

	for {
		rr, err := r.s.bucket.NewRangeReader(context.TODO(), r.name, off, int64(len(b)), nil)
		if gcerrors.Code(err) == gcerrors.NotFound {
			return 0, os.ErrNotExist
		} else if err != nil {
			log.Printf("cloud-s3: error attempting to read blob %s: %v", r.name, err)
			time.Sleep(time.Second)
			continue
		}
		n, err := io.ReadFull(rr, b)
		rr.Close()
		if err != nil {
			log.Printf("cloud-s3: error reading blob %s: %v", r.name, err)
			time.Sleep(time.Second)
			continue
		}
		if n < oldLen {
			err = io.EOF
		}
		return n, err
	}
}

func (r *s3GetReader) Close() error {
	return nil
}

func (s *S3Store) Get(name string) (cloud.GetReader, error) {
	size, err := s.Size(name)
	if err != nil {
		return nil, err
	}
	r := &s3GetReader{s: s, name: name, size: size}
	return r, nil
}

type result struct {
	n   int64
	err error
}

type s3PutWriter struct {
	s      *S3Store
	result chan result
	closed bool

	buf *util.StagingBuffer
}

func (w *s3PutWriter) Write(b []byte) (int, error) {
	n, err := w.buf.Write(b)
	if err != nil {
		err = fmt.Errorf("error writing %d bytes: %v", len(b), err)
	}
	return n, err
}

func (w *s3PutWriter) Close() error {
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

func (w *s3PutWriter) Cancel() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.buf.CloseWithError(ErrWriteCanceled)

	// TODO: Wait for request completion, and delete the object if success.
	return nil
}

func (s *S3Store) Put(name string) (cloud.PutWriter, error) {
	s.pendingSema.Acquire(context.Background(), 1)
	buf, err := util.NewStagingBuffer("")
	if err != nil {
		return nil, err
	}
	writer := &s3PutWriter{s: s, result: make(chan result, 1), buf: buf}
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
					w.Close()
				} else {
					err = w.Close()
				}
			}
			cf()
			if err == nil || err == ErrWriteCanceled {
				break
			}
			log.Printf("cloud-s3: error putting blob %s: %v", name, err)
			time.Sleep(time.Second)
		}

		writer.result <- result{n: n, err: err}
	}()
	return writer, nil
}

func (s *S3Store) Delete(name string) error {
	for {
		err := s.bucket.Delete(context.TODO(), name)
		if err == nil {
			return nil
		} else if gcerrors.Code(err) == gcerrors.NotFound {
			return os.ErrNotExist
		}
		log.Printf("cloud-s3: error deleting blob %s: %v", name, err)
		time.Sleep(time.Second)
	}
}

func (s *S3Store) List() ([]string, error) {
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
