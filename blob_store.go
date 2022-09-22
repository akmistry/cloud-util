package cloud

import "io"

type GetReader interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

type PutWriter interface {
	io.Writer
	io.Closer
	Cancel() error
}

type Lister interface {
  List() ([]string, error)
}

type BlobStore interface {
	Size(key string) (int64, error)
	Get(key string) (GetReader, error)
	Put(key string) (PutWriter, error)
	Delete(key string) error
}
