package cache

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/akmistry/cloud-util"
)

const (
	blockSize = 1024 * 1024
)

type cacheBlockReader interface {
	io.ReaderAt
	io.Closer
}

type BlockBlobCache struct {
	dir     string
	backing cloud.BlobStore

	blobReaderCache map[string]cloud.GetReader

	lock sync.Mutex
}

func NewBlockBlobCache(bs cloud.BlobStore, dir string) (*BlockBlobCache, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	c := &BlockBlobCache{
		dir:             dir,
		backing:         bs,
		blobReaderCache: make(map[string]cloud.GetReader),
	}

	return c, nil
}

func (c *BlockBlobCache) Size(key string) (int64, error) {
	// Pass-through
	// TODO: Cache this. Frequently called.
	return c.backing.Size(key)
}

func (c *BlockBlobCache) Put(key string) (cloud.PutWriter, error) {
	// Pass-through
	return c.backing.Put(key)
}

func (c *BlockBlobCache) List() ([]string, error) {
	// Pass-through
	l, ok := c.backing.(cloud.Lister)
	if !ok {
		return nil, errors.New("List() unimplemented")
	}
	return l.List()
}

func (c *BlockBlobCache) makeBlockFilePath(key string, block int64) string {
	if block%blockSize != 0 {
		log.Fatalf("block %d %% blockSize %d != 0", block, blockSize)
	}
	return filepath.Join(c.dir, fmt.Sprintf("%s-%d", key, block))
}

func (c *BlockBlobCache) getBlockReader(key string, block int64, br cloud.GetReader) (cacheBlockReader, error) {
	name := c.makeBlockFilePath(key, block)
	f, err := os.Open(name)
	if err == nil {
		return f, nil
	}

	// TODO: Ensure the same block isn't downloaded more than once concurrently
	buf := make([]byte, blockSize)
	n, err := br.ReadAt(buf, block)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n]

	f, err = os.CreateTemp(c.dir, "block-temp*")
	if err != nil {
		return nil, err
	}
	_, err = f.Write(buf)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	err = os.Rename(f.Name(), name)
	if err != nil {
		log.Printf("Unable to rename %s to %s: %v", f.Name(), name, err)
	}
	fi, err := f.Stat()
	if err == nil {
		err = f.Chmod(fi.Mode() | 0644)
	}
	if err != nil {
		log.Printf("Unable to stat or chown %s: %v", f.Name(), err)
	}

	return f, nil
}

type cacheReader struct {
	c   *BlockBlobCache
	key string
	br  cloud.GetReader
}

func (r *cacheReader) Size() int64 {
	return r.br.Size()
}

func (r *cacheReader) Close() error {
	return nil
}

func (r *cacheReader) ReadAt(p []byte, off int64) (int, error) {
	bytesRead := 0
	for len(p) > 0 {
		block := off - (off % blockSize)
		blockOff := off - block

		blockReader, err := r.c.getBlockReader(r.key, block, r.br)
		if err != nil {
			return bytesRead, err
		}
		n, err := blockReader.ReadAt(p, blockOff)
		blockReader.Close()
		bytesRead += n
		off += int64(n)
		p = p[n:]
		if err != nil && err != io.EOF {
			return bytesRead, err
		}
		// Expect to read the entire block.
		if len(p) > 0 && off%blockSize != 0 {
			return bytesRead, io.ErrUnexpectedEOF
		}
	}
	return bytesRead, nil
}

func (c *BlockBlobCache) Get(key string) (cloud.GetReader, error) {
	c.lock.Lock()
	br := c.blobReaderCache[key]
	c.lock.Unlock()

	if br == nil {
		var err error
		br, err = c.backing.Get(key)
		if err != nil {
			return nil, err
		}

		c.lock.Lock()
		c.blobReaderCache[key] = br
		c.lock.Unlock()
	}

	r := &cacheReader{
		c:   c,
		key: key,
		br:  br,
	}
	return r, nil
}

func (c *BlockBlobCache) Delete(key string) error {
	c.lock.Lock()
	br := c.blobReaderCache[key]
	if br != nil {
		br.Close()
		delete(c.blobReaderCache, key)
	}
	c.lock.Unlock()

	// TODO: Clear locally cached blocks
	return c.backing.Delete(key)
}
