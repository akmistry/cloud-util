package cache

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/golang-lru/v2"

	"github.com/akmistry/cloud-util"
)

const (
	blockSize = 1024 * 1024
)

var (
	blockBufPool = sync.Pool{New: func() interface{} {
		return make([]byte, blockSize)
	}}
)

type cacheBlockReader interface {
	io.ReaderAt
	io.Closer
}

type BlockBlobCache struct {
	dir     string
	backing cloud.BlobStore
	lru     *lru.Cache[string, bool]

	blobReaderCache map[string]cloud.GetReader

	lock sync.Mutex
}

func NewBlockBlobCache(bs cloud.BlobStore, dir string, cacheSize int64) (*BlockBlobCache, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	c := &BlockBlobCache{
		dir:             dir,
		backing:         bs,
		blobReaderCache: make(map[string]cloud.GetReader),
	}

	evictFunc := func(key string, value bool) {
		os.Remove(key)
	}
	c.lru, err = lru.NewWithEvict(int(cacheSize/blockSize), evictFunc)
	if err != nil {
		return nil, err
	}

	// Populate the cache
	filepath.WalkDir(c.dir, func(path string, d fs.DirEntry, err error) error {
		if path == c.dir {
			return nil
		} else if d.IsDir() {
			return fs.SkipDir
		} else if err != nil {
			log.Printf("Error walking path %s: %v", path, err)
			return nil
		}

		c.lru.Add(path, true)
		return nil
	})

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

func (c *BlockBlobCache) getBlockReader(key string, blobSize int64, block int64, br cloud.GetReader) (cacheBlockReader, error) {
	name := c.makeBlockFilePath(key, block)
	c.lru.Get(name)

	f, err := os.Open(name)
	if err == nil {
		return f, nil
	}

	// TODO: Ensure the same block isn't downloaded more than once concurrently
	buf := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(buf)
	if blockSize > blobSize-block {
		buf = buf[:int(blobSize-block)]
	}
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
	c.lru.Add(name, true)

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
		blockOff := off % blockSize
		blockRem := blockSize - blockOff
		block := off - blockOff

		blockReader, err := r.c.getBlockReader(r.key, r.br.Size(), block, r.br)
		if err != nil {
			return bytesRead, err
		}
		readLen := len(p)
		if int64(readLen) > blockRem {
			readLen = int(blockRem)
		}
		n, err := blockReader.ReadAt(p[:readLen], blockOff)
		blockReader.Close()
		bytesRead += n
		off += int64(n)
		p = p[n:]
		if err == io.EOF && n == readLen {
			err = nil
		} else if err != nil {
			return bytesRead, err
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

func (c *BlockBlobCache) deleteCachedBlocks(key string) {
	filepath.WalkDir(c.dir, func(path string, d fs.DirEntry, err error) error {
		if path == c.dir {
			return nil
		} else if d.IsDir() {
			return fs.SkipDir
		} else if err != nil {
			log.Printf("Error walking path %s: %v", path, err)
			return nil
		}

		if !strings.HasPrefix(d.Name(), key+"-") {
			return nil
		}

		blockOffsetStr := strings.TrimPrefix(d.Name(), key+"-")
		_, err = strconv.ParseUint(blockOffsetStr, 10, 64)
		if err != nil {
			log.Printf("Error parsing block offset '%s': %v", blockOffsetStr, err)
			return nil
		}

		err = os.Remove(path)
		if err != nil {
			log.Printf("Error removing block file %s: %v", path, err)
		}
		return nil
	})
}

func (c *BlockBlobCache) Delete(key string) error {
	c.lock.Lock()
	br := c.blobReaderCache[key]
	if br != nil {
		br.Close()
		delete(c.blobReaderCache, key)
	}
	c.lock.Unlock()

	c.deleteCachedBlocks(key)
	return c.backing.Delete(key)
}
