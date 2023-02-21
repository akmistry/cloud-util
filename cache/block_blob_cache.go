package cache

import (
	"errors"
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

var openFileCache *OpenFileCache

func init() {
	openFileCache = NewOpenFileCache(1000)
}

type BlockBlobCache struct {
	dir     string
	backing cloud.BlobStore

	blobReaderCache map[string]cloud.GetReader

	blockCacheLru *lru.Cache[blockCacheKey, *blockCacheEntry]

	lock sync.Mutex
}

type blockCacheKey struct {
	blobKey string
	block   int64
}

type blockCacheEntry struct {
	fname string

	downloadDone chan struct{}
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

	c.blockCacheLru, err = lru.NewWithEvict[blockCacheKey, *blockCacheEntry](
		int(cacheSize/blockSize), c.blockEvictFunc)
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

		basename := d.Name()
		split := strings.LastIndex(basename, "-")
		if split < 0 {
			log.Printf("Error parsing cache file name %s", basename)
			return nil
		}
		key := basename[:split]
		block, err := strconv.ParseUint(basename[split+1:], 10, 64)
		if err != nil {
			log.Printf("Error parsing cache file block offset %s: %v", basename, err)
			return nil
		}

		cacheKey := blockCacheKey{blobKey: key, block: int64(block)}
		entry := &blockCacheEntry{
			fname:        path,
			downloadDone: make(chan struct{}),
		}
		close(entry.downloadDone)
		c.blockCacheLru.Add(cacheKey, entry)

		return nil
	})

	return c, nil
}

func (c *BlockBlobCache) blockEvictFunc(key blockCacheKey, e *blockCacheEntry) {
	os.Remove(e.fname)
	openFileCache.Remove(e.fname)
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
	basename := key + "-" + strconv.FormatInt(block, 10)
	return filepath.Join(c.dir, basename)
}

func (c *BlockBlobCache) getBlockReader(key string, blobSize int64, block int64, br cloud.GetReader) (ReaderAtCloser, error) {
	cacheKey := blockCacheKey{blobKey: key, block: block}

	for {
		c.lock.Lock()
		entry, ok := c.blockCacheLru.Get(cacheKey)
		if ok {
			c.lock.Unlock()

			<-entry.downloadDone
			// Open file
			r, err := openFileCache.Open(entry.fname)
			if err != nil {
				log.Printf("Error opening cache file %s: %v", entry.fname, err)
				// Retry
				continue
			}
			return r, nil
		}

		name := c.makeBlockFilePath(key, block)
		entry = &blockCacheEntry{
			fname:        name,
			downloadDone: make(chan struct{}),
		}
		c.blockCacheLru.Add(cacheKey, entry)
		c.lock.Unlock()

		defer close(entry.downloadDone)

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

		f, err := os.CreateTemp(c.dir, "block-temp*")
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
		openFileCache.Remove(path)
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
