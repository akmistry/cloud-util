package cache

import (
	"io"
	"os"
	"sync"

	"github.com/hashicorp/golang-lru/v2"
)

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type openFileEntry struct {
	r ReaderAtCloser

	refCount int
	lock     sync.Mutex
	cond     *sync.Cond
}

func (e *openFileEntry) ref() bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.r == nil {
		return false
	}
	e.refCount++
	return true
}

func (e *openFileEntry) unref() {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.refCount--
	if e.refCount < 0 {
		panic("e.refCount < 0")
	} else if e.refCount == 0 {
		e.cond.Signal()
	}
}

type OpenFileCache struct {
	cache *lru.Cache[string, *openFileEntry]
	lock  sync.Mutex
}

func NewOpenFileCache(maxEntries int) *OpenFileCache {
	c := &OpenFileCache{}
	cache, err := lru.NewWithEvict(maxEntries, c.evictFunc)
	if err != nil {
		panic(err)
	}
	c.cache = cache
	return c
}

func (c *OpenFileCache) evictFunc(name string, e *openFileEntry) {
	go func() {
		e.lock.Lock()
		defer e.lock.Unlock()

		for e.refCount != 0 {
			e.cond.Wait()
		}
		e.r.Close()
		e.r = nil
		e.cond = nil
	}()
}

type openFileReader struct {
	e *openFileEntry
}

func (r *openFileReader) ReadAt(b []byte, off int64) (int, error) {
	return r.e.r.ReadAt(b, off)
}

func (r *openFileReader) Close() error {
	if r.e == nil {
		return nil
	}
	r.e.unref()
	r.e = nil
	return nil
}

func (c *OpenFileCache) Open(name string) (ReaderAtCloser, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	e, ok := c.cache.Get(name)
	if ok && e.ref() {
		return &openFileReader{e: e}, nil
	}

	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	e = &openFileEntry{
		r:        f,
		refCount: 1,
	}
	e.cond = sync.NewCond(&c.lock)
	c.cache.Add(name, e)
	return &openFileReader{e: e}, nil
}

func (c *OpenFileCache) Remove(name string) {
	c.cache.Remove(name)
}
