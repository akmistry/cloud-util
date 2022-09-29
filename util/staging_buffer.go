package util

import (
	"io"
	"sync"

	"github.com/akmistry/go-util/tempfile"
)

type StagingBuffer struct {
	f tempfile.File

	written  int64
	closeErr error
	lock     sync.Mutex
	cond     *sync.Cond

	readerWg sync.WaitGroup
}

func NewStagingBuffer(tempDir string) (*StagingBuffer, error) {
	f, err := tempfile.MakeTempFile(tempDir)
	if err != nil {
		return nil, err
	}

	b := &StagingBuffer{
		f: f,
	}
	b.cond = sync.NewCond(&b.lock)

	return b, nil
}

func (b *StagingBuffer) CloseWithError(err error) error {
	b.lock.Lock()
	if b.closeErr == nil {
		b.closeErr = err
	}
	b.cond.Broadcast()
	b.lock.Unlock()

	b.readerWg.Wait()
	b.f.Close()

	return nil
}

func (b *StagingBuffer) Close() error {
	return b.CloseWithError(io.EOF)
}

func (b *StagingBuffer) Write(p []byte) (int, error) {
	n, err := b.f.WriteAt(p, b.written)

	b.lock.Lock()
	b.written += int64(n)
	b.cond.Broadcast()
	b.lock.Unlock()

	return n, err
}

type StagingBufferReader struct {
	b      *StagingBuffer
	offset int64
	wg     *sync.WaitGroup
}

func (r *StagingBufferReader) Read(p []byte) (int, error) {
	r.b.lock.Lock()
	for r.offset == r.b.written {
		if r.b.closeErr != nil {
			r.b.lock.Unlock()
			return 0, r.b.closeErr
		}
		r.b.cond.Wait()
	}
	if r.b.closeErr != nil && r.b.closeErr != io.EOF {
		r.b.lock.Unlock()
		return 0, r.b.closeErr
	}
	written := r.b.written
	r.b.lock.Unlock()

	if r.offset > written {
		panic("r.offset > r.b.written")
	}
	avail := written - r.offset
	readLen := len(p)
	if int64(readLen) > avail {
		readLen = int(avail)
	}
	n, err := r.b.f.ReadAt(p[:readLen], r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *StagingBufferReader) IsOpen() bool {
	r.b.lock.Lock()
	defer r.b.lock.Unlock()
	return r.b.closeErr == nil || r.b.closeErr == io.EOF
}

func (r *StagingBufferReader) Reset() {
	r.offset = 0
}

func (r *StagingBufferReader) Close() error {
	r.wg.Done()
	return nil
}

func (b *StagingBuffer) Reader() (*StagingBufferReader, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closeErr != nil {
		return nil, io.ErrClosedPipe
	}

	b.readerWg.Add(1)
	r := &StagingBufferReader{
		b:  b,
		wg: &b.readerWg,
	}
	return r, nil
}
