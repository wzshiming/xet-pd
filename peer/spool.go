package peer

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

// spool is a growing on-disk buffer with one sequential writer and readers that
// block until the bytes they want are published.
type spool struct {
	f *os.File

	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every state change
	written int64
	size    int64 // -1 until known
	done    bool
	err     error
}

func newSpool(f *os.File, size int64) *spool {
	return &spool{f: f, changed: make(chan struct{}), size: size}
}

func (s *spool) Write(p []byte) (int, error) {
	n, err := s.f.Write(p)
	s.mu.Lock()
	s.written += int64(n)
	s.notify()
	s.mu.Unlock()
	return n, err
}

// finish publishes the outcome; only a validated spool releases its last byte.
func (s *spool) finish(err error) {
	s.mu.Lock()
	s.done, s.err = true, err
	if err == nil && s.size < 0 {
		s.size = s.written
	}
	s.notify()
	s.mu.Unlock()
}

func (s *spool) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// sizeKnown reports whether readers can be served before the fill completes.
func (s *spool) sizeKnown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size > 0
}

func (s *spool) result() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// avail is the readable prefix; the last byte is withheld until validation succeeds.
func (s *spool) avail() int64 {
	if s.done && s.err == nil {
		return s.size
	}
	if s.size < 0 {
		return 0
	}
	return max(0, min(s.written, s.size-1))
}

type spoolReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	s      *spool

	mu     sync.Mutex // serializes Read, Seek and Close
	off    int64
	closed bool
	failed error // first read failure, so the handler can abort the response
}

var errReaderClosed = errors.New("spool: reader closed")

func (s *spool) reader(ctx context.Context) *spoolReader {
	ctx, cancel := context.WithCancel(ctx)
	return &spoolReader{ctx: ctx, cancel: cancel, s: s}
}

// Close interrupts a blocked Read, waits for it and reports the first failure;
// afterwards no call touches the file, so the spool may be released.
func (r *spoolReader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.failed
}

func (r *spoolReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errReaderClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		r.s.mu.Lock()
		avail, done, err, changed := r.s.avail(), r.s.done, r.s.err, r.s.changed
		r.s.mu.Unlock()
		switch {
		case r.off < avail:
			n, err := r.s.f.ReadAt(p[:min(int64(len(p)), avail-r.off)], r.off)
			r.off += int64(n)
			if err != nil && err != io.EOF {
				r.failed = err
			}
			return n, err
		case err != nil:
			r.failed = err
			return 0, err
		case done:
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			r.failed = r.ctx.Err()
			return 0, r.failed
		case <-changed:
		}
	}
}

// Seek never waits; SeekEnd needs the size to be known.
func (r *spoolReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errReaderClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.off
	case io.SeekEnd:
		r.s.mu.Lock()
		base = r.s.size
		r.s.mu.Unlock()
		if base < 0 {
			return 0, errors.New("spool: size unknown")
		}
	default:
		return 0, errors.New("spool: invalid whence")
	}
	if base+offset < 0 {
		return 0, errors.New("spool: negative offset")
	}
	r.off = base + offset
	return r.off, nil
}
