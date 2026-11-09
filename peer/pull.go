package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

const failedCooldown = 15 * time.Second

var (
	errNotFound = fmt.Errorf("no holder served the xorb: %w", fs.ErrNotExist)
	errClosed   = errors.New("peer closed")
	errEmpty    = errors.New("empty xorb")
)

// pull downloads one xorb from a holder into a spool, validates it into the store and serves attached readers meanwhile.
type pull struct {
	s       *store
	hash    xet.XorbHash
	started chan struct{} // closed once stream is set or the pull failed before it
	done    chan struct{}
	stream  ioswmr.SWMR
	hold    io.Closer // keeps the spool file until the last release
	length  int64     // declared Content-Length, -1 if unknown
	size    int64     // committed bytes, set before the pull records the object
	err     error     // outcome, read only after done
	refs    int       // holders of the stream, guarded by s.mu

	cancel context.CancelFunc // ends the pull, queueing included
	queued bool               // still waiting for a pull slot, guarded by s.mu
	keep   bool               // forced, so never dropped for lack of readers; guarded by s.mu
}

type cooled struct {
	until time.Time
	err   error
}

// openSpool creates the spool directory under dataDir and deletes the partial pulls a previous run left in it.
func openSpool(dataDir string) (string, error) {
	spoolDir := filepath.Join(dataDir, "spool")
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return "", err
	}
	stale, _ := filepath.Glob(filepath.Join(spoolDir, "*.part"))
	for _, f := range stale {
		_ = os.Remove(f)
	}
	return spoolDir, nil
}

// acquire joins the listed pull of hash or starts one, either to release, and returns nil once the xorb is local; force keeps the pull without readers and clears a failure cooldown.
func (s *store) acquire(hash xet.XorbHash, force bool) (*pull, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.pulls[hash]; f != nil {
		f.refs++
		f.keep = f.keep || force
		return f, nil
	}
	if s.ctx.Err() != nil {
		return nil, errClosed
	}
	// A pull leaves s.pulls only after its commit, so this sees every copy committed before.
	if ok, err := s.Storage.HasXorb(s.ctx, hash); err != nil || ok {
		return nil, err
	}
	now := time.Now()
	for h, c := range s.cooldown {
		if !now.Before(c.until) {
			delete(s.cooldown, h)
		}
	}
	if c, ok := s.cooldown[hash]; ok {
		if !force {
			return nil, c.err
		}
		delete(s.cooldown, hash)
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.PullTimeout)
	f := &pull{s: s, hash: hash, started: make(chan struct{}), done: make(chan struct{}), refs: 2, cancel: cancel, queued: true, keep: force}
	s.pulls[hash] = f
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f.run(ctx)
	}()
	return f, nil
}

// run waits for a pull slot, downloads the xorb in it and settles the outcome; it ends the pull's context on return.
func (f *pull) run(ctx context.Context) {
	defer f.cancel()
	s := f.s
	var err error
	select {
	case s.sem <- struct{}{}:
		s.mu.Lock()
		f.queued = false
		s.mu.Unlock()
		err = f.download(ctx)
		<-s.sem
	case <-ctx.Done():
		err = ctx.Err()
	}
	f.finish(err)
}

// download asks the best holders in turn until one delivers a byte, streams its copy into the spool, which readers read meanwhile, and commits it.
func (f *pull) download(ctx context.Context) error {
	resp, err := f.s.lookup(ctx, apis.LookupRequest{Xorbs: []string{f.hash.String()}})
	if err != nil {
		return err
	}
	holders := resp.Xorbs[f.hash.String()]
	failed := errNotFound
	for _, p := range holders[:min(len(holders), holderAttempts)] {
		n, err := f.spool(ctx, p)
		if f.stream != nil { // readers may have seen this copy's size, so it decides the pull
			if err == nil {
				err = f.commit(ctx, n)
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		failed = fmt.Errorf("%w: %w", errNotFound, err)
	}
	return failed
}

// spool streams the copy of holder p into the spool, opened on its first byte, and returns how many bytes it took.
func (f *pull) spool(ctx context.Context, p apis.Peer) (int64, error) {
	body, length, err := f.s.fetch.xorb(ctx, p, f.hash)
	if err != nil {
		return 0, err
	}
	defer func() { _ = body.Close() }()
	if length > apis.MaxXorbBytes {
		return 0, fmt.Errorf("xorb of %d bytes exceeds cap", length)
	}
	br := bufio.NewReader(body)
	if _, err := br.Peek(1); err != nil {
		if err == io.EOF {
			err = errEmpty
		}
		return 0, err
	}
	f.open(length)
	n, err := io.Copy(f.stream.Writer(), io.LimitReader(br, apis.MaxXorbBytes+1))
	switch {
	case err != nil:
		return n, err
	case n > apis.MaxXorbBytes:
		return n, errors.New("xorb exceeds cap")
	case length >= 0 && n != length:
		return n, fmt.Errorf("short body: %d of %d bytes", n, length)
	}
	return n, nil
}

// open creates the spool for a body of the declared length, -1 if unknown, and lets readers attach.
func (f *pull) open(length int64) {
	f.stream = ioswmr.NewSWMR(ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) {
		return os.CreateTemp(f.s.spoolDir, f.hash.String()+"-*.part")
	}), ioswmr.WithAutoClose())
	f.hold, _ = f.stream.NewReader(0)
	f.length = length
	close(f.started)
}

// commit stores the n spooled bytes as the xorb.
func (f *pull) commit(ctx context.Context, n int64) error {
	f.size = n
	rs, _ := f.stream.NewReadSeeker(0, int(n))
	stored, err := f.s.PutXorb(ctx, f.hash, rs)
	_ = rs.Close()
	if err == nil && !stored {
		// The store deduplicated against an existing object without reading this spool.
		rs, _ = f.stream.NewReadSeeker(0, int(n))
		err = xorb.Validate(rs, f.hash)
		_ = rs.Close()
	}
	return err
}

// finish ends the stream with err, unlists the pull, records a cooldown on failure, then wakes waiters and drops the pull's own reference.
func (f *pull) finish(err error) {
	s := f.s
	if f.stream == nil {
		close(f.started)
	} else {
		_ = f.stream.Writer().CloseWithError(err)
	}
	f.err = err
	switch {
	case err == nil:
		s.cfg.Metrics.pull("ok")
	case errors.Is(err, errNotFound):
		s.cfg.Metrics.pull("not_found")
	case errors.Is(err, context.Canceled):
		s.cfg.Metrics.pull("canceled")
	case errors.Is(err, context.DeadlineExceeded):
		s.cfg.Metrics.pull("timeout")
	default:
		s.cfg.Metrics.pull("error")
	}
	s.mu.Lock()
	if s.pulls[f.hash] == f { // release may have dropped it and a newer pull taken the key
		delete(s.pulls, f.hash)
	}
	if err != nil && !errors.Is(err, context.Canceled) { // abandoned or closed is no fault of the xorb
		s.cooldown[f.hash] = cooled{until: time.Now().Add(failedCooldown), err: err}
	}
	s.mu.Unlock()
	close(f.done)
	f.release()
}

// servable returns the size to serve once headers are handled and the size is known, or once the pull finished.
func (f *pull) servable(ctx context.Context) (int64, error) {
	select {
	case <-f.started:
	case <-ctx.Done():
		return -1, ctx.Err()
	}
	size := int64(-1)
	select {
	case <-f.done:
	default:
		size = f.length
	}
	if size >= 1 {
		return size, nil
	}
	select {
	case <-f.done:
		return f.size, f.err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

// wait blocks until f finished or ctx ended, then lets go of f.
func (f *pull) wait(ctx context.Context) error {
	defer f.release()
	select {
	case <-f.done:
		return f.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *pull) release() {
	s := f.s
	s.mu.Lock()
	f.refs--
	if f.refs == 1 && f.queued && !f.keep {
		// Only its goroutine is left; unlisting it under the same lock keeps acquire from joining it.
		if s.pulls[f.hash] == f {
			delete(s.pulls, f.hash)
		}
		f.cancel()
	}
	last := f.refs == 0
	s.mu.Unlock()
	if last && f.hold != nil {
		_ = f.hold.Close()
	}
}
