package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/lru"
)

const failedCooldown = 15 * time.Second

var (
	errNotFound = errors.New("xorb not found at gateway")
	errClosed   = errors.New("peer closed")
	errEmpty    = errors.New("empty xorb")
)

// pull downloads one xorb from the gateway into a spool, validates it into the store and serves attached readers meanwhile.
type pull struct {
	p       *Peer
	hash    xet.XorbHash
	started chan struct{} // closed once stream is set or the pull failed before it
	done    chan struct{}
	stream  ioswmr.SWMR
	hold    io.Closer // keeps the spool file until the last release
	length  int64     // declared Content-Length, -1 if unknown
	size    int64     // committed bytes, set before the pull records the object
	err     error     // outcome, read only after done
	refs    int       // holders of the stream, guarded by p.mu

	cancel context.CancelFunc // ends the pull, queueing included
	queued bool               // still waiting for a pull slot, guarded by p.mu
	keep   bool               // asked for by the gateway, so never dropped for lack of readers; guarded by p.mu
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

// acquire returns exactly one of a joined or new pull (release it) or a pinned committed xorb (unpin it); force clears a failure cooldown.
func (p *Peer) acquire(hash xet.XorbHash, force bool) (*pull, *lru.Entry[xet.XorbHash, ready], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f := p.pulls[hash]; f != nil {
		f.refs++
		f.keep = f.keep || force
		return f, nil, nil
	}
	if p.ctx.Err() != nil {
		return nil, nil, errClosed
	}
	if entry := p.xorbs.Get(hash); entry != nil {
		exists, err := p.store.HasXorb(p.ctx, hash)
		if err != nil {
			return nil, nil, err
		}
		if exists {
			return nil, p.pinLocked(hash), nil
		}
		p.forget(entry)
	}
	now := time.Now()
	for h, c := range p.cooldown {
		if !now.Before(c.until) {
			delete(p.cooldown, h)
		}
	}
	if c, ok := p.cooldown[hash]; ok {
		if !force {
			return nil, nil, c.err
		}
		delete(p.cooldown, hash)
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.PullTimeout)
	f := &pull{p: p, hash: hash, started: make(chan struct{}), done: make(chan struct{}), refs: 2, cancel: cancel, queued: true, keep: force}
	p.pulls[hash] = f
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		f.run(ctx)
	}()
	return f, nil, nil
}

// run waits for a pull slot, downloads the xorb in it and settles the outcome; it ends the pull's context on return.
func (f *pull) run(ctx context.Context) {
	defer f.cancel()
	p := f.p
	var err error
	select {
	case p.sem <- struct{}{}:
		p.mu.Lock()
		f.queued = false
		p.mu.Unlock()
		err = f.download(ctx)
		<-p.sem
	case <-ctx.Done():
		err = ctx.Err()
	}
	f.finish(err)
}

// download fetches the xorb from the gateway into the spool, which readers stream meanwhile, and commits it.
func (f *pull) download(ctx context.Context) error {
	p := f.p
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apis.XorbURL(p.cfg.Gateway, f.hash.String()), nil)
	if err != nil {
		return err
	}
	req.Header.Set(apis.PeerHeader, p.id)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return errNotFound
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("gateway status %s", resp.Status)
	case resp.ContentLength > apis.MaxXorbBytes:
		return fmt.Errorf("xorb of %d bytes exceeds cap", resp.ContentLength)
	}
	f.open(resp.ContentLength)
	n, err := io.Copy(f.stream.Writer(), io.LimitReader(resp.Body, apis.MaxXorbBytes+1))
	switch {
	case err != nil:
		return err
	case n > apis.MaxXorbBytes:
		return errors.New("xorb exceeds cap")
	case n == 0:
		return errEmpty
	case resp.ContentLength >= 0 && n != resp.ContentLength:
		return fmt.Errorf("short body: %d of %d bytes", n, resp.ContentLength)
	}
	return f.commit(ctx, n)
}

// open creates the spool for a body of the declared length, -1 if unknown, and lets readers attach.
func (f *pull) open(length int64) {
	f.stream = ioswmr.NewSWMR(ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) {
		return os.CreateTemp(f.p.spoolDir, f.hash.String()+"-*.part")
	}), ioswmr.WithAutoClose())
	f.hold, _ = f.stream.NewReader(0)
	f.length = length
	close(f.started)
}

// commit stores the n spooled bytes as the xorb.
func (f *pull) commit(ctx context.Context, n int64) error {
	f.size = n
	rs, _ := f.stream.NewReadSeeker(0, int(n))
	stored, err := f.p.store.PutXorb(ctx, f.hash, rs)
	_ = rs.Close()
	if err == nil && !stored {
		// The store deduplicated against an existing object without reading this spool.
		rs, _ = f.stream.NewReadSeeker(0, int(n))
		err = xorb.Validate(rs, f.hash)
		_ = rs.Close()
	}
	return err
}

// finish ends the stream with err, unlists the pull, records the xorb or a cooldown, then wakes waiters and drops the pull's own reference.
func (f *pull) finish(err error) {
	p := f.p
	if f.stream == nil {
		close(f.started)
	} else {
		_ = f.stream.Writer().CloseWithError(err)
	}
	f.err = err
	p.mu.Lock()
	if p.pulls[f.hash] == f { // release may have dropped it and a newer pull taken the key
		delete(p.pulls, f.hash)
	}
	if err == nil {
		p.admit(f.hash, f.size, f)
	} else if !errors.Is(err, context.Canceled) { // abandoned or closed is no fault of the xorb
		p.cooldown[f.hash] = cooled{until: time.Now().Add(failedCooldown), err: err}
	}
	p.mu.Unlock()
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

func (f *pull) release() {
	f.p.mu.Lock()
	f.refs--
	if f.refs == 1 && f.queued && !f.keep {
		// Only its goroutine is left; unlisting it under the same lock keeps acquire from joining it.
		if f.p.pulls[f.hash] == f {
			delete(f.p.pulls, f.hash)
		}
		f.cancel()
	}
	last := f.refs == 0
	if last {
		f.p.evict() // a committed object nobody streams any more may go
	}
	f.p.mu.Unlock()
	if last && f.hold != nil {
		_ = f.hold.Close()
	}
}
