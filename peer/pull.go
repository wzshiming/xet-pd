package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
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
	size    int64     // committed bytes, set before run records the object
	err     error     // outcome, read only after done
	refs    int       // holders of the stream, guarded by p.mu
}

type cooled struct {
	until time.Time
	err   error
}

// acquire returns exactly one of a joined or new pull (release it) or a pinned committed xorb (unpin it); force clears a failure cooldown.
func (p *Peer) acquire(hash xet.XorbHash, force bool) (*pull, *ready, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f := p.pulls[hash]; f != nil {
		f.refs++
		return f, nil, nil
	}
	if p.ctx.Err() != nil {
		return nil, nil, errClosed
	}
	if entry := p.index[hash]; entry != nil {
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
	f := &pull{p: p, hash: hash, started: make(chan struct{}), done: make(chan struct{}), refs: 2}
	p.pulls[hash] = f
	p.wg.Add(1)
	go f.run()
	return f, nil, nil
}

func (f *pull) release() {
	f.p.mu.Lock()
	f.refs--
	last := f.refs == 0
	if last {
		f.p.evict() // a committed object nobody streams any more may go
	}
	f.p.mu.Unlock()
	if last && f.hold != nil {
		_ = f.hold.Close()
	}
}

// wait blocks until the stream can be served: headers handled and size known, or the pull finished.
func (f *pull) wait(ctx context.Context) (ioswmr.SWMR, int64, error) {
	select {
	case <-f.started:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	if f.length >= 1 {
		select {
		case <-f.done:
		default:
			return f.stream, f.length, nil
		}
	}
	select {
	case <-f.done:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	if f.err != nil {
		return nil, 0, f.err
	}
	return f.stream, f.size, nil
}

func (f *pull) run() {
	p := f.p
	defer p.wg.Done()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.PullTimeout)
	defer cancel()
	err := f.fetch(ctx)
	if f.stream == nil {
		close(f.started)
	} else {
		_ = f.stream.Writer().CloseWithError(err)
	}
	f.err = err
	p.mu.Lock()
	delete(p.pulls, f.hash)
	if err != nil {
		p.cooldown[f.hash] = cooled{until: time.Now().Add(failedCooldown), err: err}
	} else {
		p.admit(f.hash, f.size, f)
	}
	p.mu.Unlock()
	close(f.done)
	f.release()
}

func (f *pull) fetch(ctx context.Context) error {
	p := f.p
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.Gateway+apis.XorbPathPrefix+f.hash.String(), nil)
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
	f.stream = ioswmr.NewSWMR(ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) {
		return os.CreateTemp(p.spoolDir, f.hash.String()+"-*.part")
	}), ioswmr.WithAutoClose())
	f.hold, _ = f.stream.NewReader(0)
	f.length = resp.ContentLength
	close(f.started)
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
	f.size = n
	rs, _ := f.stream.NewReadSeeker(0, int(n))
	stored, err := p.store.PutXorb(ctx, f.hash, rs)
	_ = rs.Close()
	if err != nil || stored {
		return err
	}
	// The store deduplicated against an existing object without reading this spool.
	rs, _ = f.stream.NewReadSeeker(0, int(n))
	defer func() { _ = rs.Close() }()
	return xorb.Validate(rs, f.hash)
}

func statusOf(err error) int {
	switch {
	case errors.Is(err, errNotFound):
		return http.StatusNotFound
	case errors.Is(err, errClosed):
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}
