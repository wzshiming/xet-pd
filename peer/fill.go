package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

const (
	maxXorbBytes   = 128 << 20 // packed xorb cap, leaves room for chunk headers and footer
	failedCooldown = 15 * time.Second
)

var (
	errNotFound = errors.New("xorb not found at gateway")
	errClosed   = errors.New("peer closed")
)

// fill downloads one xorb from the gateway into a spool, validates it into the store and serves attached readers meanwhile.
type fill struct {
	p     *Peer
	ns    string
	hash  xet.XorbHash
	ready chan struct{} // closed once spool is set or err is set
	done  chan struct{}
	spool *spool
	size  int64 // committed bytes, set before run records the object
	err   error // pre-body failure, read only after ready
	refs  int   // holders of the spool file, guarded by p.mu
}

type cooled struct {
	until time.Time
	err   error
}

// acquire returns exactly one of a joined or new fill (release it) or a pinned committed xorb (unpin it); force clears a failure cooldown.
func (p *Peer) acquire(ns string, hash xet.XorbHash, force bool) (*fill, *ready, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f := p.fills[hash]; f != nil {
		f.refs++
		return f, nil, nil
	}
	if p.ctx.Err() != nil {
		return nil, nil, errClosed
	}
	if entry := p.index[hash]; entry != nil {
		exists, err := p.store.HasXorb(p.ctx, ns, hash)
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
	f := &fill{p: p, ns: ns, hash: hash, ready: make(chan struct{}), done: make(chan struct{}), refs: 2}
	p.fills[hash] = f
	p.wg.Add(1)
	go f.run()
	return f, nil, nil
}

func (f *fill) release() {
	f.p.mu.Lock()
	f.refs--
	last := f.refs == 0
	if last {
		f.p.evict() // a committed object nobody streams any more may go
	}
	f.p.mu.Unlock()
	if last && f.spool != nil {
		_ = f.spool.f.Close()
		_ = os.Remove(f.spool.f.Name())
	}
}

// wait blocks until the spool can be served: headers handled and size known, or the fill finished.
func (f *fill) wait(ctx context.Context) (*spool, error) {
	select {
	case <-f.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.spool.sizeKnown() {
		return f.spool, nil
	}
	select {
	case <-f.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := f.spool.result(); err != nil {
		return nil, err
	}
	return f.spool, nil
}

func (f *fill) run() {
	p := f.p
	defer p.wg.Done()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.FillTimeout)
	defer cancel()
	err := f.fetch(ctx)
	if f.spool == nil {
		f.err = err
		close(f.ready)
	} else {
		f.spool.finish(err)
	}
	p.mu.Lock()
	delete(p.fills, f.hash)
	if err != nil {
		p.cooldown[f.hash] = cooled{until: time.Now().Add(failedCooldown), err: err}
	} else {
		p.admit(f.hash, f.size, f)
	}
	p.mu.Unlock()
	close(f.done)
	f.release()
}

func (f *fill) fetch(ctx context.Context) error {
	p := f.p
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.Gateway+"/v1/xorbs/"+url.PathEscape(f.ns)+"/"+f.hash.String(), nil)
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
	case resp.ContentLength > maxXorbBytes:
		return fmt.Errorf("xorb of %d bytes exceeds cap", resp.ContentLength)
	}
	tmp, err := os.CreateTemp(p.spoolDir, f.hash.String()+"-*.part")
	if err != nil {
		return err
	}
	f.spool = newSpool(tmp, resp.ContentLength)
	close(f.ready)
	n, err := io.Copy(f.spool, io.LimitReader(resp.Body, maxXorbBytes+1))
	switch {
	case err != nil:
		return err
	case n > maxXorbBytes:
		return errors.New("xorb exceeds cap")
	case resp.ContentLength >= 0 && n != resp.ContentLength:
		return fmt.Errorf("short body: %d of %d bytes", n, resp.ContentLength)
	}
	f.size = n
	stored, err := p.store.PutXorb(ctx, f.ns, f.hash, io.NewSectionReader(tmp, 0, n))
	if err != nil || stored {
		return err
	}
	// The store deduplicated against an existing object without reading this spool.
	return xorb.Validate(io.NewSectionReader(tmp, 0, n), f.hash)
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
