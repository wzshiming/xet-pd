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

// acquire returns exactly one of a joined or new pull (release it) or a pinned committed xorb (unpin it); force clears a failure cooldown.
func (p *Peer) acquire(hash xet.XorbHash, force bool) (*pull, *lru.Entry[ready], error) {
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
		defer cancel()
		var err error
		select {
		case p.sem <- struct{}{}:
			p.mu.Lock()
			f.queued = false
			p.mu.Unlock()
			var req *http.Request
			if req, err = http.NewRequestWithContext(ctx, http.MethodGet, apis.XorbURL(p.cfg.Gateway, f.hash.String()), nil); err == nil {
				req.Header.Set(apis.PeerHeader, p.id)
				var resp *http.Response
				if resp, err = p.client.Do(req); err == nil {
					switch {
					case resp.StatusCode == http.StatusNotFound:
						err = errNotFound
					case resp.StatusCode != http.StatusOK:
						err = fmt.Errorf("gateway status %s", resp.Status)
					case resp.ContentLength > apis.MaxXorbBytes:
						err = fmt.Errorf("xorb of %d bytes exceeds cap", resp.ContentLength)
					default:
						f.stream = ioswmr.NewSWMR(ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) {
							return os.CreateTemp(p.spoolDir, f.hash.String()+"-*.part")
						}), ioswmr.WithAutoClose())
						f.hold, _ = f.stream.NewReader(0)
						f.length = resp.ContentLength
						close(f.started)
						var n int64
						n, err = io.Copy(f.stream.Writer(), io.LimitReader(resp.Body, apis.MaxXorbBytes+1))
						switch {
						case err != nil:
						case n > apis.MaxXorbBytes:
							err = errors.New("xorb exceeds cap")
						case n == 0:
							err = errEmpty
						case resp.ContentLength >= 0 && n != resp.ContentLength:
							err = fmt.Errorf("short body: %d of %d bytes", n, resp.ContentLength)
						default:
							f.size = n
							rs, _ := f.stream.NewReadSeeker(0, int(n))
							var stored bool
							stored, err = p.store.PutXorb(ctx, f.hash, rs)
							_ = rs.Close()
							if err == nil && !stored {
								// The store deduplicated against an existing object without reading this spool.
								rs, _ = f.stream.NewReadSeeker(0, int(n))
								err = xorb.Validate(rs, f.hash)
								_ = rs.Close()
							}
						}
					}
					_ = resp.Body.Close()
				}
			}
			<-p.sem
		case <-ctx.Done():
			err = ctx.Err()
		}
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
	}()
	return f, nil, nil
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

func statusOf(err error) int {
	switch {
	case errors.Is(err, errNotFound):
		return http.StatusNotFound
	case errors.Is(err, errClosed):
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}
