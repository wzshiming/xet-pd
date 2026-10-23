package peer

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/lru"
)

func (p *Peer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/xorbs/{prefix}/{hash}", p.serveXorb)
	mux.HandleFunc("POST "+apis.PullPath, p.servePull)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (p *Peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

func serveContent(w http.ResponseWriter, r *http.Request, hash xet.XorbHash, content io.ReadSeeker) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+hash.String()+`"`)
	http.ServeContent(w, r, "", time.Time{}, content)
}

// serveStored serves a pinned committed xorb and releases the pin; a missing file leaves the inventory so the next request pulls again.
func (p *Peer) serveStored(w http.ResponseWriter, r *http.Request, rd *lru.Entry[xet.XorbHash, ready]) error {
	defer p.unpin(rd)
	obj, err := p.store.GetXorbReadSeekCloser(r.Context(), rd.Key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			p.drop(rd)
		}
		return err
	}
	defer func() { _ = obj.Close() }()
	serveContent(w, r, rd.Key, obj)
	return nil
}

// serveXorb serves a committed xorb or streams it from a pull; requests carrying apis.PeerHeader are relays and never pull.
func (p *Peer) serveXorb(w http.ResponseWriter, r *http.Request) {
	hash, err := xet.ParseXorbHash(r.PathValue("hash"))
	if err != nil {
		http.Error(w, "invalid xorb path", http.StatusBadRequest)
		return
	}
	if stored := p.pin(hash); stored != nil {
		err := p.serveStored(w, r, stored)
		if err == nil {
			return
		}
		if !errors.Is(err, fs.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if r.Header.Get(apis.PeerHeader) != "" {
		http.NotFound(w, r)
		return
	}
	f, stored, err := p.acquire(hash, false)
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	if stored != nil {
		if err := p.serveStored(w, r, stored); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	serveStream(w, r, f)
}

// serveStream serves the spool of an acquired pull while it may still be written, then releases the pull.
func serveStream(w http.ResponseWriter, r *http.Request, f *pull) {
	defer f.release()
	size, err := f.servable(r.Context())
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	rs, err := f.stream.NewReadSeeker(0, int(size))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = rs.Close() }()
	// Reads block on the upstream regardless of the client; closing wakes them.
	stop := context.AfterFunc(r.Context(), func() { _ = rs.Close() })
	defer stop()
	serveContent(w, r, f.hash, rs)
}

// maxPullBytes bounds one pull body, which names a single xorb.
const maxPullBytes = 4096

// servePull starts (or joins) a pull on behalf of the gateway and answers 202 at once.
func (p *Peer) servePull(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, p.cfg.Secret) {
		return
	}
	var req apis.PullRequest
	if !apis.ReadJSON(w, r, maxPullBytes, &req) {
		return
	}
	hash, err := xet.ParseXorbHash(req.Hash)
	if err != nil {
		http.Error(w, "invalid pull request", http.StatusBadRequest)
		return
	}
	f, stored, err := p.acquire(hash, true)
	if err != nil {
		http.Error(w, err.Error(), statusOf(err))
		return
	}
	if f != nil {
		f.release()
	} else {
		p.unpin(stored)
	}
	w.WriteHeader(http.StatusAccepted)
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
