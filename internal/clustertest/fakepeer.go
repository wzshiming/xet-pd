package clustertest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// Pull is one pull a FakePeer accepted.
type Pull struct {
	Req    apis.PullRequest
	Header http.Header
}

// Request is one request a FakePeer received other than a pull.
type Request struct {
	Method, Path string
	Header       http.Header
}

// FakePeer serves the xorbs and shards it is given as a peer does and records the pulls it accepts; it records every other request too.
type FakePeer struct {
	*httptest.Server
	// Secret, when set before the first request, is the bearer token pulls and shard GETs must carry; xorb GETs stay open as on the real CAS route.
	Secret string

	id string

	mu         sync.Mutex
	xorbs      map[xet.XorbHash]http.HandlerFunc
	shards     map[string]apis.ShardResponse
	pullStatus int
	pulls      []Pull
	requests   []Request
}

// NewFakePeer serves as the peer id until the test ends, accepting pulls with 202.
func NewFakePeer(t testing.TB, id string) *FakePeer {
	f := &FakePeer{id: id, xorbs: map[xet.XorbHash]http.HandlerFunc{}, shards: map[string]apis.ShardResponse{}, pullStatus: http.StatusAccepted}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+apis.XorbPathPrefix+"{hash}", f.serveXorb)
	mux.HandleFunc("GET "+apis.ShardPathPrefix+"{file}", f.serveShard)
	mux.HandleFunc("POST "+apis.PullPath, f.servePull)
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != apis.PullPath {
			f.mu.Lock()
			f.requests = append(f.requests, Request{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone()})
			f.mu.Unlock()
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

// Peer is the identity the fake serves under.
func (f *FakePeer) Peer() apis.Peer { return apis.Peer{ID: f.id, URL: f.URL} }

// ServeXorb answers GETs and HEADs of hash with the whole body under its Content-Length, ignoring Range.
func (f *FakePeer) ServeXorb(hash xet.XorbHash, body []byte) {
	f.mu.Lock()
	f.xorbs[hash] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}
	f.mu.Unlock()
}

// GateXorb answers GETs of hash by flushing body[:prefix] under the full Content-Length, then holding the rest until release or client disconnect.
func (f *FakePeer) GateXorb(hash xet.XorbHash, body []byte, prefix int, release <-chan struct{}) {
	f.mu.Lock()
	f.xorbs[hash] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:prefix])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body[prefix:])
	}
	f.mu.Unlock()
}

func (f *FakePeer) serveXorb(w http.ResponseWriter, r *http.Request) {
	hash, _ := xet.ParseXorbHash(r.PathValue("hash"))
	f.mu.Lock()
	serve, ok := f.xorbs[hash]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	serve(w, r)
}

// ServeShard answers GETs of file's shard with resp.
func (f *FakePeer) ServeShard(file apis.File, resp apis.ShardResponse) {
	f.mu.Lock()
	f.shards[file.Hash] = resp
	f.mu.Unlock()
}

func (f *FakePeer) serveShard(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, f.Secret) {
		return
	}
	f.mu.Lock()
	resp, ok := f.shards[r.PathValue("file")]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// servePull records an authorized pull and answers it with the status SetPullStatus set.
func (f *FakePeer) servePull(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, f.Secret) {
		return
	}
	var req apis.PullRequest
	if !apis.ReadJSON(w, r, apis.MaxPullBytes, &req) {
		return
	}
	f.mu.Lock()
	f.pulls = append(f.pulls, Pull{Req: req, Header: r.Header.Clone()})
	code := f.pullStatus
	f.mu.Unlock()
	w.WriteHeader(code)
}

// SetPullStatus answers later pulls with code.
func (f *FakePeer) SetPullStatus(code int) {
	f.mu.Lock()
	f.pullStatus = code
	f.mu.Unlock()
}

// Pulls lists the pulls accepted so far in arrival order.
func (f *FakePeer) Pulls() []Pull {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pulls)
}

// Pull waits for the i-th pull.
func (f *FakePeer) Pull(t testing.TB, i int) Pull {
	t.Helper()
	WaitFor(t, WaitTimeout, fmt.Sprintf("pull %d", i), func() bool { return len(f.Pulls()) > i })
	return f.Pulls()[i]
}

// Requests lists every request but pulls received so far in arrival order.
func (f *FakePeer) Requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}
