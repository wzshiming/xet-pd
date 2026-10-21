package clustertest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// Announce is one announce a FakeGateway received; Code is 0 while its reply is pending.
type Announce struct {
	Req    apis.AnnounceRequest
	Header http.Header
	Code   int
}

// Request is one request a FakeGateway handed to its data handler.
type Request struct {
	Method, Path string
	Header       http.Header
}

// FakeGateway records announces in order and answers them itself; it records every other request before its data handler serves it.
type FakeGateway struct {
	*httptest.Server
	data http.HandlerFunc

	mu        sync.Mutex
	announces []Announce
	requests  []Request
	reply     func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)
}

// NewFakeGateway serves apis.AnnouncePath itself and any other path through data, which nil makes a 404.
func NewFakeGateway(t testing.TB, data http.HandlerFunc) *FakeGateway {
	if data == nil {
		data = http.NotFound
	}
	g := &FakeGateway{data: data}
	g.Server = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.Close)
	return g
}

func (g *FakeGateway) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == apis.AnnouncePath {
		g.serveAnnounce(w, r)
		return
	}
	g.mu.Lock()
	g.requests = append(g.requests, Request{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone()})
	g.mu.Unlock()
	g.data(w, r)
}

// serveAnnounce records the request after draining it, then answers through the reply hook, which may block.
func (g *FakeGateway) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	var req apis.AnnounceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	g.mu.Lock()
	i := len(g.announces)
	g.announces = append(g.announces, Announce{Req: req, Header: r.Header.Clone()})
	reply := g.reply
	g.mu.Unlock()
	code, resp := http.StatusOK, apis.AnnounceResponse{TTL: 30}
	if reply != nil {
		code, resp = reply(w, r, req)
	}
	if r.Context().Err() != nil {
		return
	}
	g.mu.Lock()
	g.announces[i].Code = code
	g.mu.Unlock()
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// SetReply answers later announces through fn, which may block; nil restores 200 with a 30s TTL.
func (g *FakeGateway) SetReply(fn func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)) {
	g.mu.Lock()
	g.reply = fn
	g.mu.Unlock()
}

// Announces lists the announces received so far in arrival order.
func (g *FakeGateway) Announces() []Announce {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.announces)
}

// Announce waits for the i-th announce.
func (g *FakeGateway) Announce(t testing.TB, i int) Announce {
	t.Helper()
	WaitFor(t, WaitTimeout, fmt.Sprintf("announce %d", i), func() bool { return len(g.Announces()) > i })
	return g.Announces()[i]
}

// FindAnnounce waits for the first announce at or after index from that matches and returns it with its index.
func (g *FakeGateway) FindAnnounce(t testing.TB, from int, match func(Announce) bool) (Announce, int) {
	t.Helper()
	var found Announce
	idx := -1
	WaitFor(t, WaitTimeout, "matching announce", func() bool {
		announces := g.Announces()
		for i := from; i < len(announces); i++ {
			if match(announces[i]) {
				found, idx = announces[i], i
				return true
			}
		}
		return false
	})
	return found, idx
}

// Requests lists the requests handed to the data handler so far in arrival order.
func (g *FakeGateway) Requests() []Request {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.requests)
}

// ServeBody answers with body under an explicit Content-Length.
func ServeBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}
}

// ServeByHash serves the body registered for the xorb hash ending the path, and 404 for any other.
func ServeByHash(bodies map[xet.XorbHash][]byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash, _ := xet.ParseXorbHash(path.Base(r.URL.Path))
		body, ok := bodies[hash]
		if !ok {
			http.NotFound(w, r)
			return
		}
		ServeBody(body)(w, r)
	}
}

// GatedBody flushes body[:prefix] under the full Content-Length, then holds the rest until release or client disconnect.
func GatedBody(body []byte, prefix int, release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
}
