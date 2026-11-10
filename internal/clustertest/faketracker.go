package clustertest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// Announce is one announce a FakeTracker recorded; Code is 0 while its reply is pending.
type Announce struct {
	Req    apis.AnnounceRequest
	Header http.Header
	Code   int
}

// Lookup is one lookup a FakeTracker accepted.
type Lookup struct {
	Req    apis.LookupRequest
	Header http.Header
}

// holding is one programmed holder of a lookup key: "xorb:", "sha256:" or "file:" then the hash.
type holding struct {
	key  string
	peer apis.Peer
	file apis.File
}

// FakeTracker records announces and lookups in arrival order and answers them, lookups from what Hold and HoldShard programmed; any other path is 404.
type FakeTracker struct {
	*httptest.Server
	// Secret, when set before the first request, is the bearer token announces and lookups must carry.
	Secret string

	mu        sync.Mutex
	announces []Announce
	reply     func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)
	upstreams []apis.Peer // sorted by ID
	lookups   []Lookup
	answer    func(apis.LookupRequest) (int, apis.LookupResponse)
	holdings  []holding // in Hold and HoldShard call order
}

// NewFakeTracker serves apis.AnnouncePath and apis.LookupPath until the test ends.
func NewFakeTracker(t testing.TB) *FakeTracker {
	f := &FakeTracker{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+apis.AnnouncePath, f.serveAnnounce)
	mux.HandleFunc("POST "+apis.LookupPath, f.serveLookup)
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

// serveAnnounce records an authorized announce, then answers through the reply hook, which may block.
func (f *FakeTracker) serveAnnounce(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, f.Secret) {
		return
	}
	var req apis.AnnounceRequest
	if !apis.ReadJSON(w, r, apis.MaxAnnounceBytes, &req) {
		return
	}
	f.mu.Lock()
	i := len(f.announces)
	f.announces = append(f.announces, Announce{Req: req, Header: r.Header.Clone()})
	reply := f.reply
	upstreams := f.upstreams
	f.mu.Unlock()
	code, resp := http.StatusOK, apis.AnnounceResponse{TTL: 30, Upstreams: upstreams}
	if reply != nil {
		code, resp = reply(w, r, req)
	}
	if r.Context().Err() != nil {
		return
	}
	f.mu.Lock()
	f.announces[i].Code = code
	f.mu.Unlock()
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// SetReply answers later announces through fn, which may block; nil restores 200 with a 30s TTL and the SetUpstreams peers.
func (f *FakeTracker) SetReply(fn func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse)) {
	f.mu.Lock()
	f.reply = fn
	f.mu.Unlock()
}

// SetUpstreams makes later replies without a SetReply hook carry peers, sorted by ID, as Upstreams; no peers clears them.
func (f *FakeTracker) SetUpstreams(peers ...apis.Peer) {
	sorted := slices.SortedFunc(slices.Values(peers), func(a, b apis.Peer) int { return strings.Compare(a.ID, b.ID) })
	f.mu.Lock()
	f.upstreams = sorted
	f.mu.Unlock()
}

// Announces lists the announces received so far in arrival order.
func (f *FakeTracker) Announces() []Announce {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.announces)
}

// Announce waits for the i-th announce.
func (f *FakeTracker) Announce(t testing.TB, i int) Announce {
	t.Helper()
	WaitFor(t, WaitTimeout, fmt.Sprintf("announce %d", i), func() bool { return len(f.Announces()) > i })
	return f.Announces()[i]
}

// Replied waits for the i-th announce to be answered.
func (f *FakeTracker) Replied(t testing.TB, i int) Announce {
	t.Helper()
	WaitFor(t, WaitTimeout, fmt.Sprintf("reply to announce %d", i), func() bool {
		announces := f.Announces()
		return len(announces) > i && announces[i].Code != 0
	})
	return f.Announces()[i]
}

// FindAnnounce waits for the first announce at or after index from that matches and returns it with its index.
func (f *FakeTracker) FindAnnounce(t testing.TB, from int, match func(Announce) bool) (Announce, int) {
	t.Helper()
	var found Announce
	idx := -1
	WaitFor(t, WaitTimeout, "matching announce", func() bool {
		announces := f.Announces()
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

// serveLookup records an authorized lookup and answers each requested key with its holders other than the asker, leaving out keys without one, unless SetLookup set a hook.
func (f *FakeTracker) serveLookup(w http.ResponseWriter, r *http.Request) {
	if !apis.Authorize(w, r, f.Secret) {
		return
	}
	var req apis.LookupRequest
	if !apis.ReadJSON(w, r, apis.MaxLookupBytes, &req) {
		return
	}
	f.mu.Lock()
	f.lookups = append(f.lookups, Lookup{Req: req, Header: r.Header.Clone()})
	answer := f.answer
	resp := apis.LookupResponse{Xorbs: map[string][]apis.Peer{}, SHA256s: map[string]apis.FileLocation{}, Files: map[string]apis.FileLocation{}}
	for _, x := range req.Xorbs {
		if loc := f.locate("xorb:"+x, req.Peer); loc.Peers != nil {
			resp.Xorbs[x] = loc.Peers
		}
	}
	for _, s := range req.SHA256s {
		if loc := f.locate("sha256:"+s, req.Peer); loc.Peers != nil {
			resp.SHA256s[s] = loc
		}
	}
	for _, h := range req.Files {
		if loc := f.locate("file:"+h, req.Peer); loc.Peers != nil {
			resp.Files[h] = loc
		}
	}
	f.mu.Unlock()
	code := http.StatusOK
	if answer != nil {
		code, resp = answer(req)
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// locate lists, in programming order, the distinct holders of key other than asker, with the file the first was programmed with; f.mu is held.
func (f *FakeTracker) locate(key, asker string) apis.FileLocation {
	var loc apis.FileLocation
	for _, h := range f.holdings {
		if h.key != key || h.peer.ID == asker || slices.ContainsFunc(loc.Peers, func(p apis.Peer) bool { return p.ID == h.peer.ID }) {
			continue
		}
		if loc.Peers == nil {
			loc.File = h.file
		}
		loc.Peers = append(loc.Peers, h.peer)
	}
	return loc
}

// Hold programs p as a holder of xorbs, listed after the holders programmed before it.
func (f *FakeTracker) Hold(p apis.Peer, xorbs ...xet.XorbHash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range xorbs {
		f.holdings = append(f.holdings, holding{key: "xorb:" + x.String(), peer: p})
	}
}

// HoldShard programs p as a holder of file's shard, answered under both its SHA-256 and its hash after the holders programmed before it.
func (f *FakeTracker) HoldShard(p apis.Peer, file apis.File) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holdings = append(f.holdings, holding{key: "sha256:" + file.SHA256, peer: p, file: file}, holding{key: "file:" + file.Hash, peer: p, file: file})
}

// Forget removes the peer with ID id from everything Hold and HoldShard programmed.
func (f *FakeTracker) Forget(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holdings = slices.DeleteFunc(f.holdings, func(h holding) bool { return h.peer.ID == id })
}

// SetLookup answers later lookups through fn; nil restores answers from Hold and HoldShard.
func (f *FakeTracker) SetLookup(fn func(apis.LookupRequest) (int, apis.LookupResponse)) {
	f.mu.Lock()
	f.answer = fn
	f.mu.Unlock()
}

// Lookups lists the lookups accepted so far in arrival order.
func (f *FakeTracker) Lookups() []Lookup {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lookups)
}
