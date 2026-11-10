package clustertest_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

var (
	peerA = apis.Peer{ID: "a", URL: "http://a"}
	peerB = apis.Peer{ID: "b", URL: "http://b"}
	peerC = apis.Peer{ID: "c", URL: "http://c"}
	file1 = apis.File{Hash: strings.Repeat("f", 64), SHA256: strings.Repeat("e", 64)}
)

// xorbHash is the valid xorb hash spelled by 64 copies of digit.
func xorbHash(t *testing.T, digit string) xet.XorbHash {
	h, err := xet.ParseXorbHash(strings.Repeat(digit, 64))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// lookup sends req to tr under tr.Secret and fails the test unless it is answered 200.
func lookup(t *testing.T, tr *clustertest.FakeTracker, req apis.LookupRequest) apis.LookupResponse {
	t.Helper()
	resp, err := apis.Lookup(t.Context(), tr.Client(), tr.URL, tr.Secret, req)
	if err != nil {
		t.Fatalf("Lookup %+v: %v", req, err)
	}
	return resp
}

func TestFakeTrackerRecordsAnnounces(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	req := apis.AnnounceRequest{Peer: peerA, Epoch: "e1", Full: true, AddFiles: []apis.File{file1}}
	resp, err := apis.Announce(t.Context(), tr.Client(), tr.URL, "s3cret", req)
	if err != nil || resp != (apis.AnnounceResponse{TTL: 30}) {
		t.Fatalf("Announce = %+v, %v, want TTL 30", resp, err)
	}
	if a := tr.Replied(t, 0); !reflect.DeepEqual(a.Req, req) || a.Code != http.StatusOK || a.Header.Get("Authorization") != "Bearer s3cret" {
		t.Fatalf("recorded %+v, want the request with its header, answered 200", a)
	}
}

func TestFakeTrackerSetReply(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	tr.SetReply(func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusServiceUnavailable, apis.AnnounceResponse{}
	})
	if _, err := apis.Announce(t.Context(), tr.Client(), tr.URL, "", apis.AnnounceRequest{Epoch: "e1"}); err == nil {
		t.Fatal("Announce answered 503 = nil error")
	}
	tr.SetReply(nil)
	if resp, err := apis.Announce(t.Context(), tr.Client(), tr.URL, "", apis.AnnounceRequest{Epoch: "e2"}); err != nil || resp.TTL != 30 {
		t.Fatalf("Announce after SetReply(nil) = %+v, %v, want TTL 30", resp, err)
	}
	if a, i := tr.FindAnnounce(t, 0, func(a clustertest.Announce) bool { return a.Req.Epoch == "e1" }); i != 0 || a.Code != http.StatusServiceUnavailable {
		t.Fatalf("FindAnnounce e1 = %+v at %d, want index 0 answered 503", a, i)
	}
	if got := tr.Announces(); len(got) != 2 || got[1].Req.Epoch != "e2" || got[1].Code != http.StatusOK {
		t.Fatalf("Announces = %+v, want e1 then e2 answered 200", got)
	}
}

func TestFakeTrackerLookupListsHoldersInHoldOrder(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	x := xorbHash(t, "1")
	tr.Hold(peerB, x)
	tr.Hold(peerA, x)
	tr.Hold(peerB, x)
	got := lookup(t, tr, apis.LookupRequest{Peer: "c", Xorbs: []string{x.String()}})
	if want := map[string][]apis.Peer{x.String(): {peerB, peerA}}; !reflect.DeepEqual(got.Xorbs, want) {
		t.Fatalf("Xorbs = %+v, want %+v", got.Xorbs, want)
	}
}

func TestFakeTrackerLookupExcludesTheAsker(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	x1, x2, unknown := xorbHash(t, "1"), xorbHash(t, "2"), xorbHash(t, "3")
	tr.Hold(peerA, x1, x2)
	tr.Hold(peerB, x1)
	tr.HoldFile(peerA, file1)
	got := lookup(t, tr, apis.LookupRequest{Peer: "a", Xorbs: []string{x1.String(), x2.String(), unknown.String()}, Files: []string{file1.Hash}})
	if want := (apis.LookupResponse{Xorbs: map[string][]apis.Peer{x1.String(): {peerB}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Lookup as a = %+v, want %+v", got, want)
	}
}

func TestFakeTrackerHoldFileAnswersBothHashes(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	tr.HoldFile(peerC, file1)
	tr.HoldFile(peerB, file1)
	got := lookup(t, tr, apis.LookupRequest{Peer: "a", SHA256s: []string{file1.SHA256}, Files: []string{file1.Hash}})
	loc := apis.FileLocation{File: file1, Peers: []apis.Peer{peerC, peerB}}
	if want := (apis.LookupResponse{SHA256s: map[string]apis.FileLocation{file1.SHA256: loc}, Files: map[string]apis.FileLocation{file1.Hash: loc}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Lookup = %+v, want %+v", got, want)
	}
}

func TestFakeTrackerForget(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	x := xorbHash(t, "1")
	tr.Hold(peerA, x)
	tr.Hold(peerB, x)
	tr.HoldFile(peerA, file1)
	tr.Forget("a")
	got := lookup(t, tr, apis.LookupRequest{Xorbs: []string{x.String()}, Files: []string{file1.Hash}})
	if want := (apis.LookupResponse{Xorbs: map[string][]apis.Peer{x.String(): {peerB}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Lookup after Forget(a) = %+v, want %+v", got, want)
	}
}

func TestFakeTrackerSetLookup(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	x := xorbHash(t, "1")
	tr.Hold(peerA, x)
	canned := apis.LookupResponse{Files: map[string]apis.FileLocation{file1.Hash: {File: file1, Peers: []apis.Peer{peerC}}}}
	asked := make(chan apis.LookupRequest, 1)
	tr.SetLookup(func(req apis.LookupRequest) (int, apis.LookupResponse) {
		asked <- req
		return http.StatusOK, canned
	})
	req := apis.LookupRequest{Peer: "b", Xorbs: []string{x.String()}}
	if got := lookup(t, tr, req); !reflect.DeepEqual(got, canned) {
		t.Fatalf("Lookup through SetLookup = %+v, want %+v", got, canned)
	}
	if got := <-asked; !reflect.DeepEqual(got, req) {
		t.Fatalf("SetLookup hook got %+v, want %+v", got, req)
	}
	tr.SetLookup(func(apis.LookupRequest) (int, apis.LookupResponse) {
		return http.StatusServiceUnavailable, apis.LookupResponse{}
	})
	if _, err := apis.Lookup(t.Context(), tr.Client(), tr.URL, "", req); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("Lookup answered 503 = %v, want an error naming the status", err)
	}
	tr.SetLookup(nil)
	if got, want := lookup(t, tr, req), map[string][]apis.Peer{x.String(): {peerA}}; !reflect.DeepEqual(got.Xorbs, want) {
		t.Fatalf("Lookup after SetLookup(nil) = %+v, want Xorbs %+v", got, want)
	}
}

func TestFakeTrackerRecordsLookups(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	req := apis.LookupRequest{Peer: "a", SHA256s: []string{file1.SHA256}}
	if _, err := apis.Lookup(t.Context(), tr.Client(), tr.URL, "s3cret", req); err != nil {
		t.Fatal(err)
	}
	if got := tr.Lookups(); len(got) != 1 || !reflect.DeepEqual(got[0].Req, req) || got[0].Header.Get("Authorization") != "Bearer s3cret" {
		t.Fatalf("Lookups = %+v, want the one request with its header", got)
	}
}

func TestFakeTrackerSecret(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	tr.Secret = "s3cret"
	if _, err := apis.Announce(t.Context(), tr.Client(), tr.URL, "", apis.AnnounceRequest{}); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Errorf("Announce without the secret = %v, want 401", err)
	}
	if _, err := apis.Lookup(t.Context(), tr.Client(), tr.URL, "wrong", apis.LookupRequest{}); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Errorf("Lookup with a wrong secret = %v, want 401", err)
	}
	if len(tr.Announces()) != 0 || len(tr.Lookups()) != 0 {
		t.Errorf("recorded %d announces and %d lookups, want rejected requests unrecorded", len(tr.Announces()), len(tr.Lookups()))
	}
	if _, err := apis.Announce(t.Context(), tr.Client(), tr.URL, "s3cret", apis.AnnounceRequest{}); err != nil {
		t.Errorf("Announce with the secret = %v", err)
	}
	if _, err := apis.Lookup(t.Context(), tr.Client(), tr.URL, "s3cret", apis.LookupRequest{}); err != nil {
		t.Errorf("Lookup with the secret = %v", err)
	}
}

func TestFakeTrackerAnswersOtherPaths404(t *testing.T) {
	t.Parallel()
	tr := clustertest.NewFakeTracker(t)
	for _, path := range []string{"/", apis.PeersPath, apis.PullPath} {
		resp, err := tr.Client().Get(tr.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}
}

// fetch sends method to url, with peer as apis.PeerHeader when set, and returns the status, Content-Length and body.
func fetch(t *testing.T, method, url, peer string) (int, int64, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if peer != "" {
		req.Header.Set(apis.PeerHeader, peer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.ContentLength, body
}

func TestFakePeerIdentity(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	if got, want := p.Peer(), (apis.Peer{ID: "p1", URL: p.URL}); got != want {
		t.Fatalf("Peer() = %+v, want %+v", got, want)
	}
}

func TestFakePeerServesXorbs(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	x, body := xorbHash(t, "1"), []byte("xorb bytes")
	p.ServeXorb(x, body)
	url := apis.XorbURL(p.URL, x.String())
	if code, size, got := fetch(t, http.MethodGet, url, ""); code != http.StatusOK || size != int64(len(body)) || !bytes.Equal(got, body) {
		t.Errorf("GET = %d, Content-Length %d, %q; want 200, %d, %q", code, size, got, len(body), body)
	}
	if code, size, got := fetch(t, http.MethodHead, url, ""); code != http.StatusOK || size != int64(len(body)) || len(got) != 0 {
		t.Errorf("HEAD = %d, Content-Length %d, %q; want 200, %d, no body", code, size, got, len(body))
	}
	if code, _, _ := fetch(t, http.MethodGet, apis.XorbURL(p.URL, xorbHash(t, "2").String()), ""); code != http.StatusNotFound {
		t.Errorf("GET of an unknown xorb = %d, want 404", code)
	}
}

func TestFakePeerGatesXorbs(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	x, body := xorbHash(t, "1"), []byte("prefix, then the rest")
	release := make(chan struct{})
	p.GateXorb(x, body, len("prefix"), release)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, apis.XorbURL(p.URL, x.String()), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	prefix := make([]byte, len("prefix"))
	if _, err := io.ReadFull(resp.Body, prefix); err != nil || string(prefix) != "prefix" || resp.ContentLength != int64(len(body)) {
		t.Fatalf("before release read %q, %v under Content-Length %d; want %q under %d", prefix, err, resp.ContentLength, "prefix", len(body))
	}
	rest := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(resp.Body)
		rest <- string(b)
	}()
	select {
	case got := <-rest:
		t.Fatalf("read %q before release", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if got := <-rest; got != ", then the rest" {
		t.Fatalf("after release read %q, want %q", got, ", then the rest")
	}
}

func TestFakePeerServesShards(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	shard := apis.ShardResponse{Shard: []byte("shard"), Offsets: map[string][]uint64{xorbHash(t, "1").String(): {10, 25}}}
	p.ServeShard(file1, shard)
	if got, err := apis.GetShard(t.Context(), p.Client(), p.URL, "", "me", file1.Hash); err != nil || !reflect.DeepEqual(got, shard) {
		t.Fatalf("GetShard = %+v, %v, want %+v", got, err, shard)
	}
	if _, err := apis.GetShard(t.Context(), p.Client(), p.URL, "", "me", strings.Repeat("0", 64)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard of an unknown file = %v, want fs.ErrNotExist", err)
	}
}

func TestFakePeerRecordsPulls(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	req := apis.PullRequest{File: file1.Hash, Seed: true}
	if err := apis.Pull(t.Context(), p.Client(), p.URL, "s3cret", req); err != nil {
		t.Fatalf("Pull = %v, want accepted", err)
	}
	if got := p.Pull(t, 0); got.Req != req || got.Header.Get("Authorization") != "Bearer s3cret" {
		t.Fatalf("Pull(0) = %+v, want %+v with its header", got, req)
	}
	p.SetPullStatus(http.StatusServiceUnavailable)
	if err := apis.Pull(t.Context(), p.Client(), p.URL, "", apis.PullRequest{File: "aa"}); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("Pull after SetPullStatus(503) = %v, want an error naming the status", err)
	}
	if got := p.Pulls(); len(got) != 2 || got[1].Req.File != "aa" {
		t.Fatalf("Pulls = %+v, want both pulls in order", got)
	}
}

func TestFakePeerSecret(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	p.Secret = "s3cret"
	x := xorbHash(t, "1")
	p.ServeXorb(x, []byte("open"))
	p.ServeShard(file1, apis.ShardResponse{})
	if err := apis.Pull(t.Context(), p.Client(), p.URL, "", apis.PullRequest{}); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Errorf("Pull without the secret = %v, want 401", err)
	}
	if _, err := apis.GetShard(t.Context(), p.Client(), p.URL, "wrong", "me", file1.Hash); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Errorf("GetShard with a wrong secret = %v, want 401", err)
	}
	if len(p.Pulls()) != 0 {
		t.Errorf("recorded %d pulls, want rejected pulls unrecorded", len(p.Pulls()))
	}
	if code, _, _ := fetch(t, http.MethodGet, apis.XorbURL(p.URL, x.String()), ""); code != http.StatusOK {
		t.Errorf("GET xorb without the secret = %d, want 200", code)
	}
	if err := apis.Pull(t.Context(), p.Client(), p.URL, "s3cret", apis.PullRequest{}); err != nil {
		t.Errorf("Pull with the secret = %v", err)
	}
	if _, err := apis.GetShard(t.Context(), p.Client(), p.URL, "s3cret", "me", file1.Hash); err != nil {
		t.Errorf("GetShard with the secret = %v", err)
	}
}

func TestFakePeerRecordsRequests(t *testing.T) {
	t.Parallel()
	p := clustertest.NewFakePeer(t, "p1")
	x := xorbHash(t, "1")
	fetch(t, http.MethodGet, apis.XorbURL(p.URL, x.String()), "p2")
	_, _ = apis.GetShard(t.Context(), p.Client(), p.URL, "", "p3", file1.Hash)
	_ = apis.Pull(t.Context(), p.Client(), p.URL, "", apis.PullRequest{File: file1.Hash})
	got := p.Requests()
	want := []struct{ method, path, peer string }{
		{http.MethodGet, apis.XorbPathPrefix + x.String(), "p2"},
		{http.MethodGet, apis.ShardPathPrefix + file1.Hash, "p3"},
	}
	if len(got) != len(want) {
		t.Fatalf("Requests = %+v, want the xorb and shard GETs only", got)
	}
	for i, w := range want {
		if got[i].Method != w.method || got[i].Path != w.path || got[i].Header.Get(apis.PeerHeader) != w.peer {
			t.Errorf("Requests[%d] = %s %s from %q, want %s %s from %q", i, got[i].Method, got[i].Path, got[i].Header.Get(apis.PeerHeader), w.method, w.path, w.peer)
		}
	}
}
