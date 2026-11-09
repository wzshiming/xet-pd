package tracker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const testHash = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// post sends body to the announce route and returns the status, body and decoded response.
func post(t *testing.T, srv *httptest.Server, body string, hdr ...string) (int, string, apis.AnnounceResponse) {
	t.Helper()
	resp, text := request(t, http.MethodPost, srv.URL+apis.AnnouncePath, body, append(hdr, "Content-Type", "application/json")...)
	var out apis.AnnounceResponse
	if resp.StatusCode == http.StatusOK {
		if resp.Header.Get("Content-Type") != "application/json" || json.Unmarshal([]byte(text), &out) != nil {
			t.Fatalf("announce reply %q %q", resp.Header.Get("Content-Type"), text)
		}
	}
	return resp.StatusCode, text, out
}

func announceJSON(t *testing.T, req apis.AnnounceRequest) string {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAnnounceRegistersCanonicalInventory(t *testing.T) {
	// p1 must outlive any -race stall between the posts, or the delta remove turns into a resync.
	const ttl = time.Hour
	tr, srv := newTestTracker(t, func(cfg *Config) { cfg.PeerTTL = ttl })
	reg := trackerRegistry(tr)
	upper := strings.ToUpper(testHash)
	file := apis.File{Hash: strings.ToUpper(seedFile.Hash), SHA256: strings.ToUpper(seedFile.SHA256)}
	status, body, resp := post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1/"}, Epoch: "e1", Full: true, Add: []string{upper}, AddFiles: []apis.File{file}}))
	if status != http.StatusOK || resp.Resync || resp.TTL != int64(ttl/time.Second) {
		t.Fatalf("full announce = %d %q %+v, want ttl %d", status, body, resp, int64(ttl/time.Second))
	}
	d := clustertest.Diag(t, srv.URL)
	if len(d.Peers) != 1 || d.Peers[0].ID != "p1" || d.Peers[0].URL != "http://127.0.0.1:1" || d.Peers[0].Epoch != "e1" || d.Peers[0].Holdings != 1 || d.Peers[0].Files != 1 {
		t.Fatalf("registry = %+v, want canonical origin, one holding and one file", d.Peers)
	}
	if holders := xorbHolders(reg, testHash); len(holders) != 1 || holders[0].URL != "http://127.0.0.1:1" {
		t.Fatalf("holders of the lowercase hash = %v", holders)
	}
	if loc := reg.lookup(apis.LookupRequest{SHA256s: []string{seedFile.SHA256}}).SHA256s[seedFile.SHA256]; loc.File != seedFile || len(loc.Peers) != 1 {
		t.Fatalf("location by the lowercase SHA-256 = %+v, want the lowercase file held by p1", loc)
	}
	status, _, resp = post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e1", Remove: []string{upper}, RemoveFiles: []string{file.Hash}}))
	if status != http.StatusOK || resp.Resync || len(xorbHolders(reg, testHash)) != 0 || len(reg.fileHolders(seedFile.Hash)) != 0 {
		t.Fatalf("delta remove = %d %+v, holders %v and %v", status, resp, xorbHolders(reg, testHash), reg.fileHolders(seedFile.Hash))
	}
	status, _, resp = post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e2", Add: []string{testHash}}))
	if status != http.StatusOK || !resp.Resync || len(clustertest.Diag(t, srv.URL).Peers) != 0 {
		t.Fatalf("delta from a new epoch = %d %+v, want resync and the peer dropped", status, resp)
	}
	status, _, resp = post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e2", Full: true}))
	if d := clustertest.Diag(t, srv.URL); status != http.StatusOK || resp.Resync || len(d.Peers) != 1 || d.Peers[0].Holdings != 0 {
		t.Fatalf("empty full = %d %+v %+v", status, resp, d.Peers)
	}
}

// The node a peer names is dropped before any compare or store.
func TestAnnounceIgnoresNode(t *testing.T) {
	_, srv := newTestTracker(t, func(cfg *Config) { cfg.PeerTTL = time.Hour })
	peer := apis.Peer{ID: "p1", URL: "http://127.0.0.1:1", Node: "worker-1"}
	if status, body, resp := post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true})); status != http.StatusOK || resp.Resync {
		t.Fatalf("full announce from worker-1 = %d %q %+v", status, body, resp)
	}
	for _, node := range []string{"worker-2", "worker 1", strings.Repeat("n", 254), ""} {
		moved := peer
		moved.Node = node
		if status, body, resp := post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: moved, Epoch: "e"})); status != http.StatusOK || resp.Resync {
			t.Fatalf("delta naming node %q = %d %q %+v, want it accepted without resync", node, status, body, resp)
		}
	}
	if _, text := request(t, http.MethodGet, srv.URL+apis.PeersPath, ""); !strings.Contains(text, `"id":"p1"`) || strings.Contains(text, `"node"`) {
		t.Fatalf("registry = %s, want p1 without a node key", text)
	}
}

func TestAnnounceReplyCarriesMinSeeds(t *testing.T) {
	_, srv := newTestTracker(t, func(cfg *Config) { cfg.MinSeeds = 2 })
	req := apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e", Full: true}
	if status, body, resp := post(t, srv, announceJSON(t, req)); status != http.StatusOK || resp.MinSeeds != 2 || !strings.Contains(body, `"min_seeds":2`) {
		t.Fatalf("full announce = %d %q, want min_seeds 2", status, body)
	}
	req.Full, req.Epoch = false, "other"
	if status, body, resp := post(t, srv, announceJSON(t, req)); status != http.StatusOK || !resp.Resync || resp.MinSeeds != 2 {
		t.Fatalf("resync = %d %q, want min_seeds 2", status, body)
	}
}

func TestAnnounceRejectsInvalidRequestsAtomically(t *testing.T) {
	tr, srv := newTestTracker(t, nil)
	reg := trackerRegistry(tr)
	peer := apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}
	valid := apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash}, AddFiles: []apis.File{seedFile}}
	withFiles := func(remove []string, add ...apis.File) string {
		return announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, AddFiles: add, RemoveFiles: remove})
	}
	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"empty body", "", http.StatusBadRequest},
		{"not json", "nope", http.StatusBadRequest},
		{"empty object", "{}", http.StatusBadRequest},
		{"trailing data", announceJSON(t, valid) + " {}", http.StatusBadRequest},
		{"empty id", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{URL: peer.URL}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"id with space", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p 1", URL: peer.URL}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"long id", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: strings.Repeat("p", 129), URL: peer.URL}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"empty epoch", announceJSON(t, apis.AnnounceRequest{Peer: peer, Full: true}), http.StatusBadRequest},
		{"epoch with newline", announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e\n", Full: true}), http.StatusBadRequest},
		{"missing url", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url path", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1/base"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url userinfo", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://u:p@127.0.0.1:1"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url query", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1?x"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url fragment", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1#f"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url scheme", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "ftp://127.0.0.1:1"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url empty host", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://:1234"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url empty port", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url port zero", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:0"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url port range", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:65536"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"url credentials unparsable", announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://bob:hunter2@127.0.0.1:1/%zz"}, Epoch: "e", Full: true}), http.StatusBadRequest},
		{"short hash", announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{"abc"}}), http.StatusBadRequest},
		{"non-hex hash", announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{strings.Repeat("zz", 32)}}), http.StatusBadRequest},
		{"one bad hash among good", announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash, "abc"}}), http.StatusBadRequest},
		{"bad remove hash", announceJSON(t, apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash}, Remove: []string{"abc"}}), http.StatusBadRequest},
		{"short file hash", withFiles(nil, apis.File{Hash: "abc", SHA256: seedFile.SHA256}), http.StatusBadRequest},
		{"non-hex file hash", withFiles(nil, apis.File{Hash: strings.Repeat("zz", 32), SHA256: seedFile.SHA256}), http.StatusBadRequest},
		{"one bad file among good", withFiles(nil, seedFile, apis.File{Hash: "abc", SHA256: seedFile.SHA256}), http.StatusBadRequest},
		{"missing sha256", withFiles(nil, apis.File{Hash: seedFile.Hash}), http.StatusBadRequest},
		{"short sha256", withFiles(nil, apis.File{Hash: seedFile.Hash, SHA256: "abc"}), http.StatusBadRequest},
		{"long sha256", withFiles(nil, apis.File{Hash: seedFile.Hash, SHA256: seedFile.SHA256 + "00"}), http.StatusBadRequest},
		{"non-hex sha256", withFiles(nil, apis.File{Hash: seedFile.Hash, SHA256: strings.Repeat("zz", 32)}), http.StatusBadRequest},
		{"bad remove file hash", withFiles([]string{"abc"}, seedFile), http.StatusBadRequest},
		{"oversize", `{"peer":{"id":"` + strings.Repeat("p", 33<<20) + `"}}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		status, body, _ := post(t, srv, tt.body)
		if status != tt.status {
			t.Fatalf("%s: status %d %q, want %d", tt.name, status, body, tt.status)
		}
		if strings.Contains(body, testHash) || strings.Contains(body, "http://") || strings.Contains(body, "hunter2") {
			t.Fatalf("%s: error echoes request data: %q", tt.name, body)
		}
	}
	if d := clustertest.Diag(t, srv.URL); len(d.Peers) != 0 || len(xorbHolders(reg, testHash)) != 0 || len(reg.fileHolders(seedFile.Hash)) != 0 {
		t.Fatalf("registry changed by invalid announces: %+v", d.Peers)
	}
}

// A body padded with whitespace to the limit is accepted; one byte more is 413 whether the overflow surfaces on the value or on the trailing check.
func TestAnnounceBodyLimit(t *testing.T) {
	// The 32 MiB posts can outlast the default TTL under -race load.
	tr, srv := newTestTracker(t, func(cfg *Config) { cfg.PeerTTL = time.Hour })
	reg := trackerRegistry(tr)
	pad := func(req apis.AnnounceRequest, n int) string {
		body := announceJSON(t, req)
		return body + strings.Repeat(" ", n-len(body))
	}
	peer := apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}
	if status, body, _ := post(t, srv, pad(apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash}}, apis.MaxAnnounceBytes)); status != http.StatusOK {
		t.Fatalf("announce at the limit = %d %q", status, body)
	}
	status, body, _ := post(t, srv, pad(apis.AnnounceRequest{Peer: peer, Epoch: "e", Remove: []string{testHash}}, apis.MaxAnnounceBytes+1))
	if status != http.StatusRequestEntityTooLarge || strings.Contains(body, testHash) {
		t.Fatalf("announce over the limit = %d %q, want 413", status, body)
	}
	if len(xorbHolders(reg, testHash)) != 1 {
		t.Fatal("oversize delta changed the registry")
	}
}

func TestAnnounceRequiresSecret(t *testing.T) {
	const secret = "s3cret"
	tr, srv := newTestTracker(t, func(cfg *Config) { cfg.Secret = secret })
	reg := trackerRegistry(tr)
	body := announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e", Full: true, Add: []string{testHash}})
	for _, hdr := range [][]string{nil, {"Authorization", "Bearer nope"}, {"Authorization", "Basic " + secret}, {"Authorization", "Bearer " + secret + "x"}} {
		if status, text, _ := post(t, srv, body, hdr...); status != http.StatusUnauthorized || strings.Contains(text, secret) {
			t.Fatalf("announce with %v = %d %q, want 401", hdr, status, text)
		}
	}
	if len(xorbHolders(reg, testHash)) != 0 {
		t.Fatal("unauthenticated announce changed the registry")
	}
	if status, _, resp := post(t, srv, body, "Authorization", "Bearer "+secret); status != http.StatusOK || resp != (apis.AnnounceResponse{TTL: 30, MinSeeds: 1}) || len(xorbHolders(reg, testHash)) != 1 {
		t.Fatalf("authenticated announce = %d %+v", status, resp)
	}
	if _, text := request(t, http.MethodGet, srv.URL+apis.PeersPath, ""); strings.Contains(text, secret) {
		t.Fatalf("diagnostics leak the secret: %q", text)
	}
}
