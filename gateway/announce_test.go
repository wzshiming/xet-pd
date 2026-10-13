package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
	"github.com/wzshiming/xet-pd/internal/hubtest"
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
	g, srv := newTestGateway(t, hubtest.New(t), func(cfg *Config, _ string) { cfg.PeerTTL = 2 * time.Second })
	upper := strings.ToUpper(testHash)
	status, body, resp := post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1/"}, Epoch: "e1", Full: true, Add: []string{upper}}))
	if status != http.StatusOK || resp.Resync || resp.TTL != 2 {
		t.Fatalf("full announce = %d %q %+v, want ttl 2", status, body, resp)
	}
	d := clustertest.Diag(t, srv.URL)
	if len(d.Peers) != 1 || d.Peers[0].ID != "p1" || d.Peers[0].URL != "http://127.0.0.1:1" || d.Peers[0].Epoch != "e1" || d.Peers[0].Holdings != 1 {
		t.Fatalf("registry = %+v, want canonical origin and one holding", d.Peers)
	}
	if holders := g.reg.holders(testHash); len(holders) != 1 || holders[0].URL != "http://127.0.0.1:1" {
		t.Fatalf("holders of the lowercase hash = %v", holders)
	}
	status, _, resp = post(t, srv, announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e1", Remove: []string{upper}}))
	if status != http.StatusOK || resp.Resync || g.reg.aliveHolders(testHash) != 0 {
		t.Fatalf("delta remove = %d %+v, holders %d", status, resp, g.reg.aliveHolders(testHash))
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

func TestAnnounceRejectsInvalidRequestsAtomically(t *testing.T) {
	g, srv := newTestGateway(t, hubtest.New(t), nil)
	peer := apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}
	valid := apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash}}
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
	if d := clustertest.Diag(t, srv.URL); len(d.Peers) != 0 || g.reg.aliveHolders(testHash) != 0 {
		t.Fatalf("registry changed by invalid announces: %+v", d.Peers)
	}
}

// A body padded with whitespace to the limit is accepted; one byte more is 413 whether the overflow surfaces on the value or on the trailing check.
func TestAnnounceBodyLimit(t *testing.T) {
	// The 32 MiB posts can outlast the default TTL under -race load.
	g, srv := newTestGateway(t, hubtest.New(t), func(cfg *Config, _ string) { cfg.PeerTTL = time.Hour })
	pad := func(req apis.AnnounceRequest, n int) string {
		body := announceJSON(t, req)
		return body + strings.Repeat(" ", n-len(body))
	}
	peer := apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}
	if status, body, _ := post(t, srv, pad(apis.AnnounceRequest{Peer: peer, Epoch: "e", Full: true, Add: []string{testHash}}, maxAnnounceBytes)); status != http.StatusOK {
		t.Fatalf("announce at the limit = %d %q", status, body)
	}
	status, body, _ := post(t, srv, pad(apis.AnnounceRequest{Peer: peer, Epoch: "e", Remove: []string{testHash}}, maxAnnounceBytes+1))
	if status != http.StatusRequestEntityTooLarge || strings.Contains(body, testHash) {
		t.Fatalf("announce over the limit = %d %q, want 413", status, body)
	}
	if g.reg.aliveHolders(testHash) != 1 {
		t.Fatal("oversize delta changed the registry")
	}
}

func TestAnnounceRequiresSecret(t *testing.T) {
	const secret = "s3cret"
	g, srv := newTestGateway(t, hubtest.New(t), func(cfg *Config, _ string) { cfg.Secret = secret })
	body := announceJSON(t, apis.AnnounceRequest{Peer: apis.Peer{ID: "p1", URL: "http://127.0.0.1:1"}, Epoch: "e", Full: true, Add: []string{testHash}})
	for _, hdr := range [][]string{nil, {"Authorization", "Bearer nope"}, {"Authorization", "Basic " + secret}, {"Authorization", "Bearer " + secret + "x"}} {
		if status, text, _ := post(t, srv, body, hdr...); status != http.StatusUnauthorized || strings.Contains(text, secret) {
			t.Fatalf("announce with %v = %d %q, want 401", hdr, status, text)
		}
	}
	if g.reg.aliveHolders(testHash) != 0 {
		t.Fatal("unauthenticated announce changed the registry")
	}
	if status, _, resp := post(t, srv, body, "Authorization", "Bearer "+secret); status != http.StatusOK || resp.TTL != 30 || g.reg.aliveHolders(testHash) != 1 {
		t.Fatalf("authenticated announce = %d %+v", status, resp)
	}
	if _, text := request(t, http.MethodGet, srv.URL+apis.PeersPath, ""); strings.Contains(text, secret) {
		t.Fatalf("diagnostics leak the secret: %q", text)
	}
}
