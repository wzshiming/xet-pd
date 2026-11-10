package tracker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

// postLookup sends body to the lookup route and returns the status, body and decoded response.
func postLookup(t *testing.T, srv *httptest.Server, body string, hdr ...string) (int, string, apis.LookupResponse) {
	t.Helper()
	resp, text := request(t, http.MethodPost, srv.URL+apis.LookupPath, body, append(hdr, "Content-Type", "application/json")...)
	var out apis.LookupResponse
	if resp.StatusCode == http.StatusOK {
		if resp.Header.Get("Content-Type") != "application/json" || json.Unmarshal([]byte(text), &out) != nil {
			t.Fatalf("lookup reply %q %q", resp.Header.Get("Content-Type"), text)
		}
	}
	return resp.StatusCode, text, out
}

func TestLookupRequiresSecret(t *testing.T) {
	const secret = "s3cret"
	_, srv := newTestTracker(t, func(cfg *Config) { cfg.Secret = secret })
	body := `{"xorbs":["` + testHash + `"]}`
	for _, hdr := range [][]string{nil, {"Authorization", "Bearer nope"}, {"Authorization", "Basic " + secret}} {
		if status, text, _ := postLookup(t, srv, body, hdr...); status != http.StatusUnauthorized || strings.Contains(text, secret) {
			t.Fatalf("lookup with %v = %d %q, want 401", hdr, status, text)
		}
	}
	if status, text, _ := postLookup(t, srv, body, "Authorization", "Bearer "+secret); status != http.StatusOK {
		t.Fatalf("authenticated lookup = %d %q, want 200", status, text)
	}
}

func TestLookupRejectsBadHashes(t *testing.T) {
	_, srv := newTestTracker(t, nil)
	list := func(key string, hashes ...string) string {
		return `{"` + key + `":["` + strings.Join(hashes, `","`) + `"]}`
	}
	tests := []struct{ name, body string }{
		{"not json", "nope"},
		{"trailing data", "{} {}"},
		{"short xorb", list("xorbs", "abc")},
		{"non-hex xorb", list("xorbs", strings.Repeat("zz", 32))},
		{"one bad xorb among good", list("xorbs", testHash, "abc")},
		{"short file", list("files", "abc")},
		{"non-hex file", list("files", strings.Repeat("zz", 32))},
		{"short sha256", list("sha256s", "abc")},
		{"long sha256", list("sha256s", seedFile.SHA256+"00")},
		{"non-hex sha256", list("sha256s", strings.Repeat("zz", 32))},
	}
	for _, tt := range tests {
		if status, text, _ := postLookup(t, srv, tt.body); status != http.StatusBadRequest {
			t.Fatalf("%s: lookup = %d %q, want 400", tt.name, status, text)
		}
	}
}

// A body padded with whitespace to the limit is answered; one byte more is 413.
func TestLookupBodyLimit(t *testing.T) {
	_, srv := newTestTracker(t, nil)
	body := `{"xorbs":["` + testHash + `"]}`
	if status, text, _ := postLookup(t, srv, body+strings.Repeat(" ", apis.MaxLookupBytes-len(body))); status != http.StatusOK {
		t.Fatalf("lookup at the limit = %d %q", status, text)
	}
	if status, text, _ := postLookup(t, srv, body+strings.Repeat(" ", apis.MaxLookupBytes+1-len(body))); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("lookup over the limit = %d %q, want 413", status, text)
	}
}

// The answer keys canonical hashes, leaves out the asker and every key without another live holder.
func TestLookupAnswerShape(t *testing.T) {
	_, srv := newTestTracker(t, func(cfg *Config) { cfg.PeerTTL = time.Hour })
	for _, id := range []string{"p1", "p2"} {
		req := apis.AnnounceRequest{Peer: apis.Peer{ID: id, URL: "http://" + id}, Epoch: "e", Full: true, Add: []string{testHash}, AddFiles: []apis.File{seedFile}}
		if status, body, _ := post(t, srv, announceJSON(t, req)); status != http.StatusOK {
			t.Fatalf("announce of %s = %d %q", id, status, body)
		}
	}
	unknown := strings.Repeat("11", 32)
	req := apis.LookupRequest{
		Peer:    "p1",
		Xorbs:   []string{strings.ToUpper(testHash), unknown},
		Files:   []string{strings.ToUpper(seedFile.Hash), unknown},
		SHA256s: []string{strings.ToUpper(seedFile.SHA256), unknown},
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	loc := `{"hash":"` + seedFile.Hash + `","sha256":"` + seedFile.SHA256 + `","peers":[{"id":"p2","url":"http://p2"}]}`
	want := `{"xorbs":{"` + testHash + `":[{"id":"p2","url":"http://p2"}]},"sha256s":{"` + seedFile.SHA256 + `":` + loc + `},"files":{"` + seedFile.Hash + `":` + loc + `}}` + "\n"
	if status, text, _ := postLookup(t, srv, string(b)); status != http.StatusOK || text != want {
		t.Fatalf("lookup by p1 = %d %s, want %s", status, text, want)
	}
	req.Peer = "p3"
	req.Xorbs, req.Files, req.SHA256s = []string{unknown}, []string{unknown}, []string{unknown}
	if b, err = json.Marshal(req); err != nil {
		t.Fatal(err)
	}
	if status, text, _ := postLookup(t, srv, string(b)); status != http.StatusOK || text != "{}\n" {
		t.Fatalf("lookup of keys nobody holds = %d %q, want every key left out", status, text)
	}
}
