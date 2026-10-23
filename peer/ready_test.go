package peer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// readyz returns the status and body of GET /readyz.
func readyz(t *testing.T, srv *httptest.Server) (int, string) {
	t.Helper()
	resp, body, err := do(t, http.MethodGet, srv.URL+"/readyz")
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestReadyzWaitsForValidation(t *testing.T) {
	dir := t.TempDir()
	seedBacklog(t, dir)
	p, srv := newTestPeer(t, clustertest.NewFakeGateway(t, nil).URL, func(c *Config) { c.CacheDir = dir })
	if code, body := readyz(t, srv); code != http.StatusServiceUnavailable || !strings.Contains(body, "validating") {
		t.Fatalf("GET /readyz during startup validation = %d %q, want 503 naming the validation", code, body)
	}
	clustertest.WaitFor(t, validationTimeout, "startup validation", p.Validated)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "an acknowledged announce", p.Announced)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		if resp, body, err := do(t, method, srv.URL+"/readyz"); err != nil || resp.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("%s /readyz once validated and acknowledged = %d %q %v, want 200 with no body", method, resp.StatusCode, body, err)
		}
	}
}

func TestReadyzTracksAnnounce(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 120, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, nil)
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	clustertest.WaitFor(t, clustertest.WaitTimeout, "an acknowledged announce", p.Announced)
	if code, msg := readyz(t, srv); code != http.StatusOK {
		t.Fatalf("GET /readyz once acknowledged = %d %q, want 200", code, msg)
	}
	g.SetReply(func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusInternalServerError, apis.AnnounceResponse{}
	})
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a refused announce", func() bool { return !p.Announced() })
	if code, msg := readyz(t, srv); code != http.StatusServiceUnavailable || !strings.Contains(msg, "announce") {
		t.Fatalf("GET /readyz after a refused announce = %d %q, want 503 naming the announce", code, msg)
	}
	g.SetReply(nil)
	commitDirect(t, p, hash, body) // wakes the announcer out of its backoff
	clustertest.WaitFor(t, clustertest.WaitTimeout, "readyz after the next acknowledged announce", func() bool {
		code, _ := readyz(t, srv)
		return code == http.StatusOK
	})
}

func TestReadyzRejectsOtherMethods(t *testing.T) {
	_, srv := newTestPeer(t, clustertest.NewFakeGateway(t, nil).URL, nil)
	resp, body, _ := do(t, http.MethodPost, srv.URL+"/readyz")
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST /readyz = %d Allow %q %q, want 405 Allow %q", resp.StatusCode, resp.Header.Get("Allow"), body, "GET, HEAD")
	}
}
