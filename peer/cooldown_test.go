package peer

import (
	"bytes"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// A failed pull is refused for the configured cooldown only; the first GET after it pulls again.
func TestFailureCooldownExpires(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 110, 2, 8<<10)
	var gets atomic.Int32
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if gets.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		clustertest.ServeBody(body)(w, r)
	})
	_, srv := newTestPeer(t, g.URL, func(c *Config) { c.FailureCooldown = 200 * time.Millisecond })
	u := xorbURL(srv, hash)
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || len(g.Requests()) != 1 {
		t.Fatalf("GET of the failing pull = %d, gateway GETs = %d, want 502 and 1", resp.StatusCode, len(g.Requests()))
	}
	if resp, _, _ := do(t, http.MethodGet, u); resp.StatusCode != http.StatusBadGateway || len(g.Requests()) != 1 {
		t.Fatalf("GET in cooldown = %d, gateway GETs = %d, want 502 and 1", resp.StatusCode, len(g.Requests()))
	}
	var got []byte
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a GET past the 200ms failure cooldown to pull again", func() bool {
		resp, b, err := do(t, http.MethodGet, u)
		got = b
		return err == nil && resp.StatusCode == http.StatusOK
	})
	if !bytes.Equal(got, body) || len(g.Requests()) != 2 {
		t.Fatalf("GET past the cooldown = %d of %d bytes, gateway GETs = %d, want the whole body and 2", len(got), len(body), len(g.Requests()))
	}
}
