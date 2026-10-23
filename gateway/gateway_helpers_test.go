package gateway

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// newTestGateway serves a gateway whose own origin is handed to mod before New runs.
func newTestGateway(t *testing.T, hub *clustertest.Hub, mod func(cfg *Config, self string)) (*Gateway, *httptest.Server) {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	cfg := Config{DataDir: t.TempDir(), Upstream: hub.URL}
	if mod != nil {
		mod(&cfg, "http://"+srv.Listener.Addr().String())
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = g
	srv.Start()
	t.Cleanup(func() {
		_ = g.Close()
		srv.Close()
	})
	return g, srv
}

func clockedRegistry(ttl time.Duration) (*registry, *time.Time) {
	r := newRegistry(ttl)
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	return r, &now
}

// gatewayRegistry needs no lock: g.reg is fixed by New.
func gatewayRegistry(g *Gateway) *registry {
	return g.reg
}

// effectiveConfig needs no lock: the ttl and the staging config are fixed by New.
func effectiveConfig(g *Gateway) (time.Duration, stagingConfig) {
	return g.reg.ttl, stagingConfigOf(g.staging)
}

func registeredPeers(r *registry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}
