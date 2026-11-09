package tracker

import (
	"cmp"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// newTestTracker serves a tracker built from the zero Config as mod leaves it.
func newTestTracker(t *testing.T, mod func(cfg *Config)) (*Tracker, *httptest.Server) {
	t.Helper()
	var cfg Config
	if mod != nil {
		mod(&cfg)
	}
	tr, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(tr)
	t.Cleanup(func() {
		_ = tr.Close()
		srv.Close()
	})
	return tr, srv
}

// trackerRegistry needs no lock: tr.reg is fixed by New.
func trackerRegistry(tr *Tracker) *registry {
	return tr.reg
}

// trackerSeeder needs no lock: tr.seeder is fixed by New.
func trackerSeeder(tr *Tracker) *seeder {
	return tr.seeder
}

// effectiveConfig needs no lock: the ttl, min seeds and seeding config are fixed by New; the metrics are left out.
func effectiveConfig(tr *Tracker) (time.Duration, int, seedConfig) {
	seed := tr.seeder.cfg
	seed.metrics = nil
	return tr.reg.ttl, tr.reg.minSeeds, seed
}

func clockedRegistry(ttl time.Duration) (*registry, *time.Time) {
	r := newRegistry(ttl, 1)
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	return r, &now
}

func registeredPeers(r *registry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

// newTestSeeder counts into metrics of its own and runs passes on wakes only unless cfg asks for a period.
func newTestSeeder(t *testing.T, reg *registry, cfg seedConfig) *seeder {
	t.Helper()
	cfg.tick = cmp.Or(cfg.tick, time.Hour)
	cfg.metrics = newTrackerMetrics()
	s := newSeeder(reg, cfg)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// settle runs a pass and waits for the pull requests in flight to finish.
func settle(t *testing.T, s *seeder) {
	t.Helper()
	s.seed()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "pull requests in flight to finish", func() bool { return len(s.sem) == 0 })
}
