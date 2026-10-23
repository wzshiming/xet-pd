package gateway

import (
	"cmp"
	"maps"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// newTestStaging takes New's replicas and minSeeds defaults and runs maintenance on wakes only unless the config asks for a period.
func newTestStaging(t *testing.T, st *store, cfg stagingConfig) *staging {
	t.Helper()
	cfg.tick = cmp.Or(cfg.tick, time.Hour)
	cfg.replicas = cmp.Or(cfg.replicas, 2)
	cfg.minSeeds = cmp.Or(cfg.minSeeds, 1)
	m, err := newStaging(st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// awaitPass waits for a maintenance pass whose check runs after the call to find the cache over its cap with nothing evictable.
func awaitPass(t *testing.T, m *staging) {
	t.Helper()
	m.mu.Lock()
	m.warned = false
	m.mu.Unlock()
	m.kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a maintenance pass to find nothing evictable", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.warned
	})
}

// askedSeeds clones under m.mu because maintenance edits asked concurrently.
func askedSeeds(m *staging, hash xet.XorbHash) map[string]time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.xorbs.Get(hash).V.asked)
}

func drainWake(m *staging) {
	select {
	case <-m.wake:
	default:
	}
}

// stagingConfigOf needs no lock: m.cfg is fixed by newStaging.
func stagingConfigOf(m *staging) stagingConfig {
	return m.cfg
}
