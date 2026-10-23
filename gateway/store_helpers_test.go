package gateway

import (
	"maps"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

func newTestStore(t *testing.T, dir string) (*store, *registry) {
	t.Helper()
	reg := newRegistry(time.Minute)
	st, err := newStore(dir, reg)
	if err != nil {
		t.Fatal(err)
	}
	return st, reg
}

// writers counts the writers holding or queued on hash's writer lock.
func writers(st *store, hash xet.XorbHash) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	if w := st.writers[hash]; w != nil {
		return w.users
	}
	return 0
}

func pins(st *store) map[xet.XorbHash]int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return maps.Clone(st.pins)
}

// storeRegistry needs no lock: st.reg is fixed by newStore.
func storeRegistry(st *store) *registry {
	return st.reg
}

func sidecarOffsets(st *store, hash xet.XorbHash) ([]uint64, error) {
	return st.readSidecar(hash)
}

func sidecarPath(st *store, hash xet.XorbHash) string {
	return st.sidecar(hash)
}

func putSidecar(st *store, hash xet.XorbHash, offsets []uint64) error {
	return st.writeSidecar(hash, offsets)
}
