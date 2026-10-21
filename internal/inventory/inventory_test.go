package inventory_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/inventory"
)

type fakeRow struct {
	name  string
	size  int64
	mtime time.Time
}

type fakeCtxKey struct{}

// fakeWalker hands its rows to fn in order, then returns err; ctxValue is what the walk's ctx carried under fakeCtxKey.
type fakeWalker struct {
	rows     []fakeRow
	err      error
	ctxValue any
}

func (w *fakeWalker) WalkXorbs(ctx context.Context, fn func(hash string, size int64, modTime time.Time) error) error {
	w.ctxValue = ctx.Value(fakeCtxKey{})
	for _, r := range w.rows {
		if err := fn(r.name, r.size, r.mtime); err != nil {
			return err
		}
	}
	return w.err
}

func testParse(t *testing.T, s string) xet.XorbHash {
	t.Helper()
	h, err := xet.ParseXorbHash(s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestScan(t *testing.T) {
	at := func(sec int64) time.Time { return time.Unix(1_700_000_000+sec, 0) }
	n1, n3, n4 := strings.Repeat("c", 64), strings.Repeat("b", 64), strings.Repeat("a", 64)
	// lo sorts first as a hash string but last as bytes: xet stores each 16-digit group little-endian.
	lo, hi := strings.Repeat("0", 15)+"1"+strings.Repeat("0", 48), "01"+strings.Repeat("0", 62)

	t.Run("sorted", func(t *testing.T) {
		w := &fakeWalker{rows: []fakeRow{
			{n3, 30, at(3)},
			{hi, 21, at(2)},
			{"not-a-hash", 90, at(0)},
			{n4, 40, at(4)},
			{lo, 20, at(2)},
			{strings.Repeat("1", 63), 91, at(1)},
			{n1, 10, at(1)},
			{strings.Repeat("g", 64), 92, at(5)},
		}}
		want := []inventory.Xorb{
			{Hash: testParse(t, n1), Size: 10},
			{Hash: testParse(t, lo), Size: 20},
			{Hash: testParse(t, hi), Size: 21},
			{Hash: testParse(t, n3), Size: 30},
			{Hash: testParse(t, n4), Size: 40},
		}
		if got, err := inventory.Scan(t.Context(), w); err != nil || !slices.Equal(got, want) {
			t.Errorf("Scan = %v, %v, want %v by mtime then hash string, invalid names skipped", got, err, want)
		}
	})
	t.Run("walk_error", func(t *testing.T) {
		walkErr := errors.New("walk failed")
		w := &fakeWalker{rows: []fakeRow{{n1, 10, at(1)}}, err: walkErr}
		if got, err := inventory.Scan(t.Context(), w); !errors.Is(err, walkErr) || len(got) != 0 {
			t.Errorf("Scan = %v, %v, want no xorbs, %v", got, err, walkErr)
		}
	})
	t.Run("ctx", func(t *testing.T) {
		w := &fakeWalker{}
		ctx := context.WithValue(t.Context(), fakeCtxKey{}, "scan")
		if _, err := inventory.Scan(ctx, w); err != nil || w.ctxValue != "scan" {
			t.Errorf("Scan: err %v, WalkXorbs ctx value %v, want <nil>, scan", err, w.ctxValue)
		}
	})
}
