// Package inventory lists the xorbs already in xet's local storage so the gateway and peer can seed their indexes.
package inventory

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/wzshiming/xet"
)

// Walker is the part of xet's local storage that lists stored xorbs.
type Walker interface {
	WalkXorbs(ctx context.Context, fn func(hash string, size int64, modTime time.Time) error) error
}

type Xorb struct {
	Hash xet.XorbHash
	Size int64
}

// Scan lists w's xorbs least recently modified first, ties by hash string; names that xet.ParseXorbHash rejects are skipped; a walk error is returned as is.
func Scan(ctx context.Context, w Walker) ([]Xorb, error) {
	type found struct {
		Xorb
		modTime time.Time
	}
	var objs []found
	err := w.WalkXorbs(ctx, func(name string, size int64, modTime time.Time) error {
		if hash, err := xet.ParseXorbHash(name); err == nil {
			objs = append(objs, found{Xorb{hash, size}, modTime})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(objs, func(a, b found) int {
		return cmp.Or(a.modTime.Compare(b.modTime), cmp.Compare(a.Hash.String(), b.Hash.String()))
	})
	xorbs := make([]Xorb, len(objs))
	for i, o := range objs {
		xorbs[i] = o.Xorb
	}
	return xorbs, nil
}
