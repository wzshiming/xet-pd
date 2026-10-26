package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// relayAttempts bounds the holders tried per open.
const relayAttempts = 3

// relay opens xorb byte ranges on live holders over the store's transport, marked as peer traffic and carrying no upstream credentials.
type relay struct {
	reg       *registry
	transport http.RoundTripper
	metrics   *gatewayMetrics
}

// open returns [start, end] of the xorb from a live holder, end -1 meaning EOF; failed holders are skipped before any handle is handed out.
func (r relay) open(ctx context.Context, hash xet.XorbHash, start, end int64) (io.ReadSeekCloser, error) {
	holders := r.reg.holders(hash.String())
	if len(holders) == 0 {
		r.metrics.relay("no_holder")
		return nil, fmt.Errorf("xorb %s has no live holder: %w", hash, fs.ErrNotExist)
	}
	var err error
	for _, p := range holders[:min(len(holders), relayAttempts)] {
		var req *http.Request
		if req, err = http.NewRequestWithContext(ctx, http.MethodGet, apis.XorbURL(p.URL, hash.String()), nil); err == nil {
			req.Header.Set(apis.PeerHeader, "gateway")
			sk := httpseek.NewRangeSeeker(ctx, r.transport, req, start, end)
			if end < 0 {
				if _, err = sk.Seek(0, io.SeekEnd); err == nil {
					_, err = sk.Seek(start, io.SeekStart)
				}
			} else if resp, rerr := sk.Response(); rerr != nil {
				err = rerr
			} else if resp.StatusCode != http.StatusPartialContent {
				err = errors.New(resp.Status)
			}
			if err == nil {
				r.metrics.relay("ok")
				return sk, nil
			}
			_ = sk.Close()
			err = fmt.Errorf("holder %s: %w", p.ID, err)
		}
		if cerr := ctx.Err(); cerr != nil {
			r.metrics.relay("canceled")
			return nil, cerr
		}
	}
	r.metrics.relay("error")
	return nil, fmt.Errorf("xorb %s: %w", hash, err)
}
