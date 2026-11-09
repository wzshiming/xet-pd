package peer

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
)

// fetcher is the one place this peer requests bytes from other peers.
type fetcher struct {
	client *http.Client
	self   string // sent as apis.PeerHeader
}

// xorb GETs the copy of hash peer holds, returning its body and declared length, -1 when unknown; a 404 wraps fs.ErrNotExist.
func (f fetcher) xorb(ctx context.Context, peer apis.Peer, hash xet.XorbHash) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apis.XorbURL(peer.URL, hash.String()), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set(apis.PeerHeader, f.self)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == http.StatusOK {
		return resp.Body, resp.ContentLength, nil
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, fmt.Errorf("peer %s: status %s: %w", peer.ID, resp.Status, fs.ErrNotExist)
	}
	return nil, 0, fmt.Errorf("peer %s: status %s", peer.ID, resp.Status)
}

// shard fetches the ShardResponse of fileHash from peer.
func (f fetcher) shard(ctx context.Context, peer apis.Peer, secret, fileHash string) (apis.ShardResponse, error) {
	return apis.GetShard(ctx, f.client, peer.URL, secret, f.self, fileHash)
}
