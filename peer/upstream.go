package peer

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/wzshiming/xet/auth"

	"github.com/wzshiming/xet-pd/apis"
)

// errNoUpstream fails the resolves of an internal peer while the tracker lists no access peer.
var errNoUpstream = errors.New("no live peer has an upstream")

// repoKey is the context key under which servePublic puts the repository of a request.
type repoKey struct{}

// repoOf returns the owner/name a hub path names, "" when it names none.
func repoOf(path string) string {
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if segs[0] == "api" {
		segs = segs[1:]
	}
	if len(segs) > 0 && slices.Contains([]string{"models", "datasets", "spaces"}, segs[0]) {
		segs = segs[1:]
	}
	if len(segs) < 2 {
		return ""
	}
	return segs[0] + "/" + segs[1]
}

// accessUpstream is the upstream of an internal peer: the access peer apis.Rank puts first for the repository, "" outside a request.
type accessUpstream struct{ ann *announcer }

func (u accessUpstream) Resolve(ctx context.Context, _ auth.Permission) (string, string, error) {
	repo, _ := ctx.Value(repoKey{}).(string)
	peers := apis.Rank(repo, u.ann.upstreams())
	if len(peers) == 0 {
		return "", "", errNoUpstream
	}
	return peers[0].URL, "", nil
}

// peerTransport sends id as apis.PeerHeader on every request without one, so an access peer answers from its own copies.
type peerTransport struct {
	base http.RoundTripper
	id   string
}

func (t peerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get(apis.PeerHeader) == "" {
		r = r.Clone(r.Context())
		r.Header.Set(apis.PeerHeader, t.id)
	}
	return t.base.RoundTrip(r)
}
