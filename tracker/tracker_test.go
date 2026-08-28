package tracker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/token"

	"github.com/wzshiming/xet-pd/tracker"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyA = "/org/repo/resolve/main/a.bin"
)

func newTracker(t *testing.T, dir string, opts ...tracker.Option) (*httptest.Server, *tracker.Client) {
	t.Helper()
	h, err := tracker.NewHandler(append([]tracker.Option{tracker.WithStorageDir(dir)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := tracker.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return srv, c
}

func announceA(t *testing.T, c *tracker.Client, peer string) {
	t.Helper()
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: peer,
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{
			{SHA256: shaA, FileHash: "fh-a", Keys: []string{keyA}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnnounceLookupRoundtrip(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	announceA(t, c, "http://a:8880/")
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://b:8880",
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{{SHA256: shaA, Keys: []string{keyA}}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{Files: []string{shaA}, Keys: []string{keyA}})
	if err != nil {
		t.Fatal(err)
	}
	wantPeers := []string{"http://a:8880", "http://b:8880"}
	for name, loc := range map[string]tracker.FileLocation{"files": resp.Files[shaA], "keys": resp.Keys[keyA]} {
		sort.Strings(loc.Peers)
		if !reflect.DeepEqual(loc.Peers, wantPeers) {
			t.Errorf("%s peers = %v, want %v (announced trailing slash must be trimmed)", name, loc.Peers, wantPeers)
		}
		if loc.FileHash != "fh-a" {
			t.Errorf("%s fileHash = %q, want fh-a", name, loc.FileHash)
		}
	}
	if resp.Keys[keyA].SHA256 != shaA {
		t.Errorf("key lookup sha256 = %q, want %q", resp.Keys[keyA].SHA256, shaA)
	}

	t.Run("misses are absent", func(t *testing.T) {
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{
			Files: []string{strings.Repeat("bb", 32)},
			Keys:  []string{"/org/repo/resolve/main/missing"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Files) != 0 || len(resp.Keys) != 0 {
			t.Fatalf("miss lookup = %+v, want empty", resp)
		}
	})

	t.Run("removes withdraw one peer", func(t *testing.T) {
		err := c.Announce(context.Background(), &tracker.AnnounceRequest{
			Peer:    "http://b:8880",
			Removes: tracker.AnnounceSet{Files: []tracker.FileAdd{{SHA256: shaA, Keys: []string{keyA}}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{Keys: []string{keyA}})
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Keys[keyA].Peers; !reflect.DeepEqual(got, []string{"http://a:8880"}) {
			t.Fatalf("peers after remove = %v, want [http://a:8880]", got)
		}
	})
}

func TestLookupExpiresClaims(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://a:8880",
		TTL:  1,
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{{SHA256: shaA, Keys: []string{keyA}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{Files: []string{shaA}, Keys: []string{keyA}})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Files) == 0 && len(resp.Keys) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("claim with 1s TTL still returned: %+v", resp)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIndexSurvivesRestart: the index is plain files, so a new handler on the
// same directory answers immediately, without waiting for an announce cycle.
func TestIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	_, c := newTracker(t, dir)
	announceA(t, c, "http://a:8880")

	_, c2 := newTracker(t, dir)
	resp, err := c2.Lookup(context.Background(), &tracker.LookupRequest{Files: []string{shaA}, Keys: []string{keyA}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Files[shaA].Peers) != 1 || len(resp.Keys[keyA].Peers) != 1 {
		t.Fatalf("restarted lookup = %+v, want the announced peer", resp)
	}
}

// TestAnnounceAuth: with a cluster secret, announces need a minted token
// while lookups stay anonymous.
func TestAnnounceAuth(t *testing.T) {
	issuer, err := token.NewIssuer([]byte("cluster-secret"), 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	validate := func(tok string) bool { return issuer.Validate(tok, time.Now()) }
	srv, anon := newTracker(t, t.TempDir(), tracker.WithAuth(validate))

	if err := anon.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://a:8880",
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{{SHA256: shaA}}},
	}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("anonymous announce error = %v, want 401", err)
	}

	minted, err := tracker.NewClient(srv.URL, tracker.WithMintToken(issuer.Mint))
	if err != nil {
		t.Fatal(err)
	}
	announceA(t, minted, "http://a:8880")

	// A token minted from a different (e.g. per-process handshake) issuer
	// must be rejected: only the cluster secret authorizes announces.
	foreign, err := token.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	foreignClient, err := tracker.NewClient(srv.URL, tracker.WithMintToken(foreign.Mint))
	if err != nil {
		t.Fatal(err)
	}
	if err := foreignClient.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://evil:8880",
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{{SHA256: shaA}}},
	}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("foreign-issuer announce error = %v, want 401", err)
	}

	// Lookup is anonymous by design.
	resp, err := anon.Lookup(context.Background(), &tracker.LookupRequest{Files: []string{shaA}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Files[shaA].Peers) != 1 {
		t.Fatalf("anonymous lookup = %+v, want the announced peer", resp)
	}
}

// TestHostileIdentifiers: non-hex sha256 values and traversal-shaped keys
// must neither be indexed nor touch unexpected paths.
func TestHostileIdentifiers(t *testing.T) {
	_, c := newTracker(t, t.TempDir())
	err := c.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: "http://a:8880",
		Adds: tracker.AnnounceSet{Files: []tracker.FileAdd{
			{SHA256: "../../etc/passwd", Keys: []string{"/../../escape"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{
		Files: []string{"../../etc/passwd"},
		Keys:  []string{"/../../escape"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Files) != 0 {
		t.Fatalf("hostile sha256 was indexed: %+v", resp.Files)
	}
	// Keys are stored under their hash, so even hostile ones stay contained;
	// the claim itself may round-trip.
	if loc, ok := resp.Keys["/../../escape"]; ok && len(loc.Peers) != 1 {
		t.Fatalf("hostile key peers = %v", loc.Peers)
	}
}

func TestNonPostRoutes(t *testing.T) {
	srv, _ := newTracker(t, t.TempDir())
	for _, path := range []string{"/v1/announce", "/v1/lookup", "/anything"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}
}
