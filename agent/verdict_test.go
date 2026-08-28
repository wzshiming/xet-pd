package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// packOneChunk builds a minimal valid packed chunk sequence.
func packOneChunk(t *testing.T, payload string) []byte {
	t.Helper()
	var packed bytes.Buffer
	enc := xorb.NewEncoder(&packed, false)
	if _, err := enc.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return packed.Bytes()
}

// TestIngestVerdictReleasesAndPurges pins both verdict branches: a passed
// checksum releases quarantined segments for serving, a failed one deletes
// them from disk and cools the peer that supplied them. No announcer is
// configured, which the release path must tolerate.
func TestIngestVerdictReleasesAndPurges(t *testing.T) {
	origin := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(origin.Close)
	dir := t.TempDir()
	a, err := New(Config{Upstream: origin.URL, StorageDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	data := packOneChunk(t, "verdict-chunk")
	span := tracker.ChunkSpan{Start: 0, End: 1}
	goodHash := strings.Repeat("aa", 32)
	badHash := strings.Repeat("bb", 32)
	goodFile := strings.Repeat("cc", 32)
	badFile := strings.Repeat("dd", 32)
	const goodPeer, badPeer = "http://127.0.0.1:2", "http://127.0.0.1:3"

	for _, seed := range []struct {
		hash, file, peer string
	}{
		{goodHash, goodFile, goodPeer},
		{badHash, badFile, badPeer},
	} {
		created, err := a.segments.PutQuarantined(seed.hash, span.Start, span.End, 0, int64(len(data))-1, data, seed.file, seed.peer)
		if err != nil || !created {
			t.Fatalf("got PutQuarantined = (%v, %v), want created", created, err)
		}
		a.quar.add(seed.file, quarItem{xorbHash: seed.hash, span: span, peer: seed.peer})
	}

	a.ingestVerdict([]string{"/org/repo/resolve/main/good.bin"}, goodFile, true)
	if seg, ok := a.segments.Find(goodHash, span.Start, span.End); !ok || seg.Quarantined {
		t.Fatalf("got (found = %v, quarantined = %v) after passed verdict, want released segment", ok, seg.Quarantined)
	}

	// A failed verdict must also drop the client's decoded-chunk cache for
	// the xorb, or retries would replay the corrupt chunks from it.
	cacheDir := filepath.Join(dir, "mirror", "chunks", badHash[:2], badHash[2:])
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "0-1_0-9"), []byte("cached"), 0644); err != nil {
		t.Fatal(err)
	}

	a.ingestVerdict([]string{"/org/repo/resolve/main/bad.bin"}, badFile, false)
	if _, ok := a.segments.Find(badHash, span.Start, span.End); ok {
		t.Fatal("got segment still served after failed verdict, want purged")
	}
	// Find also skips quarantined entries, so prove actual deletion.
	var onDisk bool
	err = a.segments.Walk(func(seg segstore.Segment) error {
		if seg.XorbHash == badHash {
			onDisk = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if onDisk {
		t.Fatal("got purged segment still on disk, want deleted")
	}
	if !a.capture.cooledDown(badPeer) {
		t.Fatal("got supplying peer not cooled after failed verdict, want cooldown")
	}
	if a.capture.cooledDown(goodPeer) {
		t.Fatal("got passed verdict peer cooled, want untouched")
	}
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatalf("got chunk cache stat err = %v after failed verdict, want removed", err)
	}
}

// TestIngestVerdictWarmBranchPathSettles: once the mirror's branch mapping
// is warm it resolves only the pinned commit path, so the selector
// remembers the origin file hash there, while the flight settles under the
// downstream branch path. The verdict must still reach the quarantined
// items through the selector's branch pin — for both outcomes, even though
// a failed flight carries no file hash at all.
func TestIngestVerdictWarmBranchPathSettles(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	tests := []struct {
		name string
		ok   bool
	}{
		{"released on success", true},
		{"purged on failure", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(origin.Close)
			a, err := New(Config{Upstream: origin.URL, StorageDir: t.TempDir(),
				Peers: []string{"http://127.0.0.1:1"}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })

			// The selector saw only this flight's commit-path resolve, plus
			// the branch pin learned when the mapping was warmed.
			originFile := strings.Repeat("0f", 32)
			a.sel.noteBranchCommit("org/repo", "main", commit)
			a.sel.noteResolve("/org/repo/resolve/"+commit+"/warm.bin", originFile)

			data := packOneChunk(t, "warm-branch-chunk")
			span := tracker.ChunkSpan{Start: 0, End: 1}
			xorbHash := strings.Repeat("1a", 32)
			const peer = "http://127.0.0.1:4"
			if created, err := a.segments.PutQuarantined(xorbHash, span.Start, span.End, 0, int64(len(data))-1, data, originFile, peer); err != nil || !created {
				t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
			}
			if !a.quar.add(originFile, quarItem{xorbHash: xorbHash, span: span, peer: peer}) {
				t.Fatal("got quarantine add = false, want registered")
			}

			// Success flights report the locally recomputed hash — which
			// differs from the origin's here — failed flights none.
			localHash := ""
			if tt.ok {
				localHash = strings.Repeat("2b", 32)
			}
			a.ingestVerdict([]string{"/org/repo/resolve/main/warm.bin"}, localHash, tt.ok)

			if items := a.quar.take(originFile); len(items) != 0 {
				t.Fatalf("got %d unsettled quarantine items, want 0", len(items))
			}
			seg, found := a.segments.Find(xorbHash, span.Start, span.End)
			if tt.ok {
				if !found || seg.Quarantined {
					t.Fatalf("got (found = %v, quarantined = %v), want released segment", found, seg.Quarantined)
				}
				if a.capture.cooledDown(peer) {
					t.Fatal("got peer cooled after passed verdict, want untouched")
				}
				return
			}
			var onDisk bool
			if err := a.segments.Walk(func(seg segstore.Segment) error {
				if seg.XorbHash == xorbHash {
					onDisk = true
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if found || onDisk {
				t.Fatalf("got (found = %v, on disk = %v) after failed verdict, want purged", found, onDisk)
			}
			if !a.capture.cooledDown(peer) {
				t.Fatal("got supplying peer not cooled after failed verdict, want cooldown")
			}
		})
	}
}

// TestBranchPinLifetime: branch pins must outlive the mirror's branch
// mapping, which alone re-teaches them via re-probes — a mirror that never
// revalidates needs never-expiring pins, a slow one pins for twice its
// cadence, and the default keeps the resolve-note TTL.
func TestBranchPinLifetime(t *testing.T) {
	origin := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(origin.Close)
	tests := []struct {
		name       string
		revalidate *time.Duration
		want       time.Duration
	}{
		{"default keeps note TTL", nil, resolveHashTTL},
		{"fast cadence keeps note TTL", ptrDuration(time.Minute), resolveHashTTL},
		{"slow cadence doubles it", ptrDuration(2 * time.Hour), 4 * time.Hour},
		{"never revalidates never expires", ptrDuration(-1), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := New(Config{Upstream: origin.URL, StorageDir: t.TempDir(),
				Peers:              []string{"http://127.0.0.1:1"},
				RevalidateInterval: tt.revalidate})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })
			if got := a.sel.branchPinTTL; got != tt.want {
				t.Fatalf("got branchPinTTL = %v, want %v", got, tt.want)
			}
			if a.gate.pin == nil {
				t.Fatal("got gate.pin = nil, want successful flights re-teaching the selector")
			}
			// A pin taught under the configured lifetime translates now.
			a.gate.pin("org/repo", "main", strings.Repeat("ab", 20))
			a.sel.noteResolve("/org/repo/resolve/"+strings.Repeat("ab", 20)+"/f.bin", strings.Repeat("0d", 32))
			if got := a.sel.fileHashFor("/org/repo/resolve/main/f.bin"); got != strings.Repeat("0d", 32) {
				t.Fatalf("got fileHashFor via fresh pin = %q, want the noted hash", got)
			}
		})
	}
}

func ptrDuration(d time.Duration) *time.Duration { return &d }

// TestTrackerPickRejectsOutOfPolicyURLs: file-level directed probing vets
// tracker-supplied candidates with the same helper as the term-fetch chain,
// so out-of-policy URLs are never contacted.
func TestTrackerPickRejectsOutOfPolicyURLs(t *testing.T) {
	var probes atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.Header().Set("ETag", `"deadbeef"`)
	}))
	t.Cleanup(target.Close)
	host := strings.TrimPrefix(target.URL, "http://")

	const path = "/org/repo/resolve/main/f.bin"
	lookup := &tracker.LookupResponse{
		Keys: map[string]tracker.FileLocation{
			path: {Peers: []string{
				"http://user:pw@" + host,
				"ftp://" + host,
				target.URL + "?q=1",
			}},
		},
	}
	fakeTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(lookup)
	}))
	t.Cleanup(fakeTracker.Close)

	tc, err := tracker.NewClient(fakeTracker.URL)
	if err != nil {
		t.Fatal(err)
	}
	s := newPeerSelector("http://origin.invalid", nil, tc, "", time.Second, 4)
	val := &lazyValidator{fetch: func() string {
		t.Error("origin validator fetched, want no candidate ever probed")
		return ""
	}}
	if got := s.trackerPick(context.Background(), path, path, val); got != "" {
		t.Fatalf("got tracker pick %q, want none", got)
	}
	if got := probes.Load(); got != 0 {
		t.Fatalf("got %d probes to out-of-policy candidates, want 0", got)
	}
}

// TestIngestVerdictCoolsFileLevelPeer: a flight whose resolve was served by
// a peer at the file level leaves no per-term quarantine records, so the
// verdict acts on the remembered peer directly: failure cools it, and both
// outcomes drop the whole decoded-chunk cache — the checksum vouches only
// for the file's own bytes, never for neighboring chunks the flight may
// have decoded into the cache.
func TestIngestVerdictCoolsFileLevelPeer(t *testing.T) {
	tests := []struct {
		name string
		ok   bool
	}{
		{"failure cools and purges", false},
		{"success purges without cooling", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(origin.Close)
			dir := t.TempDir()
			a, err := New(Config{Upstream: origin.URL, StorageDir: dir,
				Peers: []string{"http://127.0.0.1:1"}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })

			const key = "/org/repo/resolve/main/file-level.bin"
			const peer = "http://127.0.0.1:5"
			if !a.sel.notePeerServed(key, peer) {
				t.Fatal("got notePeerServed = false, want stored")
			}

			cacheDir := filepath.Join(dir, "mirror", "chunks", "ab", strings.Repeat("cd", 31))
			if err := os.MkdirAll(cacheDir, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cacheDir, "0-1_0-9"), []byte("cached"), 0644); err != nil {
				t.Fatal(err)
			}

			localHash := ""
			if tt.ok {
				localHash = strings.Repeat("2b", 32)
			}
			a.ingestVerdict([]string{key}, localHash, tt.ok)

			if got := a.capture.cooledDown(peer); got != !tt.ok {
				t.Fatalf("got cooledDown = %v after verdict ok = %v, want %v", got, tt.ok, !tt.ok)
			}
			if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
				t.Fatalf("got chunk cache stat err = %v after verdict, want removed", err)
			}
			if got := a.sel.takeResolvePeers([]string{key}); len(got) != 0 {
				t.Fatalf("got %d remembered peers after verdict, want memory consumed", len(got))
			}
		})
	}
}

// TestTakeResolvePeersBranchPin: a failed flight settles under the branch
// path while a warm mirror proxied only the commit path, so the peer memory
// is reached through the same branch pin fileHashFor uses.
func TestTakeResolvePeersBranchPin(t *testing.T) {
	s := newPeerSelector("http://origin.invalid", nil, nil, "", time.Second, 4)
	commit := strings.Repeat("ab", 20)
	const peer = "http://127.0.0.1:6"
	s.noteBranchCommit("org/repo", "main", commit)
	if !s.notePeerServed("/org/repo/resolve/"+commit+"/f.bin", peer) {
		t.Fatal("got notePeerServed = false, want stored")
	}

	got := s.takeResolvePeers([]string{"/org/repo/resolve/main/f.bin"})
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("got takeResolvePeers = %v, want [%s]", got, peer)
	}
	if again := s.takeResolvePeers([]string{"/org/repo/resolve/main/f.bin"}); len(again) != 0 {
		t.Fatalf("got %d peers on second take, want memory consumed", len(again))
	}
}

// TestNotePeerServedRefusesAtCap: once the attribution map is full, new
// paths are refused — the proxy then rejects the peer, because a flight
// whose supplier the verdict could never reach must not exist. Known paths
// keep refreshing.
func TestNotePeerServedRefusesAtCap(t *testing.T) {
	s := newPeerSelector("http://origin.invalid", nil, nil, "", time.Second, 4)
	for i := range maxResolveHashes {
		if !s.notePeerServed(fmt.Sprintf("/org/repo/resolve/main/%d.bin", i), "http://127.0.0.1:7") {
			t.Fatalf("got notePeerServed = false at entry %d, want stored", i)
		}
	}
	if s.notePeerServed("/org/repo/resolve/main/overflow.bin", "http://127.0.0.1:7") {
		t.Fatal("got notePeerServed = true past the cap, want refused")
	}
	if !s.notePeerServed("/org/repo/resolve/main/0.bin", "http://127.0.0.1:8") {
		t.Fatal("got notePeerServed = false for a known path at the cap, want refreshed")
	}
}

// TestOriginValidatorScope: the anchoring probe sends the mirror's
// credential only to the origin host — never to redirect hops — collects
// the validator across hops like the mirror's probe, and memoizes both
// usable and unusable answers so a cold path costs one probe.
func TestOriginValidatorScope(t *testing.T) {
	sha := strings.Repeat("1f", 32)
	var cdnAuth atomic.Value
	cdnAuth.Store("unset")
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cdnAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("ETag", `"`+sha+`"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(cdn.Close)

	var originHits, originAuthed atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		if r.Header.Get("Authorization") == "Bearer tok" {
			originAuthed.Add(1)
		}
		if strings.Contains(r.URL.Path, "bare") {
			w.Header().Set("ETag", `"not-a-sha"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		// No validator on this hop: the probe must follow to the CDN,
		// without the credential.
		http.Redirect(w, r, cdn.URL+"/data", http.StatusFound)
	}))
	t.Cleanup(origin.Close)

	s := newPeerSelector(origin.URL, nil, nil, "", time.Second, 4)
	ctx := context.Background()

	const path = "/org/repo/resolve/main/f.bin"
	if got := s.originValidator(ctx, "Bearer tok", path); got != sha {
		t.Fatalf("got validator %q, want %q", got, sha)
	}
	if got := originAuthed.Load(); got != 1 {
		t.Fatalf("got %d authorized origin hits, want 1", got)
	}
	if got := cdnAuth.Load(); got != "" {
		t.Fatalf("got redirect-hop Authorization %q, want stripped", got)
	}
	if got := s.originValidator(ctx, "Bearer tok", path); got != sha {
		t.Fatalf("got cached validator %q, want %q", got, sha)
	}
	if got := originHits.Load(); got != 1 {
		t.Fatalf("got %d origin hits after cached lookup, want 1", got)
	}

	// A non-sha etag is unusable and remembered as such.
	const bare = "/org/repo/resolve/main/bare.bin"
	for range 2 {
		if got := s.originValidator(ctx, "", bare); got != "" {
			t.Fatalf("got validator %q for non-sha etag, want none", got)
		}
	}
	if got := originHits.Load(); got != 2 {
		t.Fatalf("got %d origin hits after negative cache, want 2", got)
	}
}

// TestAgentWithoutTrackerSweepsQuarantine — Finding B regression: the
// quarantine sweeper is wired into the Agent lifecycle itself, so an agent
// with no tracker and no announcer still recovers on startup: a quarantined
// segment left behind by a pre-upgrade binary (no timestamp, unsettleable)
// is purged, while an old verified segment stays served exactly as today.
func TestAgentWithoutTrackerSweepsQuarantine(t *testing.T) {
	origin := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(origin.Close)
	dir := t.TempDir()
	data := packOneChunk(t, "pre-upgrade segment")
	quarHash := strings.Repeat("11", 32)
	okHash := strings.Repeat("22", 32)
	for hash, quarantined := range map[string]bool{quarHash: true, okHash: false} {
		segDir := filepath.Join(dir, "segments", hash)
		if err := os.MkdirAll(segDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(segDir, "0-1"), data, 0644); err != nil {
			t.Fatal(err)
		}
		meta, err := json.Marshal(segstore.Segment{
			XorbHash:    hash,
			ChunkStart:  0,
			ChunkEnd:    1,
			ByteStart:   0,
			ByteEnd:     int64(len(data)) - 1,
			Size:        int64(len(data)),
			Quarantined: quarantined,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(segDir, "0-1.json"), meta, 0644); err != nil {
			t.Fatal(err)
		}
	}

	a, err := New(Config{Upstream: origin.URL, StorageDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	deadline := time.Now().Add(10 * time.Second)
	for {
		var onDisk bool
		if err := a.segments.Walk(func(seg segstore.Segment) error {
			if seg.XorbHash == quarHash {
				onDisk = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !onDisk {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("got stale quarantined segment still on disk, want the startup sweep to purge it without a tracker")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if seg, ok := a.segments.Find(okHash, 0, 1); !ok || seg.Quarantined {
		t.Fatalf("got Find(old verified meta) = (%+v, %v), want it served exactly as today", seg, ok)
	}
}

// TestIngestVerdictPurgesChunkCacheForPartialCoverage: a fetch entry wider
// than the file's terms leaves chunks the checksum never validated in the
// client's decoded-chunk cache, where a later flight could replay them
// without any transport request — so a passing verdict must drop that
// xorb's cache, while a fully covered xorb keeps its validated cache.
func TestIngestVerdictPurgesChunkCacheForPartialCoverage(t *testing.T) {
	origin := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(origin.Close)
	dir := t.TempDir()
	a, err := New(Config{Upstream: origin.URL, StorageDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	data := packOneChunk(t, "partial-coverage-chunk")
	span := tracker.ChunkSpan{Start: 0, End: 1}
	file := strings.Repeat("5e", 32)
	partialHash := strings.Repeat("3c", 32)
	fullHash := strings.Repeat("4d", 32)
	cacheDir := func(hash string) string {
		return filepath.Join(dir, "mirror", "chunks", hash[:2], hash[2:])
	}
	for hash, partial := range map[string]bool{partialHash: true, fullHash: false} {
		if created, err := a.segments.PutQuarantined(hash, span.Start, span.End, 0, int64(len(data))-1, data, file, "http://127.0.0.1:6"); err != nil || !created {
			t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
		}
		if !a.quar.add(file, quarItem{xorbHash: hash, span: span, peer: "http://127.0.0.1:6", partial: partial}) {
			t.Fatal("got quarantine add = false, want registered")
		}
		if err := os.MkdirAll(cacheDir(hash), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cacheDir(hash), "0-1_0-9"), []byte("cached"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	a.ingestVerdict([]string{"/org/repo/resolve/main/p.bin"}, file, true)

	for _, hash := range []string{partialHash, fullHash} {
		if seg, ok := a.segments.Find(hash, span.Start, span.End); !ok || seg.Quarantined {
			t.Fatalf("got Find(%s) = (%+v, %v) after passed verdict, want released", hash, seg, ok)
		}
	}
	if _, err := os.Stat(cacheDir(partialHash)); !os.IsNotExist(err) {
		t.Fatalf("got partial-coverage chunk cache stat err = %v after passed verdict, want removed", err)
	}
	if _, err := os.Stat(cacheDir(fullHash)); err != nil {
		t.Fatalf("got fully covered chunk cache stat err = %v, want it kept (every chunk validated)", err)
	}
}
