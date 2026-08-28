package agent_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/agent"
	"github.com/wzshiming/xet-pd/internal/hubtest"
	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// waitXorbAnnounced polls the tracker until a peer covers the chunk span.
func waitXorbAnnounced(t *testing.T, trackerURL, hash string, span tracker.ChunkSpan, wantPeer string) {
	t.Helper()
	c, err := tracker.NewClient(trackerURL)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s:%d-%d", hash, span.Start, span.End)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Lookup(context.Background(), &tracker.LookupRequest{
			Xorbs: []tracker.XorbQuery{{Hash: hash, Range: span}},
		})
		if err == nil {
			for _, loc := range resp.Xorbs[key] {
				if loc.Peer == wantPeer {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("chunk span %s never announced by %s", key, wantPeer)
}

// TestXetIngestCapturesAndAnnounces is the S3b acceptance: a cold download
// from a xet-capable hub leaves exact origin segments in the agent's segment
// store, and the tracker learns which chunk ranges the agent covers.
func TestXetIngestCapturesAndAnnounces(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/xet.bin"
	data := deterministicData(2 << 20)
	hub.Set(t, resolvePath, data)

	trackerSrv := newTrackerServer(t)
	dir := t.TempDir()
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dir,
		Tracker:    trackerSrv.URL,
	})

	if body := getBody(t, srv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold xet download body mismatch")
	}
	waitReady(t, srv.URL+resolvePath)

	hash, xorbData := hub.SingleXorb(t)
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	chunks := uint32(len(offsets))

	t.Run("segments match hub xorb bytes", func(t *testing.T) {
		segs, err := segstore.New(segstore.WithDir(filepath.Join(dir, "segments")))
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = segs.Walk(func(seg segstore.Segment) error {
			count++
			if seg.XorbHash != hash {
				return fmt.Errorf("got segment xorb %s, want %s", seg.XorbHash, hash)
			}
			byteStart, byteEnd, err := xorb.ChunkDataRangeFromOffsets(offsets, seg.ChunkStart, seg.ChunkEnd)
			if err != nil {
				return err
			}
			if seg.ByteStart != byteStart || seg.ByteEnd != byteEnd {
				return fmt.Errorf("got segment bytes [%d, %d], want [%d, %d] for chunks [%d, %d)",
					seg.ByteStart, seg.ByteEnd, byteStart, byteEnd, seg.ChunkStart, seg.ChunkEnd)
			}
			f, err := segs.Open(seg)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			got, err := io.ReadAll(f)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, xorbData[byteStart:byteEnd+1]) {
				return fmt.Errorf("segment chunks [%d, %d) differs from hub xorb bytes", seg.ChunkStart, seg.ChunkEnd)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatal("got 0 captured segments after xet ingest, want at least 1")
		}
	})

	t.Run("mid-file chunk span resolves to the agent", func(t *testing.T) {
		mid := chunks / 2
		waitXorbAnnounced(t, trackerSrv.URL, hash, tracker.ChunkSpan{Start: mid, End: mid + 1}, srv.URL)
	})

	t.Run("full xorb span resolves after ready", func(t *testing.T) {
		waitXorbAnnounced(t, trackerSrv.URL, hash, tracker.ChunkSpan{Start: 0, End: chunks}, srv.URL)
	})
}

// TestAgentCloseUnblockedByHangingIngest: a stalled origin must not pin
// Close — the recorder leaves the still-running ingest flight behind.
func TestAgentCloseUnblockedByHangingIngest(t *testing.T) {
	release := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A non-hex ETag keeps the mirror from demanding a sha256 match.
		w.Header().Set("ETag", `"hang-etag"`)
		w.Header().Set("Content-Length", "8")
		if r.Method == http.MethodHead {
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write([]byte("01234567"))
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()

	a, err := agent.New(agent.Config{
		Upstream:   origin.URL,
		StorageDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a)
	defer srv.Close()

	const resolvePath = "/org/repo/resolve/main/hang.bin"
	// The request triggers the recorder and the ingest, then gives up: the
	// ingest keeps hanging on the gated origin body.
	c := &http.Client{Timeout: 300 * time.Millisecond}
	if resp, err := c.Get(srv.URL + resolvePath); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("got Close error = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Agent.Close blocked on a hanging ingest")
	}
	// Let the background ingest flight finish before cleanup tears down its
	// storage.
	close(release)
	waitReady(t, srv.URL+resolvePath)
}

// TestAgentResponsesCarryMarker: every response of the agent surface carries
// the peer marker, whichever handler answers.
func TestAgentResponsesCarryMarker(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()
	const resolvePath = "/org/repo/resolve/main/marked.bin"
	hub.set(resolvePath, deterministicData(1024))
	srv := newAgentServer(t, hubSrv.URL, nil)

	c := noRedirectClient()
	for name, url := range map[string]string{
		"resolve":    srv.URL + resolvePath,
		"miss route": srv.URL + "/v1/xorbs/default/" + strings.Repeat("ab", 32),
	} {
		resp, err := c.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if got := resp.Header.Get("X-Xet-PD-Agent"); got != "1" {
			t.Errorf("got %s X-Xet-PD-Agent = %q, want 1", name, got)
		}
	}
	// The resolve request kicked off a background ingest; wait it out so
	// cleanup does not race the storage writes.
	waitReady(t, srv.URL+resolvePath)
}

// waitSegmentsReleased polls until the agent's segment store holds at least
// one segment and none of them is still quarantined — the ingest verdict
// runs in the recorder after the entry lands, so release is asynchronous.
func waitSegmentsReleased(t *testing.T, dir string) {
	t.Helper()
	segs, err := segstore.New(segstore.WithDir(filepath.Join(dir, "segments")))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var total, quarantined int
		err := segs.Walk(func(seg segstore.Segment) error {
			total++
			if seg.Quarantined {
				quarantined++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if total > 0 && quarantined == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d quarantined of %d segments, want some and all released", quarantined, total)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSegmentRelayBetweenAgents is the S3c acceptance: with the file-level
// peer path out of the way, a second agent's cold ingest pulls every term
// from the first agent's announced segments — zero origin xorb traffic —
// and its quarantined copies are released and announced once its own
// checksum verdict passes.
func TestSegmentRelayBetweenAgents(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/relay.bin"
	data := deterministicData(2 << 20)
	hub.Set(t, resolvePath, data)
	trackerSrv := newTrackerServer(t)

	dirA := t.TempDir()
	srvA, installA := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dirA,
		Tracker:    trackerSrv.URL,
	})
	if body := getBody(t, srvA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent A cold xet download body mismatch")
	}
	waitReady(t, srvA.URL+resolvePath)

	hash, xorbData := hub.SingleXorb(t)
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	chunks := uint32(len(offsets))
	waitXorbAnnounced(t, trackerSrv.URL, hash, tracker.ChunkSpan{Start: 0, End: chunks}, srvA.URL)

	// Dropping A's ready index silences its file-level path — local-only
	// resolve probes now miss — while its xorb data keeps serving, so B's
	// ingest must go through the per-term segment chain.
	if err := os.RemoveAll(filepath.Join(dirA, "peer", "ready")); err != nil {
		t.Fatal(err)
	}
	baseline := hub.XorbFetchCount()

	dirB := t.TempDir()
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dirB,
		Tracker:    trackerSrv.URL,
	})
	if body := getBody(t, srvB.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent B cold xet download body mismatch")
	}
	waitReady(t, srvB.URL+resolvePath)

	if got := hub.XorbFetchCount() - baseline; got != 0 {
		t.Fatalf("got %d hub xorb fetches during B's ingest, want 0 (terms relayed from A)", got)
	}
	waitSegmentsReleased(t, dirB)
	waitXorbAnnounced(t, trackerSrv.URL, hash, tracker.ChunkSpan{Start: 0, End: chunks}, srvB.URL)
}

// corruptChunkPayloads flips the last payload byte of every chunk, leaving
// all 8-byte headers intact: any slice still walks as valid chunks but
// decodes to different content, so only the file checksum can reject it.
func corruptChunkPayloads(t *testing.T, xorbData []byte) []byte {
	t.Helper()
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	corrupted := bytes.Clone(xorbData)
	prev := uint64(0)
	for _, off := range offsets {
		if off-prev > 8 {
			corrupted[off-1] ^= 0xff
		}
		prev = off
	}
	return corrupted
}

// TestPeerContentMismatchPurged: a peer serving structurally valid but
// content-corrupted segments passes the fetch-time walk, so only the file's
// checksum verdict catches it — the resolve never turns ready with wrong
// bytes, retries settle on the origin once the peer cools down, and the
// quarantined copies are purged.
func TestPeerContentMismatchPurged(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/mismatch.bin"
	data := deterministicData(2 << 20)
	hub.Set(t, resolvePath, data)
	trackerSrv := newTrackerServer(t)

	hash, xorbData := hub.SingleXorb(t)
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	chunks := uint32(len(offsets))
	corrupted := corruptChunkPayloads(t, xorbData)

	var fakeHits atomic.Int64
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fakeHits.Add(1)
		m := peerRangeRe.FindStringSubmatch(r.Header.Get("Range"))
		if m == nil {
			http.Error(w, "Unsupported range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, _ := strconv.ParseInt(m[1], 10, 64)
		end, _ := strconv.ParseInt(m[2], 10, 64)
		if start > end || end >= int64(len(corrupted)) {
			http.Error(w, "Out of bounds", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("X-Xet-PD-Agent", "1")
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(corrupted)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(corrupted[start : end+1])
	}))
	t.Cleanup(fake.Close)

	tc, err := tracker.NewClient(trackerSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = tc.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: fake.URL,
		Adds: tracker.AnnounceSet{Xorbs: []tracker.XorbAdd{
			{Hash: hash, Ranges: []tracker.ChunkSpan{{Start: 0, End: chunks}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dir,
		Tracker:    trackerSrv.URL,
	})

	// The first flight consumes the corrupted terms and must fail its
	// checksum; its client stream may abort or carry wrong bytes, but the
	// entry never turns ready with them. Once the verdict cools the peer
	// and drops the poisoned chunk cache, the retry after the mirror's
	// failure backoff (~10s) lands correct bytes from the origin.
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(srv.URL + resolvePath)
		var body []byte
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		if err == nil && resp.StatusCode == http.StatusOK && bytes.Equal(body, data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent never served correct bytes after content-mismatched peer")
		}
		time.Sleep(250 * time.Millisecond)
	}
	waitReady(t, srv.URL+resolvePath)

	if got := fakeHits.Load(); got == 0 {
		t.Fatal("got 0 fetches to the mismatching peer, want at least 1 (chain consulted it)")
	}
	// Everything the failed flights quarantined is purged; what remains was
	// re-captured from the origin and released.
	waitSegmentsReleased(t, dir)
}

// peerRangeRe parses the single bounded byte range peer term fetches send.
var peerRangeRe = regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)

// TestMultiXorbFixtureColdDownload sanity-checks the hand-built multi-xorb
// fixture: one cold download reassembles the exact bytes, and every xorb's
// full chunk span ends up announced — the reconstruction really carried one
// term per xorb.
func TestMultiXorbFixtureColdDownload(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/multi.bin"
	data := deterministicData(10 * 2 * (64 << 10))
	mf := hub.SetMultiXorb(t, resolvePath, data, 64<<10, 2)
	if got := len(mf.Xorbs); got != 10 {
		t.Fatalf("got %d fixture xorbs, want 10", got)
	}
	trackerSrv := newTrackerServer(t)
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
	})
	if body := getBody(t, srv.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold multi-xorb download body mismatch")
	}
	waitReady(t, srv.URL+resolvePath)
	for i, x := range mf.Xorbs {
		if hub.XorbHashGets(x.Hash) == 0 {
			t.Errorf("got 0 origin fetches for xorb %d, want at least 1", i)
		}
		waitXorbAnnounced(t, trackerSrv.URL, x.Hash, tracker.ChunkSpan{Start: 0, End: x.Chunks}, srv.URL)
	}
}

// TestPartialCaptureSharedBetweenAgents is the S3d partial-content
// acceptance: agent A's cold flight blocks on gated xorbs, but the prefix
// it captured from the origin is announced — and agent B's overlapping cold
// download pulls exactly that prefix from A, costing the origin zero
// traffic for the shared xorbs.
func TestPartialCaptureSharedBetweenAgents(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/partial.bin"
	data := deterministicData(10 * 2 * (64 << 10))
	mf := hub.SetMultiXorb(t, resolvePath, data, 64<<10, 2)
	trackerSrv := newTrackerServer(t)

	var gatedHashes []string
	for _, x := range mf.Xorbs[3:] {
		gatedHashes = append(gatedHashes, x.Hash)
	}
	hub.SetGated(true, gatedHashes...)
	defer hub.SetGated(false, gatedHashes...)

	srvA, installA := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
	})
	// The client gives up quickly; A's flight keeps running and blocks on
	// the first gated xorb after capturing and announcing the open prefix.
	shortClient := &http.Client{Timeout: 300 * time.Millisecond}
	if resp, err := shortClient.Get(srvA.URL + resolvePath); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	for _, x := range mf.Xorbs[:3] {
		waitXorbAnnounced(t, trackerSrv.URL, x.Hash, tracker.ChunkSpan{Start: 0, End: x.Chunks}, srvA.URL)
	}

	before := make([]int, len(mf.Xorbs))
	for i, x := range mf.Xorbs {
		before[i] = hub.XorbHashGets(x.Hash)
	}

	dirB := t.TempDir()
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dirB,
		Tracker:    trackerSrv.URL,
	})
	bDone := make(chan []byte, 1)
	go func() {
		resp, err := http.Get(srvB.URL + resolvePath)
		if err != nil {
			bDone <- nil
			return
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			body = nil
		}
		bDone <- body
	}()
	// B shares the prefix from A, then blocks on the same gate; its arrival
	// there is the deterministic signal that both flights overlap.
	waitCounter(t, func() bool { return hub.XorbHashGets(mf.Xorbs[3].Hash) > before[3] })
	hub.SetGated(false, gatedHashes...)

	if body := <-bDone; !bytes.Equal(body, data) {
		t.Fatal("agent B cold download body mismatch")
	}
	for i, x := range mf.Xorbs[:3] {
		if grew := hub.XorbHashGets(x.Hash) - before[i]; grew != 0 {
			t.Errorf("got %d origin fetches for shared xorb %d after A captured it, want 0 (served by A)", grew, i)
		}
	}
	// Both flights settle: A finishes its interrupted ingest, B turns ready,
	// its quarantined copies of A's segments are released and announced.
	waitReady(t, srvA.URL+resolvePath)
	waitReady(t, srvB.URL+resolvePath)
	waitSegmentsReleased(t, dirB)
	waitXorbAnnounced(t, trackerSrv.URL, mf.Xorbs[0].Hash, tracker.ChunkSpan{Start: 0, End: mf.Xorbs[0].Chunks}, srvB.URL)
}

// waitCounter polls until cond holds.
func waitCounter(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentColdDownloadsShareOrigin is the S3 overlapping-cold
// acceptance: a follower whose flight overlaps a leader's pulls every span
// the leader has already announced, thanks to per-span lookup-on-miss. Both
// flights end up blocked on the same gated last xorb, so the origin serves
// each open xorb once plus the gated one twice: (N+1)/N of the file, under
// the 1.2× bound.
//
// The follower starts after the leader announced its open spans on purpose:
// announces only happen once a term lands, so two strictly simultaneous
// cold starts each go to the origin. Coordinating those needs the S4
// cluster lease, not the segment index.
func TestConcurrentColdDownloadsShareOrigin(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/concurrent.bin"
	data := deterministicData(10 * 2 * (64 << 10))
	mf := hub.SetMultiXorb(t, resolvePath, data, 64<<10, 2)
	trackerSrv := newTrackerServer(t)

	last := mf.Xorbs[len(mf.Xorbs)-1]
	hub.SetGated(true, last.Hash)
	defer hub.SetGated(false, last.Hash)

	srvA, installA := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
	})
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: t.TempDir(),
		Tracker:    trackerSrv.URL,
	})

	fetch := func(srv *httptest.Server) chan []byte {
		done := make(chan []byte, 1)
		go func() {
			resp, err := http.Get(srv.URL + resolvePath)
			if err != nil {
				done <- nil
				return
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				body = nil
			}
			done <- body
		}()
		return done
	}

	aDone := fetch(srvA)
	// A captures and announces every open xorb, then blocks on the gate.
	for _, x := range mf.Xorbs[:len(mf.Xorbs)-1] {
		waitXorbAnnounced(t, trackerSrv.URL, x.Hash, tracker.ChunkSpan{Start: 0, End: x.Chunks}, srvA.URL)
	}
	bDone := fetch(srvB)
	// B relays the announced spans from A and joins the gate: two blocked
	// origin fetches of the last xorb mean both flights are in flight.
	waitCounter(t, func() bool { return hub.XorbHashGets(last.Hash) >= 2 })
	hub.SetGated(false, last.Hash)

	if body := <-aDone; !bytes.Equal(body, data) {
		t.Fatal("agent A cold download body mismatch")
	}
	if body := <-bDone; !bytes.Equal(body, data) {
		t.Fatal("agent B cold download body mismatch")
	}
	total := mf.PackedTotal()
	served := hub.XorbRouteBytes()
	if float64(served) >= 1.2*float64(total) {
		t.Fatalf("got %d origin xorb-route bytes for two concurrent cold downloads, want < 1.2 × %d", served, total)
	}
	// Let both ingests settle before cleanup tears their storage down.
	waitReady(t, srvA.URL+resolvePath)
	waitReady(t, srvB.URL+resolvePath)
}

// TestWarmBranchSecondFileVerdictSettles: once the first file warms the
// repo's branch mapping, the mirror resolves later files of that branch
// under their pinned commit path only, while flights still settle under
// the downstream branch path. The second file's peer-sourced quarantined
// segments must nevertheless be released and announced when its checksum
// passes.
func TestWarmBranchSecondFileVerdictSettles(t *testing.T) {
	hub := hubtest.New(t)
	const firstPath = "/org/repo/resolve/main/warm-first.bin"
	const secondPath = "/org/repo/resolve/main/warm-second.bin"
	all := deterministicData(5 * (64 << 10))
	dataFirst, dataSecond := all[:64<<10], all[64<<10:]
	mf := hub.SetMultiXorb(t, secondPath, dataSecond, 64<<10, 2)
	hub.Set(t, firstPath, dataFirst)
	trackerSrv := newTrackerServer(t)

	// Agent A owns and announces the second file's segments.
	dirA := t.TempDir()
	srvA, installA := newSwappableServer(t)
	newAgentAt(t, srvA, installA, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dirA,
		Tracker:    trackerSrv.URL,
	})
	if body := getBody(t, srvA.URL+secondPath); !bytes.Equal(body, dataSecond) {
		t.Fatal("agent A cold xet download body mismatch")
	}
	waitReady(t, srvA.URL+secondPath)
	for _, x := range mf.Xorbs {
		waitXorbAnnounced(t, trackerSrv.URL, x.Hash, tracker.ChunkSpan{Start: 0, End: x.Chunks}, srvA.URL)
	}
	// Silence A's file-level path so B's ingest must use the term chain.
	if err := os.RemoveAll(filepath.Join(dirA, "peer", "ready")); err != nil {
		t.Fatal(err)
	}

	dirB := t.TempDir()
	srvB, installB := newSwappableServer(t)
	newAgentAt(t, srvB, installB, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dirB,
		Tracker:    trackerSrv.URL,
	})
	// The first file warms B's branch mapping from the origin.
	if body := getBody(t, srvB.URL+firstPath); !bytes.Equal(body, dataFirst) {
		t.Fatal("agent B first-file download body mismatch")
	}
	waitReady(t, srvB.URL+firstPath)

	before := make([]int, len(mf.Xorbs))
	for i, x := range mf.Xorbs {
		before[i] = hub.XorbHashGets(x.Hash)
	}
	if body := getBody(t, srvB.URL+secondPath); !bytes.Equal(body, dataSecond) {
		t.Fatal("agent B second-file download body mismatch")
	}
	waitReady(t, srvB.URL+secondPath)
	for i, x := range mf.Xorbs {
		if grew := hub.XorbHashGets(x.Hash) - before[i]; grew != 0 {
			t.Errorf("got %d origin fetches for xorb %d during B's warm-branch ingest, want 0 (relayed from A)", grew, i)
		}
	}
	// The regression: B's verdict arrives under the branch path while the
	// selector remembered the origin hash under the commit path only — the
	// quarantined copies must still be released and announced.
	waitSegmentsReleased(t, dirB)
	for _, x := range mf.Xorbs {
		waitXorbAnnounced(t, trackerSrv.URL, x.Hash, tracker.ChunkSpan{Start: 0, End: x.Chunks}, srvB.URL)
	}
}

// TestWarmBranchSecondFileMismatchPurged: with the branch mapping warmed by
// an earlier file, a content-mismatched peer feeding a later file's flight
// must still be caught by the verdict delivered under the branch path: the
// quarantined copies and the poisoned chunk cache are purged and the peer
// cools down, so retries settle on the origin with correct bytes instead of
// replaying the corrupt chunks forever.
func TestWarmBranchSecondFileMismatchPurged(t *testing.T) {
	hub := hubtest.New(t)
	const firstPath = "/org/repo/resolve/main/mm-first.bin"
	const secondPath = "/org/repo/resolve/main/mm-second.bin"
	all := deterministicData(2<<20 + 64<<10)
	dataSecond, dataFirst := all[:2<<20], all[2<<20:]
	hub.Set(t, secondPath, dataSecond)
	hash, xorbData := hub.SingleXorb(t)
	hub.Set(t, firstPath, dataFirst)
	trackerSrv := newTrackerServer(t)

	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	chunks := uint32(len(offsets))
	corrupted := corruptChunkPayloads(t, xorbData)

	var fakeHits atomic.Int64
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fakeHits.Add(1)
		m := peerRangeRe.FindStringSubmatch(r.Header.Get("Range"))
		if m == nil {
			http.Error(w, "Unsupported range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, _ := strconv.ParseInt(m[1], 10, 64)
		end, _ := strconv.ParseInt(m[2], 10, 64)
		if start > end || end >= int64(len(corrupted)) {
			http.Error(w, "Out of bounds", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("X-Xet-PD-Agent", "1")
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(corrupted)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(corrupted[start : end+1])
	}))
	t.Cleanup(fake.Close)

	tc, err := tracker.NewClient(trackerSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = tc.Announce(context.Background(), &tracker.AnnounceRequest{
		Peer: fake.URL,
		Adds: tracker.AnnounceSet{Xorbs: []tracker.XorbAdd{
			{Hash: hash, Ranges: []tracker.ChunkSpan{{Start: 0, End: chunks}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:   hub.Server.URL,
		StorageDir: dir,
		Tracker:    trackerSrv.URL,
	})

	// The first file warms the branch mapping from the origin.
	if body := getBody(t, srv.URL+firstPath); !bytes.Equal(body, dataFirst) {
		t.Fatal("first-file download body mismatch")
	}
	waitReady(t, srv.URL+firstPath)

	// The second file's first flight consumes the corrupted terms and must
	// fail its checksum; the branch-path verdict purges and cools, and the
	// retry after the mirror's failure backoff lands correct origin bytes.
	// The deadline only bounds "eventually": the failed flight plus its
	// ~10s backoff can stretch well past half a minute under a loaded race
	// detector.
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := http.Get(srv.URL + secondPath)
		var body []byte
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		if err == nil && resp.StatusCode == http.StatusOK && bytes.Equal(body, dataSecond) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent never served correct bytes for the warm-branch second file")
		}
		time.Sleep(250 * time.Millisecond)
	}
	waitReady(t, srv.URL+secondPath)
	if got := fakeHits.Load(); got == 0 {
		t.Fatal("got 0 fetches to the mismatching peer, want at least 1 (chain consulted it)")
	}
	// Everything the failed flight quarantined is purged; what remains was
	// re-captured from the origin and released.
	waitSegmentsReleased(t, dir)
}

// TestXetPeerRelayZeroOriginDataBytes is the anchored S1 acceptance for the
// xet path: with peer A warm and its file-level index intact, B's cold
// download flows entirely through A — the origin's data route serves zero
// bytes, paying only metadata-sized validator probes.
func TestXetPeerRelayZeroOriginDataBytes(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/file-relay.bin"
	data := deterministicData(2 << 20)
	hub.Set(t, resolvePath, data)

	srvA := newAgentServer(t, hub.Server.URL, nil)
	if body := getBody(t, srvA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent A cold xet download body mismatch")
	}
	waitReady(t, srvA.URL+resolvePath)
	waitPeerReady(t, srvA.URL+resolvePath)
	canonical := strings.Replace(resolvePath, "/main/", "/"+hub.Commit+"/", 1)
	waitPeerReady(t, srvA.URL+canonical)

	baseline := hub.XorbRouteBytes()
	srvB := newAgentServer(t, hub.Server.URL, nil, srvA.URL)
	if body := getBody(t, srvB.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent B cold download via peer body mismatch")
	}
	waitReady(t, srvB.URL+resolvePath)

	if got := hub.XorbRouteBytes() - baseline; got != 0 {
		t.Fatalf("origin data route served %d bytes during B's peer-relayed download, want 0", got)
	}
}

// TestXetPeerTrueEtagWrongBytesCooled closes the failure loop of the S1
// anchor: a peer advertising the origin's true validator but serving
// corrupted xorb bytes passes the pick, so only the flight's checksum
// catches it. The verdict must cool that peer and drop the poisoned
// decoded-chunk cache — the corrupt chunks were cached under the origin's
// own xorb hash — so the retry lands correct origin bytes instead of
// replaying them forever.
func TestXetPeerTrueEtagWrongBytesCooled(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/true-etag.bin"
	data := deterministicData(2 << 20)
	f := hub.Set(t, resolvePath, data)
	hash, xorbData := hub.SingleXorb(t)

	// A warms honestly, then its stored xorb rots in place: the ready
	// index keeps advertising the true validator while the CAS route
	// serves structurally valid, content-corrupted bytes.
	dirA := t.TempDir()
	srvA, installA := newSwappableServer(t)
	peerA, err := agent.New(agent.Config{
		Upstream:    hub.Server.URL,
		StorageDir:  dirA,
		ExternalURL: srvA.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peerA.Close() })
	var peerTermHits atomic.Int64
	installA(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			peerTermHits.Add(1)
		}
		peerA.ServeHTTP(w, r)
	}))
	if body := getBody(t, srvA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent A cold xet download body mismatch")
	}
	waitReady(t, srvA.URL+resolvePath)
	waitPeerReady(t, srvA.URL+resolvePath)
	xorbPath := filepath.Join(dirA, "storage", "xorbs", hash[:2], hash[2:])
	if _, err := os.Stat(xorbPath); err != nil {
		t.Fatalf("stat stored xorb: %v", err)
	}
	if err := os.WriteFile(xorbPath, corruptChunkPayloads(t, xorbData), 0644); err != nil {
		t.Fatal(err)
	}

	srvB := newAgentServer(t, hub.Server.URL, nil, srvA.URL)

	// The first flight relays A's corrupt terms and must fail its
	// checksum; once the verdict cools A and drops the poisoned chunk
	// cache, the retry after the mirror's ~10s failure backoff lands
	// correct bytes from the origin. The deadline only bounds
	// "eventually".
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := http.Get(srvB.URL + resolvePath)
		var body []byte
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		if err == nil && resp.StatusCode == http.StatusOK && bytes.Equal(body, data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent never served correct bytes after the true-etag corrupt peer")
		}
		time.Sleep(250 * time.Millisecond)
	}
	ready := waitReady(t, srvB.URL+resolvePath)
	if got, want := ready.Header.Get("ETag"), `"`+f.SHA+`"`; got != want {
		t.Fatalf("ready ETag = %q, want the origin's %q", got, want)
	}
	if got := peerTermHits.Load(); got == 0 {
		t.Fatal("got 0 term fetches from the corrupt peer, want at least 1 (flight relayed through it)")
	}
}
