package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

// storeCAS adapts a storage.Storage to upload.ClientAdapter, the same shape
// the mirror's local ingest uses, so tests plant real shards in storage.
type storeCAS struct {
	stor storage.Storage
}

func (c *storeCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return c.stor.HasXorb(ctx, "default", xorbHash)
}

func (c *storeCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	wasInserted, err := c.stor.PutXorb(ctx, "default", xorbHash, r)
	if err != nil {
		return nil, err
	}
	return &upload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (c *storeCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	if _, err := c.stor.PutShard(ctx, shardObj); err != nil {
		return nil, err
	}
	return &upload.ShardUploadResponse{Result: 1}, nil
}

func (c *storeCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, h := range chunkHashes {
		results[h] = &upload.DeduplicationResult{ChunkHash: h, IsNew: true}
	}
	return results, nil
}

// newAnnounceHarness plants one ingested file (shard in storage, entry in
// the ready index) plus one segment of an unrelated xorb, and returns an
// announcer over a fresh tracker, ready for direct driving.
func newAnnounceHarness(t *testing.T) (an *announcer, tc *tracker.Client, shardXorb string, shardChunks uint32, segXorb packedXorb) {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1<<20)
	_, _ = mrand.New(mrand.NewSource(23)).Read(data)
	fileHash, err := upload.UploadFile(context.Background(), &storeCAS{stor: stor}, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := stor.GetShard(context.Background(), fileHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(sh.CASInfos) != 1 {
		t.Fatalf("got %d CAS blocks, want 1", len(sh.CASInfos))
	}
	shardXorb = sh.CASInfos[0].CASHash.String()
	shardChunks = uint32(len(sh.CASInfos[0].Chunks))

	sum := sha256.Sum256(data)
	idx := readyIndex{dir: t.TempDir()}
	err = idx.put(&readyEntry{
		Key:       "/org/repo/resolve/main/announced.bin",
		SHA256:    hex.EncodeToString(sum[:]),
		FileHash:  fileHash.String(),
		Size:      int64(len(data)),
		CheckedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// A segment of a xorb no ready shard names, isolating the segment walk
	// from the shard-derived full ranges.
	segXorb = buildPackedXorb(t)
	segs, err := segstore.New(segstore.WithDir(filepath.Join(t.TempDir(), "segments")))
	if err != nil {
		t.Fatal(err)
	}
	byteStart, byteEnd, segData := segXorb.slice(t, 1, 5)
	if err := segs.Put(segXorb.hash, 1, 5, byteStart, byteEnd, segData); err != nil {
		t.Fatal(err)
	}

	th, err := tracker.NewHandler(tracker.WithStorageDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(th)
	t.Cleanup(srv.Close)
	tc, err = tracker.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	an = newAnnouncer(tc, "http://self:1", idx, stor, segs)
	return an, tc, shardXorb, shardChunks, segXorb
}

func lookupXorbSpan(t *testing.T, tc *tracker.Client, hash string, span tracker.ChunkSpan) []tracker.XorbLocation {
	t.Helper()
	resp, err := tc.Lookup(context.Background(), &tracker.LookupRequest{
		Xorbs: []tracker.XorbQuery{{Hash: hash, Range: span}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Xorbs[fmt.Sprintf("%s:%d-%d", hash, span.Start, span.End)]
}

// TestAnnounceAllClaimsSegmentsAndShards: one announceAll pass claims both
// the stored segment's span and the full chunk range of every xorb named by
// ready files' shards.
func TestAnnounceAllClaimsSegmentsAndShards(t *testing.T) {
	an, tc, shardXorb, shardChunks, segXorb := newAnnounceHarness(t)
	defer an.cancel()
	an.announceAll()

	tests := []struct {
		name string
		hash string
		span tracker.ChunkSpan
		hit  bool
	}{
		{"shard full range", shardXorb, tracker.ChunkSpan{Start: 0, End: shardChunks}, true},
		{"shard mid range contained", shardXorb, tracker.ChunkSpan{Start: 1, End: 2}, true},
		{"segment exact span", segXorb.hash, tracker.ChunkSpan{Start: 1, End: 5}, true},
		{"segment sub span contained", segXorb.hash, tracker.ChunkSpan{Start: 2, End: 4}, true},
		{"beyond the segment misses", segXorb.hash, tracker.ChunkSpan{Start: 0, End: 5}, true},
	}
	// The segment xorb was never ingested as a file, so only [1, 5) exists.
	tests[len(tests)-1].hit = false
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locs := lookupXorbSpan(t, tc, tt.hash, tt.span)
			if got := len(locs) > 0; got != tt.hit {
				t.Fatalf("got hit = %v (%+v), want %v", got, locs, tt.hit)
			}
			if tt.hit && locs[0].Peer != "http://self:1" {
				t.Fatalf("got peer = %q, want http://self:1", locs[0].Peer)
			}
		})
	}
}

// TestNotifySegmentAnnounces: spans queued through the capture notify path
// reach the tracker and merge into one claim.
func TestNotifySegmentAnnounces(t *testing.T) {
	an, tc, _, _, segXorb := newAnnounceHarness(t)
	go an.run()
	defer an.stop()

	an.notifySegment(segXorb.hash, tracker.ChunkSpan{Start: 6, End: 7})
	an.notifySegment(segXorb.hash, tracker.ChunkSpan{Start: 7, End: 8})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if locs := lookupXorbSpan(t, tc, segXorb.hash, tracker.ChunkSpan{Start: 6, End: 8}); len(locs) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("notified segment spans never resolved as one merged claim")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAnnounceAllSkipsQuarantined: a quarantined segment is never claimed;
// releasing it makes the next full pass claim it.
func TestAnnounceAllSkipsQuarantined(t *testing.T) {
	an, tc, _, _, segXorb := newAnnounceHarness(t)
	defer an.cancel()
	byteStart, byteEnd, data := segXorb.slice(t, 6, 8)
	if created, err := an.segs.PutQuarantined(segXorb.hash, 6, 8, byteStart, byteEnd, data, strings.Repeat("aa", 32), "http://127.0.0.1:9"); err != nil || !created {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", created, err)
	}

	an.announceAll()
	if locs := lookupXorbSpan(t, tc, segXorb.hash, tracker.ChunkSpan{Start: 6, End: 8}); len(locs) != 0 {
		t.Fatalf("got quarantined span claimed by %+v, want no claim", locs)
	}

	if released, err := an.segs.Release(segXorb.hash, 6, 8); err != nil || !released {
		t.Fatalf("got Release = (%v, %v), want (true, nil)", released, err)
	}
	an.announceAll()
	if locs := lookupXorbSpan(t, tc, segXorb.hash, tracker.ChunkSpan{Start: 6, End: 8}); len(locs) == 0 {
		t.Fatal("got no claim for the released span, want it announced")
	}
}

// TestMergedXorbAddsCoalesces pins the client-side batching shape: spans of
// one xorb merge into one sorted add.
func TestMergedXorbAddsCoalesces(t *testing.T) {
	adds := mergedXorbAdds(map[string][]tracker.ChunkSpan{
		"bb": {{Start: 4, End: 6}, {Start: 0, End: 2}, {Start: 2, End: 4}},
		"aa": {{Start: 1, End: 3}},
	})
	if len(adds) != 2 || adds[0].Hash != "aa" || adds[1].Hash != "bb" {
		t.Fatalf("got adds = %+v, want sorted by hash", adds)
	}
	want := []tracker.ChunkSpan{{Start: 0, End: 6}}
	if len(adds[1].Ranges) != 1 || adds[1].Ranges[0] != want[0] {
		t.Fatalf("got ranges = %v, want %v", adds[1].Ranges, want)
	}
}

// countingTracker records every announce request body on its way to the
// real handler.
type countingTracker struct {
	inner http.Handler

	mu       sync.Mutex
	requests []tracker.AnnounceRequest
}

func (ct *countingTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/announce" {
		body, err := io.ReadAll(r.Body)
		if err == nil {
			var req tracker.AnnounceRequest
			if json.Unmarshal(body, &req) == nil {
				ct.mu.Lock()
				ct.requests = append(ct.requests, req)
				ct.mu.Unlock()
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	ct.inner.ServeHTTP(w, r)
}

// TestAnnounceAllBatchesMixedKinds: files and xorbs share the 512-item batch
// budget, one XorbAdd counting as one item.
func TestAnnounceAllBatchesMixedKinds(t *testing.T) {
	idx := readyIndex{dir: t.TempDir()}
	for i := range 400 {
		err := idx.put(&readyEntry{
			Key:       fmt.Sprintf("/org/repo/resolve/main/f%03d.bin", i),
			SHA256:    fmt.Sprintf("%064x", i+1),
			Size:      1,
			CheckedAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	segs, err := segstore.New(segstore.WithDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	// One tiny real packed chunk reused under many synthetic xorb hashes.
	var packed bytes.Buffer
	enc := xorb.NewEncoder(&packed, false)
	if _, err := enc.Write([]byte("one-chunk")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		hash := fmt.Sprintf("%064x", 0x10000+i)
		if err := segs.Put(hash, 0, 1, 0, int64(packed.Len()-1), packed.Bytes()); err != nil {
			t.Fatal(err)
		}
	}

	th, err := tracker.NewHandler(tracker.WithStorageDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ct := &countingTracker{inner: th}
	srv := httptest.NewServer(ct)
	t.Cleanup(srv.Close)
	tc, err := tracker.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	an := newAnnouncer(tc, "http://self:1", idx, nil, segs)
	defer an.cancel()
	an.announceAll()

	ct.mu.Lock()
	defer ct.mu.Unlock()
	var files, xorbs int
	mixed := false
	for _, req := range ct.requests {
		if n := len(req.Adds.Files) + len(req.Adds.Xorbs); n > announceBatchSize {
			t.Errorf("got a batch of %d items, want at most %d", n, announceBatchSize)
		}
		if len(req.Adds.Files) > 0 && len(req.Adds.Xorbs) > 0 {
			mixed = true
		}
		files += len(req.Adds.Files)
		xorbs += len(req.Adds.Xorbs)
	}
	if files != 400 || xorbs != 200 {
		t.Fatalf("got %d files and %d xorbs announced, want 400 and 200", files, xorbs)
	}
	if !mixed {
		t.Fatal("got no mixed batch, want files and xorbs sharing one request")
	}
}
