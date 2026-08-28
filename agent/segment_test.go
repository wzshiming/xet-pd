package agent_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/agent"
	"github.com/wzshiming/xet-pd/segstore"
)

// memCAS is a minimal upload.ClientAdapter that captures packed xorbs in
// memory, so tests slice segments from real encoder output.
type memCAS struct {
	mu    sync.Mutex
	xorbs map[string][]byte
}

func (c *memCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.xorbs[xorbHash.String()]
	return ok, nil
}

func (c *memCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xorbs[xorbHash.String()] = data
	return &upload.XorbUploadResponse{WasInserted: true}, nil
}

func (c *memCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return &upload.ShardUploadResponse{Result: 1}, nil
}

func (c *memCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, h := range chunkHashes {
		results[h] = &upload.DeduplicationResult{ChunkHash: h, IsNew: true}
	}
	return results, nil
}

// xorbFixture is one real packed xorb built by the xet upload pipeline.
type xorbFixture struct {
	hash    string
	data    []byte
	offsets []uint64
}

var (
	xorbFixtureOnce sync.Once
	xorbFixtureVal  xorbFixture
	xorbFixtureErr  error
)

// buildXorbFixture uploads a few MiB of seeded pseudo-random data through
// the real chunker and encoder, yielding one multi-chunk xorb.
func buildXorbFixture(t *testing.T) xorbFixture {
	t.Helper()
	xorbFixtureOnce.Do(func() {
		src := make([]byte, 2<<20)
		_, _ = mrand.New(mrand.NewSource(7)).Read(src)
		cas := &memCAS{xorbs: map[string][]byte{}}
		if _, err := upload.UploadFile(context.Background(), cas, bytes.NewReader(src)); err != nil {
			xorbFixtureErr = fmt.Errorf("upload fixture file: %w", err)
			return
		}
		if len(cas.xorbs) != 1 {
			xorbFixtureErr = fmt.Errorf("got %d fixture xorbs, want 1", len(cas.xorbs))
			return
		}
		for hash, data := range cas.xorbs {
			xorbFixtureVal.hash, xorbFixtureVal.data = hash, data
		}
		offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(xorbFixtureVal.data))
		if err != nil {
			xorbFixtureErr = fmt.Errorf("scan fixture xorb: %w", err)
			return
		}
		if len(offsets) < 8 {
			xorbFixtureErr = fmt.Errorf("fixture xorb has %d chunks, want at least 8", len(offsets))
			return
		}
		xorbFixtureVal.offsets = offsets
	})
	if xorbFixtureErr != nil {
		t.Fatal(xorbFixtureErr)
	}
	return xorbFixtureVal
}

// slice returns chunks [chunkStart, chunkEnd) of the fixture xorb with their
// packed byte range.
func (fx xorbFixture) slice(t *testing.T, chunkStart, chunkEnd uint32) (byteStart, byteEnd int64, data []byte) {
	t.Helper()
	byteStart, byteEnd, err := xorb.ChunkDataRangeFromOffsets(fx.offsets, chunkStart, chunkEnd)
	if err != nil {
		t.Fatal(err)
	}
	return byteStart, byteEnd, fx.data[byteStart : byteEnd+1]
}

// newSegmentAgent runs an agent against a throwaway hub and returns its
// server, storage dir and a segstore over the agent's segments directory,
// so tests seed segments the way later capture stages will.
func newSegmentAgent(t *testing.T) (*httptest.Server, string, *segstore.Store) {
	t.Helper()
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	t.Cleanup(hubSrv.Close)
	dir := t.TempDir()
	srv, install := newSwappableServer(t)
	newAgentAt(t, srv, install, agent.Config{
		Upstream:   hubSrv.URL,
		StorageDir: dir,
	})
	segs, err := segstore.New(segstore.WithDir(filepath.Join(dir, "segments")))
	if err != nil {
		t.Fatal(err)
	}
	return srv, dir, segs
}

func doXorbRequest(t *testing.T, method, url string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestSegmentGateServesFromSegment(t *testing.T) {
	fx := buildXorbFixture(t)
	srv, _, segs := newSegmentAgent(t)
	segByteStart, segByteEnd, segData := fx.slice(t, 1, 4)
	if err := segs.Put(fx.hash, 1, 4, segByteStart, segByteEnd, segData); err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/v1/xorbs/default/" + fx.hash

	t.Run("byte range inside segment", func(t *testing.T) {
		a, b := segByteStart+10, segByteEnd-10
		resp := doXorbRequest(t, http.MethodGet, url, http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", a, b)}})
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/*", a, b); got != want {
			t.Fatalf("Content-Range = %q, want %q", got, want)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
			t.Fatalf("Content-Type = %q, want application/octet-stream", got)
		}
		body := readAllWithin(t, resp.Body, 10*time.Second, "segment byte range")
		if !bytes.Equal(body, fx.data[a:b+1]) {
			t.Fatalf("byte range body differs from origin xorb bytes [%d, %d]", a, b)
		}
	})

	t.Run("byte range HEAD", func(t *testing.T) {
		a, b := segByteStart, segByteEnd
		resp := doXorbRequest(t, http.MethodHead, url, http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", a, b)}})
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/*", a, b); got != want {
			t.Fatalf("Content-Range = %q, want %q", got, want)
		}
		if got, want := resp.Header.Get("Content-Length"), fmt.Sprint(b-a+1); got != want {
			t.Fatalf("Content-Length = %q, want %q", got, want)
		}
		if body := readAllWithin(t, resp.Body, 10*time.Second, "HEAD body"); len(body) != 0 {
			t.Fatalf("HEAD body has %d bytes, want 0", len(body))
		}
	})

	t.Run("chunk keyed", func(t *testing.T) {
		_, _, want := fx.slice(t, 2, 4)
		resp := doXorbRequest(t, http.MethodGet, url, http.Header{"X-Xet-Pd-Chunks": {"2-4"}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got, want := resp.Header.Get("Content-Length"), fmt.Sprint(len(want)); got != want {
			t.Fatalf("Content-Length = %q, want %q", got, want)
		}
		body := readAllWithin(t, resp.Body, 10*time.Second, "chunk keyed range")
		if !bytes.Equal(body, want) {
			t.Fatal("chunk-keyed body differs from origin packed chunks [2, 4)")
		}
	})

	t.Run("chunk keyed HEAD", func(t *testing.T) {
		_, _, want := fx.slice(t, 1, 4)
		resp := doXorbRequest(t, http.MethodHead, url, http.Header{"X-Xet-Pd-Chunks": {"1-4"}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got, want := resp.Header.Get("Content-Length"), fmt.Sprint(len(want)); got != want {
			t.Fatalf("Content-Length = %q, want %q", got, want)
		}
		if body := readAllWithin(t, resp.Body, 10*time.Second, "HEAD body"); len(body) != 0 {
			t.Fatalf("HEAD body has %d bytes, want 0", len(body))
		}
	})
}

func TestSegmentGateMisses(t *testing.T) {
	fx := buildXorbFixture(t)
	srv, _, segs := newSegmentAgent(t)
	segByteStart, segByteEnd, segData := fx.slice(t, 1, 4)
	if err := segs.Put(fx.hash, 1, 4, segByteStart, segByteEnd, segData); err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/v1/xorbs/default/" + fx.hash

	tests := []struct {
		name        string
		rangeValue  string
		chunksValue string
	}{
		{"bare GET", "", ""},
		{"range starting before segment", fmt.Sprintf("bytes=0-%d", segByteEnd), ""},
		{"range ending past segment", fmt.Sprintf("bytes=%d-%d", segByteStart, segByteEnd+1), ""},
		{"suffix range", "bytes=-5", ""},
		{"open-ended range", fmt.Sprintf("bytes=%d-", segByteStart), ""},
		{"multi range", fmt.Sprintf("bytes=%d-%d,%d-%d", segByteStart, segByteStart+1, segByteStart+3, segByteStart+4), ""},
		{"chunks not covered", "", "0-4"},
		{"chunks inverted", "", "4-2"},
		{"chunks malformed", "", "one-two"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			if tt.rangeValue != "" {
				header.Set("Range", tt.rangeValue)
			}
			if tt.chunksValue != "" {
				header.Set("X-Xet-Pd-Chunks", tt.chunksValue)
			}
			resp := doXorbRequest(t, http.MethodGet, url, header)
			_ = readAllWithin(t, resp.Body, 10*time.Second, "miss body")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
		})
	}

	// Repeated header lines are a second way to smuggle a multi-range in.
	t.Run("repeated range headers", func(t *testing.T) {
		header := http.Header{}
		header.Add("Range", fmt.Sprintf("bytes=%d-%d", segByteStart, segByteStart+1))
		header.Add("Range", fmt.Sprintf("bytes=%d-%d", segByteStart+3, segByteStart+4))
		resp := doXorbRequest(t, http.MethodGet, url, header)
		_ = readAllWithin(t, resp.Body, 10*time.Second, "miss body")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})
	t.Run("repeated chunks headers", func(t *testing.T) {
		header := http.Header{}
		header.Add("X-Xet-Pd-Chunks", "1-2")
		header.Add("X-Xet-Pd-Chunks", "2-3")
		resp := doXorbRequest(t, http.MethodGet, url, header)
		_ = readAllWithin(t, resp.Body, 10*time.Second, "miss body")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})
}

// TestSegmentGateStaleSegmentData pins that a segment whose data file went
// missing or was damaged after indexing answers 404 — for HEAD too — instead
// of confident headers over wrong bytes.
func TestSegmentGateStaleSegmentData(t *testing.T) {
	fx := buildXorbFixture(t)
	tests := []struct {
		name   string
		damage func(t *testing.T, dataPath string)
	}{
		{"removed data file", func(t *testing.T, dataPath string) {
			if err := os.Remove(dataPath); err != nil {
				t.Fatal(err)
			}
		}},
		{"truncated data file", func(t *testing.T, dataPath string) {
			if err := os.WriteFile(dataPath, []byte("short"), 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"equal-size corrupted data file", func(t *testing.T, dataPath string) {
			fi, err := os.Stat(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dataPath, make([]byte, fi.Size()), 0644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, dir, segs := newSegmentAgent(t)
			segByteStart, segByteEnd, segData := fx.slice(t, 1, 4)
			if err := segs.Put(fx.hash, 1, 4, segByteStart, segByteEnd, segData); err != nil {
				t.Fatal(err)
			}
			tt.damage(t, filepath.Join(dir, "segments", fx.hash, "1-4"))
			url := srv.URL + "/v1/xorbs/default/" + fx.hash
			rangeHeader := http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", segByteStart, segByteEnd)}}
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				resp := doXorbRequest(t, method, url, rangeHeader)
				_ = readAllWithin(t, resp.Body, 10*time.Second, "stale body")
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("%s status = %d, want 404", method, resp.StatusCode)
				}
			}
			// A first-chunk request would walk only the leading headers;
			// the gate must still reject the whole damaged file.
			for _, chunks := range []string{"1-4", "1-2"} {
				resp := doXorbRequest(t, http.MethodGet, url, http.Header{"X-Xet-Pd-Chunks": {chunks}})
				_ = readAllWithin(t, resp.Body, 10*time.Second, "stale body")
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("chunk-keyed %s status = %d, want 404", chunks, resp.StatusCode)
				}
			}
		})
	}
}

func TestSegmentGatePassThroughFullXorb(t *testing.T) {
	fx := buildXorbFixture(t)
	srv, dir, segs := newSegmentAgent(t)
	// A covering segment is present too: the full local xorb must still win.
	segByteStart, segByteEnd, segData := fx.slice(t, 1, 4)
	if err := segs.Put(fx.hash, 1, 4, segByteStart, segByteEnd, segData); err != nil {
		t.Fatal(err)
	}
	stor, err := storage.NewFileStorage(storage.WithBasePath(filepath.Join(dir, "storage")))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := xet.ParseXorbHash(fx.hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stor.PutXorb(context.Background(), "default", hash, bytes.NewReader(fx.data)); err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/v1/xorbs/default/" + fx.hash

	t.Run("full GET reaches inner CAS server", func(t *testing.T) {
		resp := doXorbRequest(t, http.MethodGet, url, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		// The gate never sets an ETag; seeing the CAS server's proves the
		// request passed through.
		if got, want := resp.Header.Get("ETag"), `"`+fx.hash+`"`; got != want {
			t.Fatalf("ETag = %q, want %q", got, want)
		}
		body := readAllWithin(t, resp.Body, 10*time.Second, "full xorb")
		if !bytes.Equal(body, fx.data) {
			t.Fatal("full xorb body differs from stored xorb")
		}
	})

	t.Run("range GET reports the full length", func(t *testing.T) {
		a, b := segByteStart+10, segByteEnd-10
		resp := doXorbRequest(t, http.MethodGet, url, http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", a, b)}})
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}
		// A concrete total length is the inner server's signature; the gate
		// would answer with an unknown total.
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", a, b, len(fx.data)); got != want {
			t.Fatalf("Content-Range = %q, want %q", got, want)
		}
		body := readAllWithin(t, resp.Body, 10*time.Second, "full xorb range")
		if !bytes.Equal(body, fx.data[a:b+1]) {
			t.Fatal("range body differs from stored xorb bytes")
		}
	})
}

func TestSegmentGateFullXorbChunkKeyed(t *testing.T) {
	fx := buildXorbFixture(t)
	srv, dir, _ := newSegmentAgent(t)
	stor, err := storage.NewFileStorage(storage.WithBasePath(filepath.Join(dir, "storage")))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := xet.ParseXorbHash(fx.hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stor.PutXorb(context.Background(), "default", hash, bytes.NewReader(fx.data)); err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/v1/xorbs/default/" + fx.hash
	byteStart, byteEnd, want := fx.slice(t, 2, 5)

	t.Run("chunk-keyed GET serves the stored range", func(t *testing.T) {
		resp := doXorbRequest(t, http.MethodGet, url, http.Header{"X-Xet-PD-Chunks": {"2-5"}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got, wantLen := resp.Header.Get("Content-Length"), fmt.Sprint(byteEnd-byteStart+1); got != wantLen {
			t.Fatalf("Content-Length = %q, want %q", got, wantLen)
		}
		body := readAllWithin(t, resp.Body, 10*time.Second, "full xorb chunks")
		if !bytes.Equal(body, want) {
			t.Fatal("chunk-keyed body differs from GetXorbDataRange slice")
		}
	})

	t.Run("HEAD sends headers only", func(t *testing.T) {
		resp := doXorbRequest(t, http.MethodHead, url, http.Header{"X-Xet-PD-Chunks": {"2-5"}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got, wantLen := resp.Header.Get("Content-Length"), fmt.Sprint(byteEnd-byteStart+1); got != wantLen {
			t.Fatalf("Content-Length = %q, want %q", got, wantLen)
		}
		if body := readAllWithin(t, resp.Body, 10*time.Second, "HEAD body"); len(body) != 0 {
			t.Fatalf("HEAD body = %d bytes, want 0", len(body))
		}
	})

	t.Run("misses", func(t *testing.T) {
		tests := []struct {
			name   string
			header http.Header
		}{
			{"out-of-bounds chunk range", http.Header{"X-Xet-PD-Chunks": {"0-9999"}}},
			{"inverted chunk range", http.Header{"X-Xet-PD-Chunks": {"5-2"}}},
			{"malformed value", http.Header{"X-Xet-PD-Chunks": {"abc"}}},
			{"repeated header lines", http.Header{"X-Xet-PD-Chunks": {"2-5", "5-8"}}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp := doXorbRequest(t, http.MethodGet, url, tt.header)
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("status = %d, want 404", resp.StatusCode)
				}
			})
		}
	})
}

func TestSegmentGateWriteStaysRejected(t *testing.T) {
	fx := buildXorbFixture(t)
	srv, _, _ := newSegmentAgent(t)
	resp := doXorbRequest(t, http.MethodPost, srv.URL+"/v1/xorbs/default/"+fx.hash, nil)
	_ = readAllWithin(t, resp.Body, 10*time.Second, "write body")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST xorb status = %d, want 403", resp.StatusCode)
	}
}
