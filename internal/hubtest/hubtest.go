// Package hubtest provides a xet-capable fake upstream hub fixture shared by agent and e2e tests.
package hubtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// hubCAS adapts the hub fixture's storage to upload.ClientAdapter and keeps
// a copy of every packed xorb, so tests compare captured segments against
// the exact bytes the hub serves.
type hubCAS struct {
	stor storage.Storage
	hub  *Hub
}

func (c *hubCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return c.stor.HasXorb(ctx, "default", xorbHash)
}

func (c *hubCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	c.hub.mu.Lock()
	c.hub.xorbs[xorbHash.String()] = data
	c.hub.mu.Unlock()
	wasInserted, err := c.stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return &upload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (c *hubCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	if _, err := c.stor.PutShard(ctx, shardObj); err != nil {
		return nil, err
	}
	return &upload.ShardUploadResponse{Result: 1}, nil
}

func (c *hubCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, h := range chunkHashes {
		results[h] = &upload.DeduplicationResult{ChunkHash: h, IsNew: true}
	}
	return results, nil
}

// File is one hub-side ingested file.
type File struct {
	data     []byte
	SHA      string
	fileHash string
}

// Hub is a xet-capable upstream hub fixture: resolve responses carry the
// metadata and Link headers a real hub sends for xet files, /xet-token mints
// a static CAS token pointing back at the hub, and the pinned CAS server
// serves reconstructions and xorb ranges from real hub-side storage.
type Hub struct {
	Server *httptest.Server
	Commit string

	stor *storage.FileStorage

	mu        sync.Mutex
	files     map[string]*File
	xorbs     map[string][]byte
	xorbGets  int
	hashGets  map[string]int
	xorbBytes int64
	gated     map[string]bool
	gateWait  chan struct{}
	xorbDelay time.Duration
}

// New starts a hub fixture whose server shuts down on test cleanup.
func New(t *testing.T) *Hub {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	hub := &Hub{
		stor:     stor,
		Commit:   strings.Repeat("cd", 20),
		files:    map[string]*File{},
		xorbs:    map[string][]byte{},
		hashGets: map[string]int{},
		gated:    map[string]bool{},
		gateWait: make(chan struct{}),
	}
	cas := server.NewHandler(
		server.WithStorage(stor),
		server.WithNext(http.HandlerFunc(hub.serveHub)),
	)
	hub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			hash := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
			hub.mu.Lock()
			hub.xorbGets++
			hub.hashGets[hash]++
			delay := hub.xorbDelay
			hub.mu.Unlock()
			if delay > 0 {
				time.Sleep(delay)
			}
			if !hub.awaitUngated(r, hash) {
				return
			}
			cw := &countingWriter{ResponseWriter: w}
			cas.ServeHTTP(cw, r)
			hub.mu.Lock()
			hub.xorbBytes += cw.n
			hub.mu.Unlock()
			return
		}
		cas.ServeHTTP(w, r)
	}))
	t.Cleanup(hub.Server.Close)
	return hub
}

// countingWriter counts body bytes actually written on the xorb route.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

// XorbFetchCount returns how many xorb downloads the hub has served.
func (h *Hub) XorbFetchCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.xorbGets
}

// XorbHashGets returns how many xorb-route GETs one hash has received.
func (h *Hub) XorbHashGets(hash string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hashGets[hash]
}

// XorbRouteBytes returns the total body bytes served on the xorb route.
func (h *Hub) XorbRouteBytes() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.xorbBytes
}

// SetGated blocks xorb-route GETs for the given hashes until ungated —
// blocking rather than erroring keeps flights alive, since a failing
// prefetcher entry trips a data race inside the pinned download package.
func (h *Hub) SetGated(gated bool, hashes ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, hash := range hashes {
		h.gated[hash] = gated
	}
	close(h.gateWait)
	h.gateWait = make(chan struct{})
}

// awaitUngated blocks while hash is gated; it reports false when the
// request died first.
func (h *Hub) awaitUngated(r *http.Request, hash string) bool {
	for {
		h.mu.Lock()
		gated := h.gated[hash]
		wait := h.gateWait
		h.mu.Unlock()
		if !gated {
			return true
		}
		select {
		case <-wait:
		case <-r.Context().Done():
			return false
		}
	}
}

// SetXorbDelay adds an artificial delay to every xorb-route GET.
func (h *Hub) SetXorbDelay(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.xorbDelay = d
}

// hubResolveRe splits a hub resolve path around its revision element.
var hubResolveRe = regexp.MustCompile(`^(/.+?/resolve/)[^/]+(/.+)$`)

// Set ingests data into hub storage through the real upload pipeline and
// registers it under the resolve path and, like a real hub, under the same
// path with the revision replaced by the commit.
func (h *Hub) Set(t *testing.T, path string, data []byte) *File {
	t.Helper()
	fileHash, err := upload.UploadFile(context.Background(), &hubCAS{stor: h.stor, hub: h}, bytes.NewReader(data),
		upload.WithEnableSHA256(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	f := &File{data: data, SHA: hex.EncodeToString(sum[:]), fileHash: fileHash.String()}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files[path] = f
	if m := hubResolveRe.FindStringSubmatch(path); m != nil {
		h.files[m[1]+h.Commit+m[2]] = f
	}
	return f
}

// SingleXorb returns the hash and packed bytes of the hub's only xorb.
func (h *Hub) SingleXorb(t *testing.T) (string, []byte) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.xorbs) != 1 {
		t.Fatalf("got %d hub xorbs, want 1", len(h.xorbs))
	}
	for hash, data := range h.xorbs {
		return hash, data
	}
	return "", nil
}

// Xorb describes one xorb of a multi-xorb hub file, in file order.
type Xorb struct {
	Hash   string
	packed int64
	Chunks uint32
}

// MultiXorbFile is one hub file hand-built across several xorbs.
type MultiXorbFile struct {
	fileHash string
	Xorbs    []Xorb
}

// PackedTotal is the sum of all packed xorb sizes — what a full cold
// download costs the origin in xorb-route bytes.
func (f *MultiXorbFile) PackedTotal() int64 {
	var total int64
	for _, x := range f.Xorbs {
		total += x.packed
	}
	return total
}

// SetMultiXorb registers a file split across several xorbs, so its
// reconstruction carries one term per xorb: the data is chunked into
// fixed-size pieces, grouped chunksPerXorb to a xorb, each xorb encoded and
// stored, and the shard hand-built with one file entry per xorb. The file
// hash is synthetic — resolve, shard and reconstruction only need it
// self-consistent — while PutShard recomputes and indexes the true SHA-256
// by decoding the entries, proving the fixture reassembles exactly.
func (h *Hub) SetMultiXorb(t *testing.T, path string, data []byte, chunkSize, chunksPerXorb int) *MultiXorbFile {
	t.Helper()
	sum := sha256.Sum256(data)
	fileSum := sha256.Sum256(append([]byte("xet-pd multi-xorb file: "), data...))
	fileHash, err := xet.ParseFileHash(hex.EncodeToString(fileSum[:]))
	if err != nil {
		t.Fatal(err)
	}
	var chunks [][]byte
	for off := 0; off < len(data); off += chunkSize {
		chunks = append(chunks, data[off:min(off+chunkSize, len(data))])
	}
	ctx := context.Background()
	fb := shard.FileBlock{FileHash: fileHash}
	sh := &shard.Shard{}
	mf := &MultiXorbFile{fileHash: fileHash.String()}
	for start := 0; start < len(chunks); start += chunksPerXorb {
		group := chunks[start:min(start+chunksPerXorb, len(chunks))]
		var buf bytes.Buffer
		enc := xorb.NewEncoder(&buf, false)
		var casChunks []shard.CASChunkSequenceEntry
		var unpacked uint32
		for _, c := range group {
			if _, err := enc.Write(c); err != nil {
				t.Fatal(err)
			}
			casChunks = append(casChunks, shard.CASChunkSequenceEntry{
				ChunkHash:        xet.ComputeChunkHash(c),
				ByteRangeStart:   unpacked,
				UnpackedSegBytes: uint32(len(c)),
			})
			unpacked += uint32(len(c))
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		xorbHash := enc.SummoryHash()
		packed := bytes.Clone(buf.Bytes())
		if _, err := h.stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(packed)); err != nil {
			t.Fatal(err)
		}
		fb.Entries = append(fb.Entries, shard.FileDataSequenceEntry{
			CASHash:          xorbHash,
			UnpackedSegBytes: unpacked,
			ChunkIndexStart:  0,
			ChunkIndexEnd:    uint32(len(group)),
		})
		sh.CASInfos = append(sh.CASInfos, shard.CASBlock{
			CASHash:        xorbHash,
			Chunks:         casChunks,
			NumBytesInCAS:  unpacked,
			NumBytesOnDisk: uint32(len(packed)),
		})
		h.mu.Lock()
		h.xorbs[xorbHash.String()] = packed
		h.mu.Unlock()
		mf.Xorbs = append(mf.Xorbs, Xorb{Hash: xorbHash.String(), packed: int64(len(packed)), Chunks: uint32(len(group))})
	}
	sh.Files = []shard.FileBlock{fb}
	if _, err := h.stor.PutShard(ctx, sh); err != nil {
		t.Fatal(err)
	}
	f := &File{data: data, SHA: hex.EncodeToString(sum[:]), fileHash: fileHash.String()}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files[path] = f
	if m := hubResolveRe.FindStringSubmatch(path); m != nil {
		h.files[m[1]+h.Commit+m[2]] = f
	}
	return mf
}

// serveHub answers everything the CAS server does not: the token endpoint
// and hub resolve paths, shaped like the real hub's xet-capable responses.
func (h *Hub) serveHub(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/xet-token" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"casUrl":      h.Server.URL,
			"accessToken": "static-hub-token",
			"exp":         time.Now().Add(time.Hour).Unix(),
		})
		return
	}
	h.mu.Lock()
	f, ok := h.files[r.URL.Path]
	h.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	etag := `"` + f.SHA + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Linked-Etag", etag)
	w.Header().Set("X-Linked-Size", fmt.Sprint(len(f.data)))
	w.Header().Set("X-Repo-Commit", h.Commit)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Add("Link", fmt.Sprintf("<%s/xet-token>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", h.Server.URL, h.Server.URL, f.fileHash))
	w.Header().Set("X-Xet-Hash", f.fileHash)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.data))
}
