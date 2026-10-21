package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// fixture is one footered xorb built by the real encoder, with the raw chunks it packs.
type fixture struct {
	hash   xet.XorbHash
	body   []byte
	chunks [][]byte
	hashes []xet.ChunkHash
	sizes  []uint64
}

func makeXorb(t *testing.T, seed byte, n, size int) *fixture {
	t.Helper()
	return buildXorb(t, clustertest.RandomChunks(seed, n, size))
}

// buildXorb packs chunks with the real encoder, which picks the smallest encoding per chunk.
func buildXorb(t *testing.T, chunks [][]byte) *fixture {
	t.Helper()
	x := &fixture{chunks: chunks}
	for _, chunk := range chunks {
		x.hashes = append(x.hashes, xet.ComputeChunkHash(chunk))
		x.sizes = append(x.sizes, uint64(len(chunk)))
	}
	x.hash, x.body = clustertest.PackXorb(t, chunks)
	return x
}

// compressible builds two valid encodings of one hash, LZ4 and raw footer-less, whose chunk offsets differ.
func compressible(t *testing.T, n int) (x *fixture, raw []byte) {
	t.Helper()
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = bytes.Repeat([]byte{'a' + byte(i)}, 4<<10)
	}
	x = buildXorb(t, chunks)
	for _, c := range chunks {
		size := len(c)
		raw = append(raw, 0, byte(size), byte(size>>8), byte(size>>16), 0, byte(size), byte(size>>8), byte(size>>16))
		raw = append(raw, c...)
	}
	if err := xorb.Validate(bytes.NewReader(raw), x.hash); err != nil {
		t.Fatal(err)
	}
	packed, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	if plain, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(raw)); slices.Equal(packed, plain) {
		t.Fatal("fixture encodings must pack differently")
	}
	return x, raw
}

// gate parks callers until release is closed; started is closed once the first caller arrives.
type gate struct {
	started, release chan struct{}
	once             sync.Once
}

func newGate() *gate {
	return &gate{started: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) wait() {
	g.once.Do(func() { close(g.started) })
	<-g.release
}

// Read makes a gate usable as an empty stream prefix that blocks its consumer.
func (g *gate) Read([]byte) (int, error) {
	g.wait()
	return 0, io.EOF
}

// file describes chunks [from, to) of x as one file; base offsets its indices into a combined chunk list.
func (x *fixture) file(base, from, to int) shard.FileInfo {
	var idx []int
	for i := from; i < to; i++ {
		idx = append(idx, base+i)
	}
	sum := sha256.Sum256(x.content(from, to))
	return shard.FileInfo{Hash: xet.ComputeFileHash(x.hashes[from:to], x.sizes[from:to]), SHA256: sum, ChunkIndices: idx}
}

// infos lists x's chunks for shard.BuildShard; isNew adds the CAS block ingest verifies.
func (x *fixture) infos(isNew bool) []shard.ChunkInfo {
	out := make([]shard.ChunkInfo, len(x.chunks))
	for i := range x.chunks {
		out[i] = shard.ChunkInfo{Hash: x.hashes[i], Size: uint32(x.sizes[i]), IsNew: isNew, XorbHash: x.hash, ChunkIndex: uint32(i)}
	}
	return out
}

func (x *fixture) content(from, to int) []byte {
	return bytes.Join(x.chunks[from:to], nil)
}

// decodeAll unpacks a xorb chunk stream; the decoder needs chunk-sized reads.
func decodeAll(t *testing.T, r io.Reader) []byte {
	t.Helper()
	dec, buf := xorb.NewDecoder(r, false), make([]byte, xet.MaxChunkSize)
	var out []byte
	for {
		n, err := dec.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// newTestStore keeps the former 30s relay header limit.
func newTestStore(t *testing.T, dir string) (*store, *registry) {
	t.Helper()
	reg := newRegistry(time.Minute)
	st, err := newStore(dir, reg, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return st, reg
}

// ingest stores x and the shard describing files over it, as an upload would.
func ingest(t *testing.T, st *store, x *fixture, sh *shard.Shard) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutShard(ctx, sh); err != nil {
		t.Fatal(err)
	}
}

type seen struct{ method, rng, peer, auth string }

// holder is a fake peer that serves the xorbs it is given and records every request.
type holder struct {
	*httptest.Server
	peer apis.Peer
	hook func() // runs on every xorb request before it is answered
	mu   sync.Mutex
	objs map[xet.XorbHash][]byte
	seen []seen
}

func newHolder(t *testing.T, reg *registry, id string, xs ...*fixture) *holder {
	t.Helper()
	h := &holder{objs: map[xet.XorbHash][]byte{}}
	var add []string
	for _, x := range xs {
		h.objs[x.hash] = x.body
		add = append(add, x.hash.String())
	}
	h.Server = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.Close)
	h.peer = apis.Peer{ID: id, URL: h.URL}
	reg.announce(apis.AnnounceRequest{Peer: h.peer, Epoch: "e", Full: true, Add: add})
	return h
}

func (h *holder) serve(w http.ResponseWriter, r *http.Request) {
	hash, err := xet.ParseXorbHash(path.Base(r.URL.Path))
	if err != nil || !strings.HasPrefix(r.URL.Path, "/v1/xorbs/default/") {
		http.Error(w, "bad xorb path", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.seen = append(h.seen, seen{r.Method, r.Header.Get("Range"), r.Header.Get(apis.PeerHeader), r.Header.Get("Authorization")})
	body, ok := h.objs[hash]
	h.mu.Unlock()
	if h.hook != nil {
		h.hook()
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", `"`+hash.String()+`"`)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
}

func (h *holder) requests() []seen {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.seen)
}

// drop makes h answer 404 for hash while it stays announced, like a stale registry entry.
func (h *holder) drop(hash xet.XorbHash) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.objs, hash)
}

// assertRelayed checks every request so far was marked as a relay carrying no credentials.
func (h *holder) assertRelayed(t *testing.T, min int) {
	t.Helper()
	reqs := h.requests()
	if len(reqs) < min {
		t.Fatalf("holder %s saw %d requests, want at least %d", h.peer.ID, len(reqs), min)
	}
	for _, r := range reqs {
		if r.peer == "" || r.auth != "" {
			t.Fatalf("relay request %+v must carry %s and no Authorization", r, apis.PeerHeader)
		}
	}
}

// darkHolder accepts relay connections and never answers on them, like a hung holder process.
type darkHolder struct {
	addr  string
	conns atomic.Int64 // accepted so far
}

func newDarkHolder(t *testing.T) *darkHolder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &darkHolder{addr: ln.Addr().String()}
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
			d.conns.Add(1)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done // the accept loop has returned, so held is complete
		for _, c := range held {
			_ = c.Close()
		}
	})
	return d
}

// rankOrder lists ids best first for hash; ranks depend only on the hash and the IDs, so a throwaway registry tells them.
func rankOrder(hash xet.XorbHash, ids ...string) []string {
	r := newRegistry(time.Minute)
	for _, id := range ids {
		r.announce(fullReq(apis.Peer{ID: id, URL: "http://" + id}, "e", hash.String()))
	}
	var out []string
	for _, p := range r.holders(hash.String()) {
		out = append(out, p.ID)
	}
	return out
}

// cooledWarnings counts the warnings that a holder of hash started cooling; it swaps the default logger, so its test must not run in parallel.
func cooledWarnings(t *testing.T, hash xet.XorbHash) *atomic.Int64 {
	t.Helper()
	c := &warnCounter{hash: hash.String()}
	prev, out, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(out) // SetDefault also redirected the log package
		log.SetFlags(flags)
	})
	return &c.n
}

type warnCounter struct {
	hash string
	n    atomic.Int64
}

func (c *warnCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *warnCounter) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *warnCounter) WithGroup(string) slog.Handler            { return c }

func (c *warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn && r.Message == "gateway: relay holder cooled" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "hash" && a.Value.String() == c.hash {
				c.n.Add(1)
			}
			return true
		})
	}
	return nil
}

func TestGetXorbURLPrefersLiveHolder(t *testing.T) {
	x := makeXorb(t, 1, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	ctx := context.Background()
	want := "/v1/xorbs/default/" + x.hash.String()
	if u, err := st.GetXorbURL(ctx, x.hash); err != nil || u != want {
		t.Fatalf("GetXorbURL without peers = %q, %v, want relative %q", u, err, want)
	}
	reg.announce(fullReq(testPeer(1), "e"))
	reg.announce(fullReq(testPeer(2), "e", x.hash.String()))
	if u, err := st.GetXorbURL(ctx, x.hash); err != nil || u != "http://p2"+want {
		t.Fatalf("GetXorbURL with holder = %q, %v, want holder URL", u, err)
	}
	if u, err := st.GetXorbURL(ctx, makeXorb(t, 2, 1, 4<<10).hash); err != nil || !strings.HasPrefix(u, "http://p") {
		t.Fatalf("GetXorbURL without holder = %q, %v, want picked live peer", u, err)
	}
}

func TestChunkDedupAlwaysMisses(t *testing.T) {
	x := makeXorb(t, 3, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	ingest(t, st, x, shard.BuildShard([]shard.FileInfo{x.file(0, 0, 2)}, x.infos(true)))
	ctx := context.Background()
	if _, err := st.Storage.GetShardByChunkHash(ctx, x.hashes[0]); err != nil {
		t.Fatalf("local chunk index missing: %v", err)
	}
	if _, err := st.GetShardByChunkHash(ctx, x.hashes[0]); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShardByChunkHash = %v, want fs.ErrNotExist", err)
	}
	var _ storage.Storage = st
}

func TestGetShardNeedsLocalOrHeldXorbs(t *testing.T) {
	x := makeXorb(t, 4, 3, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	fi := x.file(0, 0, 3)
	ingest(t, st, x, shard.BuildShard([]shard.FileInfo{fi}, x.infos(true)))
	ctx := context.Background()
	if _, err := st.GetShard(ctx, fi.Hash); err != nil {
		t.Fatalf("GetShard with local xorb: %v", err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetShard(ctx, fi.Hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard with all copies lost = %v, want fs.ErrNotExist", err)
	}
	if _, err := st.GetFileHashBySHA256(ctx, fi.SHA256); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetFileHashBySHA256 with all copies lost = %v, want fs.ErrNotExist", err)
	}
	if ok, err := st.HasXorb(ctx, x.hash); err != nil || ok {
		t.Fatalf("HasXorb without copies = %v, %v", ok, err)
	}
	reg.announce(fullReq(testPeer(1), "e", x.hash.String()))
	if ok, err := st.HasXorb(ctx, x.hash); err != nil || !ok {
		t.Fatalf("HasXorb with live holder = %v, %v", ok, err)
	}
	if _, err := st.GetShard(ctx, fi.Hash); err != nil {
		t.Fatalf("GetShard with live holder: %v", err)
	}
	if fh, err := st.GetFileHashBySHA256(ctx, fi.SHA256); err != nil || fh != fi.Hash {
		t.Fatalf("GetFileHashBySHA256 with live holder = %s, %v", fh, err)
	}
}

func TestGetShardChecksOnlyRequestedFile(t *testing.T) {
	x, y := makeXorb(t, 5, 2, 4<<10), makeXorb(t, 6, 2, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	ctx := context.Background()
	a, b, empty := x.file(0, 0, 2), y.file(2, 0, 2), shard.FileInfo{}
	sh := shard.BuildShard([]shard.FileInfo{a, b, empty}, slices.Concat(x.infos(true), y.infos(true)))
	if _, err := st.PutXorb(ctx, y.hash, bytes.NewReader(y.body)); err != nil {
		t.Fatal(err)
	}
	ingest(t, st, x, sh)
	if err := st.DeleteXorb(ctx, y.hash); err != nil {
		t.Fatal(err)
	}
	for _, fi := range []shard.FileInfo{a, empty} {
		if _, err := st.GetShard(ctx, fi.Hash); err != nil {
			t.Fatalf("GetShard(%s) with its xorbs local: %v", fi.Hash, err)
		}
	}
	if fh, err := st.GetFileHashBySHA256(ctx, empty.SHA256); err != nil || fh != empty.Hash {
		t.Fatalf("GetFileHashBySHA256(empty file) = %s, %v", fh, err)
	}
	if _, err := st.GetShard(ctx, b.Hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard of the lost file = %v, want fs.ErrNotExist", err)
	}
}

func TestOffsetsSidecarSurvivesDeleteAndRestart(t *testing.T) {
	x := makeXorb(t, 7, 3, 4<<10)
	dir := t.TempDir()
	st, _ := newTestStore(t, dir)
	ctx := context.Background()
	want, err := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Storage.GetXorbChunkOffsets(ctx, x.hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("local offsets after delete = %v, want fs.ErrNotExist", err)
	}
	st, _ = newTestStore(t, dir) // restart with no peers and no network
	if got, err := st.GetXorbChunkOffsets(ctx, x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("GetXorbChunkOffsets after delete+restart = %v, %v, want %v", got, err, want)
	}
	ws, we, _ := xorb.ChunkDataRangeFromOffsets(want, 1, 3)
	if start, end, err := st.GetXorbDataRange(ctx, x.hash, 1, 3); err != nil || start != ws || end != we {
		t.Fatalf("GetXorbDataRange = %d, %d, %v, want %d, %d", start, end, err, ws, we)
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || !inserted {
		t.Fatalf("PutXorb after delete = %v, %v, want inserted", inserted, err)
	}
	if err := os.Remove(st.sidecar(x.hash)); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || inserted {
		t.Fatalf("dedup PutXorb = %v, %v, want existing", inserted, err)
	}
	if got, err := st.readSidecar(x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar after dedup PutXorb = %v, %v, want repaired %v", got, err, want)
	}
}

// The xorb hash binds decoded chunks, not their packing: a deleted hash may be re-inserted with another encoding.
func TestOffsetsFollowReplacedEncoding(t *testing.T) {
	x, raw := compressible(t, 3)
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(raw))
	dir := t.TempDir()
	st, reg := newTestStore(t, dir)
	ctx := context.Background()
	fi := x.file(0, 0, 3)
	ingest(t, st, x, shard.BuildShard([]shard.FileInfo{fi}, x.infos(true)))
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(raw)); err != nil || !inserted {
		t.Fatalf("PutXorb of the raw encoding = %v, %v, want inserted", inserted, err)
	}
	if got, err := st.readSidecar(x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar after re-insertion = %v, %v, want refreshed %v", got, err, want)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	held := *x
	held.body = raw
	newHolder(t, reg, "h1", &held)
	st, err := newStore(dir, reg, 30*time.Second) // restart: only the sidecar and the holder remain
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetXorbChunkOffsets(ctx, x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("GetXorbChunkOffsets after restart = %v, %v, want %v", got, err, want)
	}
	start, end, err := st.GetXorbDataRange(ctx, x.hash, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := st.GetXorbRangeReadCloser(ctx, x.hash, start, end)
	if err != nil {
		t.Fatal(err)
	}
	plain := decodeAll(t, rc)
	_ = rc.Close()
	if !bytes.Equal(plain, x.content(1, 3)) {
		t.Fatal("relayed range does not decode to chunks 1..3 of the held encoding")
	}
	f, err := st.GetReconstructedFile(ctx, fi.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if got, err := io.ReadAll(f); err != nil || !bytes.Equal(got, x.content(0, 3)) {
		t.Fatalf("reconstruction over the held encoding differs (%d bytes, %v)", len(got), err)
	}
}

// Offsets of an evicted xorb are read from its sidecar once, then served from memory.
func TestOffsetsCacheServesEvictedXorbWithoutSidecar(t *testing.T) {
	x := makeXorb(t, 17, 3, 4<<10)
	dir := t.TempDir()
	st, _ := newTestStore(t, dir)
	ctx := context.Background()
	want, err := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	if err != nil {
		t.Fatal(err)
	}
	ws, we, _ := xorb.ChunkDataRangeFromOffsets(want, 1, 3)
	dataRange := func(when string) {
		t.Helper()
		if start, end, err := st.GetXorbDataRange(ctx, x.hash, 1, 3); err != nil || start != ws || end != we {
			t.Fatalf("GetXorbDataRange %s = %d, %d, %v, want %d, %d", when, start, end, err, ws, we)
		}
	}
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	st, _ = newTestStore(t, dir) // restart with no peers: only the sidecar knows the offsets
	dataRange("from the sidecar")
	if err := os.Remove(st.sidecar(x.hash)); err != nil {
		t.Fatal(err)
	}
	dataRange("after the sidecar it read was removed")
	st, _ = newTestStore(t, dir) // restart again so only the next put can fill the cache
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(st.sidecar(x.hash)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	dataRange("after the put's sidecar was removed and its bytes evicted")
}

// Cached offsets follow the encoding the latest put or holder gave the hash, as the sidecar does.
func TestOffsetsCacheFollowsReplacedEncoding(t *testing.T) {
	x, raw := compressible(t, 3)
	packed, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	plain, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(raw))
	dir := t.TempDir()
	st, reg := newTestStore(t, dir)
	ctx := context.Background()
	offsets := func(want []uint64, when string) {
		t.Helper()
		if got, err := st.GetXorbChunkOffsets(ctx, x.hash); err != nil || !slices.Equal(got, want) {
			t.Fatalf("GetXorbChunkOffsets %s = %v, %v, want %v", when, got, err, want)
		}
	}
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	offsets(packed, "after the packed copy was evicted")
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	offsets(plain, "after the raw re-insertion")
	if err := os.Remove(st.sidecar(x.hash)); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	offsets(plain, "after the raw copy and its sidecar were removed")
	held := *x
	held.body = raw
	h := newHolder(t, reg, "h1", &held)
	st, err := newStore(dir, reg, 30*time.Second) // restart: only the holder knows the offsets
	if err != nil {
		t.Fatal(err)
	}
	offsets(plain, "from the holder")
	asked := len(h.requests())
	if err := os.Remove(st.sidecar(x.hash)); err != nil {
		t.Fatal(err)
	}
	h.drop(x.hash)
	offsets(plain, "after the learned sidecar was removed and the holder dropped the xorb")
	if n := len(h.requests()); n != asked {
		t.Fatalf("holder asked %d more times for offsets already learned", n-asked)
	}
}

// Readers filling the cache from an evicted copy's sidecar while a put of another encoding commits must not cache over the put's offsets.
func TestReaderFillRacesPut(t *testing.T) {
	x, raw := compressible(t, 3)
	packed, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	plain, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(raw))
	// The raw copy's sidecar, padded to the largest one accepted so readers are still parsing it when the put commits.
	stale, _ := json.Marshal(plain)
	stale = append(stale, bytes.Repeat([]byte{' '}, maxSidecarBytes-len(stale))...)
	st, _ := newTestStore(t, t.TempDir())
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Dir(st.sidecar(x.hash)), 0o755); err != nil {
		t.Fatal(err)
	}
	for round := range 20 {
		st.offsets.Remove(x.hash)
		if err := os.WriteFile(st.sidecar(x.hash), stale, 0o644); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for {
					_, _ = st.GetXorbChunkOffsets(ctx, x.hash)
					select {
					case <-stop:
						return
					default:
						st.offsets.Remove(x.hash) // the next read goes back to the sidecar
					}
				}
			})
		}
		_, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body))
		close(stop)
		wg.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.DeleteXorb(ctx, x.hash); err != nil {
			t.Fatal(err)
		}
		if got, err := st.GetXorbChunkOffsets(ctx, x.hash); err != nil || !slices.Equal(got, packed) {
			t.Fatalf("round %d: GetXorbChunkOffsets after the put was evicted = %v, %v, want the put's %v", round, got, err, packed)
		}
	}
}

// writers counts the writers holding or queued on hash's writer lock.
func writers(st *store, hash xet.XorbHash) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	if w := st.writers[hash]; w != nil {
		return w.users
	}
	return 0
}

// Concurrent puts of one hash may carry different encodings; writers serialize so one inserts and the other dedups and re-records.
func TestConcurrentSameHashPutsSerialize(t *testing.T) {
	x, raw := compressible(t, 3)
	st, _ := newTestStore(t, t.TempDir())
	ctx := context.Background()
	type result struct {
		inserted bool
		err      error
	}
	g1, g2 := newGate(), newGate()
	results := make(chan result, 2)
	put := func(g *gate, body []byte) {
		inserted, err := st.PutXorb(ctx, x.hash, io.MultiReader(g, bytes.NewReader(body)))
		results <- result{inserted, err}
	}
	go put(g1, x.body)
	<-g1.started // passed the local existence check and is streaming
	go put(g2, raw)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the second writer to queue", func() bool { return writers(st, x.hash) == 2 })
	select {
	case <-g2.started:
		t.Fatal("second writer streams while the first still does")
	default:
	}
	close(g1.release)
	close(g2.release)
	inserted := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.inserted {
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("%d of 2 concurrent puts inserted, want exactly 1", inserted)
	}
	f, err := st.Storage.GetXorbReadSeekCloser(ctx, x.hash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	want, err := storage.ReadXorbChunkOffsets(f)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st.readSidecar(x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar = %v, %v, want the committed file's %v", got, err, want)
	}
	if len(st.pins) != 0 {
		t.Fatalf("pins leaked: %v", st.pins)
	}
}

// Offsets learned from a holder must yield to bytes committed locally while the holder answered.
func TestRemoteOffsetsYieldToLocalCommit(t *testing.T) {
	x, raw := compressible(t, 3)
	st, reg := newTestStore(t, t.TempDir())
	ctx := context.Background()
	h := newHolder(t, reg, "h1", x)
	g := newGate()
	h.hook = g.wait
	type result struct {
		offsets []uint64
		err     error
	}
	done := make(chan result, 1)
	go func() {
		offsets, err := st.GetXorbChunkOffsets(ctx, x.hash)
		done <- result{offsets, err}
	}()
	<-g.started // no local copy or sidecar: the holder is being asked
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(raw)); err != nil || !inserted {
		t.Fatalf("PutXorb during remote discovery = %v, %v, want inserted", inserted, err)
	}
	close(g.release)
	r := <-done
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(raw))
	if r.err != nil || !slices.Equal(r.offsets, want) {
		t.Fatalf("GetXorbChunkOffsets racing a local commit = %v, %v, want committed %v", r.offsets, r.err, want)
	}
	if got, err := st.readSidecar(x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar after remote discovery = %v, %v, want committed %v", got, err, want)
	}
}

func TestMalformedSidecarIsRejected(t *testing.T) {
	x := makeXorb(t, 8, 1, 4<<10)
	dir := t.TempDir()
	st, _ := newTestStore(t, dir)
	ctx := context.Background()
	long := make([]uint64, xet.MaxChunksPerXorb+1)
	for i := range long {
		long[i] = uint64(i+1) * 8
	}
	bad := [][]uint64{{16, 8}, {4}, long, {math.MaxUint64}, {math.MaxUint64 - 3, 4}, {apis.MaxXorbBytes + 1}, {8, apis.MaxXorbBytes + 1}}
	for _, offsets := range bad {
		if err := st.writeSidecar(x.hash, offsets); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetXorbChunkOffsets(ctx, x.hash); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("GetXorbChunkOffsets with sidecar %v... = %v, want validation error", offsets[:1], err)
		}
		if start, end, err := st.GetXorbDataRange(ctx, x.hash, 0, 1); err == nil || start < 0 || end < 0 {
			t.Errorf("GetXorbDataRange with sidecar %v... = %d, %d, %v, want an error and no negative bound", offsets[:1], start, end, err)
		}
		if _, ok := st.offsets.Get(x.hash); ok {
			t.Errorf("offsets cache holds the rejected sidecar %v...", offsets[:1])
		}
	}
	malformed := []string{"[8,16]x", "[8,16][24]", "[8,16]" + strings.Repeat(" ", maxSidecarBytes), `{"a":8}`, "[-8]", "[8.5]", "[1e3]"}
	for _, raw := range malformed {
		if err := os.WriteFile(st.sidecar(x.hash), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetXorbChunkOffsets(ctx, x.hash); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("GetXorbChunkOffsets with sidecar %.12q = %v, want decode error", raw, err)
		}
		if _, ok := st.offsets.Get(x.hash); ok {
			t.Errorf("offsets cache holds the rejected sidecar %.12q", raw)
		}
	}
	accepted := map[string][]uint64{"[]": {}, "null": nil, "[8]": {8}, "[8]" + strings.Repeat(" ", maxSidecarBytes-3): {8}, fmt.Sprintf("[%d]", apis.MaxXorbBytes): {apis.MaxXorbBytes}}
	for raw, want := range accepted {
		st, _ = newTestStore(t, dir) // a store keeps the first sidecar it accepts
		if err := os.WriteFile(st.sidecar(x.hash), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if got, err := st.GetXorbChunkOffsets(ctx, x.hash); err != nil || !slices.Equal(got, want) {
			t.Errorf("GetXorbChunkOffsets with sidecar %.12q = %v, %v, want %v", raw, got, err, want)
		}
	}
}

func TestRelayReadsFromLiveHolder(t *testing.T) {
	t.Setenv("HF_TOKEN", "hf_must_not_leak")
	x := makeXorb(t, 9, 4, 8<<10)
	st, reg := newTestStore(t, t.TempDir())
	if st.transport.MaxIdleConnsPerHost != relayIdleConns {
		t.Fatalf("relay transport keeps %d idle conns per holder, want relayIdleConns = %d", st.transport.MaxIdleConnsPerHost, relayIdleConns)
	}
	h := newHolder(t, reg, "h1", x)
	ctx := context.Background()
	rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
	if err != nil {
		t.Fatal(err)
	}
	if size, err := rs.Seek(0, io.SeekEnd); err != nil || size != int64(len(x.body)) {
		t.Fatalf("Seek end = %d, %v, want %d", size, err, len(x.body))
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(rs); err != nil || !bytes.Equal(got, x.body) {
		t.Fatalf("relayed whole xorb differs (%d bytes, %v)", len(got), err)
	}
	start, end, err := st.GetXorbDataRange(ctx, x.hash, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Seek(start, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	part := make([]byte, end-start+1)
	if _, err := io.ReadFull(rs, part); err != nil || !bytes.Equal(part, x.body[start:end+1]) {
		t.Fatalf("relayed seek+read differs: %v", err)
	}
	_ = rs.Close()
	rc, err := st.GetXorbRangeReadCloser(ctx, x.hash, start, end)
	if err != nil {
		t.Fatal(err)
	}
	plain := decodeAll(t, rc)
	_ = rc.Close()
	if !bytes.Equal(plain, x.content(1, 3)) {
		t.Fatal("relayed range does not decode to chunks 1..3")
	}
	h.assertRelayed(t, 3)
	reqs := h.requests()
	if reqs[0].method != http.MethodHead {
		t.Fatalf("first relay request %+v, want HEAD size probe before handing out the handle", reqs[0])
	}
	if last := reqs[len(reqs)-1]; last.rng != fmt.Sprintf("bytes=%d-%d", start, end) {
		t.Fatalf("range relay request %+v, want bounded Range", last)
	}
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(x.body))
	if got, err := st.readSidecar(x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("offsets learned from holder = %v, %v, want persisted %v", got, err, want)
	}
	if ok, err := st.Storage.HasXorb(ctx, x.hash); err != nil || ok {
		t.Fatalf("relay staged the xorb locally: %v, %v", ok, err)
	}
}

func TestRelaySkipsStaleHolder(t *testing.T) {
	x := makeXorb(t, 10, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	stale, live := newHolder(t, reg, "h1", x), newHolder(t, reg, "h2", x)
	if reg.holders(x.hash.String())[0].ID != stale.peer.ID {
		stale, live = live, stale
	}
	stale.drop(x.hash)
	ctx := context.Background()
	rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rs)
	_ = rs.Close()
	if err != nil || !bytes.Equal(got, x.body) {
		t.Fatalf("relay past stale holder differs: %v", err)
	}
	rc, err := st.GetXorbRangeReadCloser(ctx, x.hash, 0, 15)
	if err != nil {
		t.Fatal(err)
	}
	head, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(head, x.body[:16]) {
		t.Fatalf("range relay past stale holder differs: %v", err)
	}
	stale.assertRelayed(t, 2)
	live.assertRelayed(t, 2)
	live.drop(x.hash)
	if _, err := st.GetXorbReadSeekCloser(ctx, x.hash); err == nil {
		t.Fatal("GetXorbReadSeekCloser with every holder stale must fail")
	}
	if _, err := st.GetXorbRangeReadCloser(ctx, x.hash, 0, 15); err == nil {
		t.Fatal("GetXorbRangeReadCloser with every holder stale must fail")
	}
	if ok, _ := st.Storage.HasXorb(ctx, x.hash); ok {
		t.Fatal("relay staged the xorb locally")
	}
}

func TestRelayRespectsContextCancel(t *testing.T) {
	x := makeXorb(t, 11, 2, 8<<10)
	st, reg := newTestStore(t, t.TempDir())
	release := make(chan struct{})
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(x.body)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(x.body[:100])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(x.body[100:])
	}))
	t.Cleanup(gate.Close)
	reg.announce(apis.AnnounceRequest{Peer: apis.Peer{ID: "gate", URL: gate.URL}, Epoch: "e", Full: true, Add: []string{x.hash.String()}})
	ctx, cancel := context.WithCancel(context.Background())
	rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs.Close() }()
	prefix := make([]byte, 100)
	if _, err := io.ReadFull(rs, prefix); err != nil || !bytes.Equal(prefix, x.body[:100]) {
		t.Fatalf("prefix read: %v", err)
	}
	cancel()
	if _, err := io.ReadAll(rs); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancel = %v, want context.Canceled", err)
	}
	if _, err := st.GetXorbReadSeekCloser(ctx, x.hash); !errors.Is(err, context.Canceled) {
		t.Fatalf("open with canceled context = %v, want context.Canceled", err)
	}
	close(release)
}

// A holder that accepts connections but never answers is cut off after the relay timeout, then tried last until one PeerTTL of the registry clock has passed.
func TestRelayFailsOverFromSilentHolder(t *testing.T) {
	const attempt = 200 * time.Millisecond
	x := makeXorb(t, 18, 2, 4<<10)
	// Ranks depend only on the hash and the IDs, so a throwaway registry tells which ID is tried first.
	order := newRegistry(time.Minute)
	for _, id := range []string{"h1", "h2"} {
		order.announce(fullReq(apis.Peer{ID: id, URL: "http://" + id}, "e", x.hash.String()))
	}
	first := order.holders(x.hash.String())
	darkID, liveID := first[0].ID, first[1].ID
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			reg, now := clockedRegistry(time.Minute)
			st, err := newStore(t.TempDir(), reg, attempt)
			if err != nil {
				t.Fatal(err)
			}
			dark := newDarkHolder(t)
			silent := apis.Peer{ID: darkID, URL: scheme + "://" + dark.addr}
			reg.announce(fullReq(silent, "e", x.hash.String()))
			live := newHolder(t, reg, liveID, x)
			ctx, cancel := context.WithTimeout(context.Background(), clustertest.WaitTimeout)
			defer cancel()
			dialed := func(want int64, when string) {
				t.Helper()
				clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("connection %d to the silent holder %s", want, when), func() bool { return dark.conns.Load() >= want })
				if n := dark.conns.Load(); n != want {
					t.Fatalf("silent holder dialed %d times %s, want %d", n, when, want)
				}
			}
			readHead := func(when string) {
				t.Helper()
				rc, err := st.GetXorbRangeReadCloser(ctx, x.hash, 0, 15)
				if err != nil {
					t.Fatalf("range relay %s = %v, want the live holder", when, err)
				}
				head, err := io.ReadAll(rc)
				_ = rc.Close()
				if err != nil || !bytes.Equal(head, x.body[:16]) {
					t.Fatalf("range relay %s differs: %v", when, err)
				}
			}

			start := time.Now()
			rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
			if err != nil {
				t.Fatalf("relay past the silent holder = %v after %v, want the live holder once the %v relay timeout ends the silent attempt", err, time.Since(start).Round(time.Millisecond), attempt)
			}
			got, err := io.ReadAll(rs)
			_ = rs.Close()
			if err != nil || !bytes.Equal(got, x.body) {
				t.Fatalf("relay past the silent holder differs: %v", err)
			}
			live.assertRelayed(t, 1)
			dialed(1, "by the first relay")

			*now = now.Add(30 * time.Second) // half the silent holder's cooldown of one PeerTTL
			readHead("during the silent holder's cooldown")
			dialed(1, "during its cooldown")

			reg.announce(fullReq(silent, "e", x.hash.String())) // both stay alive past the cooldown
			reg.announce(fullReq(live.peer, "e", x.hash.String()))
			*now = now.Add(45 * time.Second)
			readHead("after the silent holder's cooldown")
			dialed(2, "after its cooldown, back in rank order")
		})
	}
}

// A relay failing on every holder names each one it tried, and the cooldowns it leaves hide no holder from availability.
func TestRelayReportsEveryHolderTried(t *testing.T) {
	const attempt = 200 * time.Millisecond
	x := makeXorb(t, 19, 2, 4<<10)
	reg := newRegistry(time.Minute)
	st, err := newStore(t.TempDir(), reg, attempt)
	if err != nil {
		t.Fatal(err)
	}
	stale := newHolder(t, reg, "h1", x)
	stale.drop(x.hash)
	ids := []string{stale.peer.ID}
	for _, id := range []string{"h2", "h3"} {
		dark := newDarkHolder(t)
		reg.announce(fullReq(apis.Peer{ID: id, URL: "http://" + dark.addr}, "e", x.hash.String()))
		ids = append(ids, id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), clustertest.WaitTimeout)
	defer cancel()
	start := time.Now()
	rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
	elapsed := time.Since(start)
	if err == nil {
		_ = rs.Close()
		t.Fatal("relay with every holder failing must fail")
	}
	for _, id := range ids {
		if want := "holder " + id + ": "; !strings.Contains(err.Error(), want) {
			t.Errorf("relay error %q lacks %q, want every holder tried reported", err, want)
		}
	}
	if want := "xorb " + x.hash.String() + ": "; !strings.HasPrefix(err.Error(), want) {
		t.Errorf("relay error %q, want prefix %q", err, want)
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("relay error %v is fs.ErrNotExist, which only a xorb without holders reports", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("relay through %d failing holders took %v, want each silent one cut off after the %v relay timeout", len(ids), elapsed, attempt)
	}
	if n := reg.aliveHolders(x.hash.String()); n != len(ids) {
		t.Errorf("aliveHolders after the failed relay = %d, want %d: cooldowns must not hide holders", n, len(ids))
	}
	if ok, err := st.HasXorb(context.Background(), x.hash); err != nil || !ok {
		t.Errorf("HasXorb after the failed relay = %v, %v, want true: cooldowns must not hide holders", ok, err)
	}
}

// A relay whose context has already ended returns its error and cools nobody, so the next relay still tries the first holder in rank order.
func TestRelayCancelCoolsNoHolder(t *testing.T) {
	x := makeXorb(t, 20, 2, 4<<10)
	ranked := rankOrder(x.hash, "h1", "h2")
	reg := newRegistry(time.Minute)
	st, err := newStore(t.TempDir(), reg, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	dark := newDarkHolder(t)
	reg.announce(fullReq(apis.Peer{ID: ranked[0], URL: "http://" + dark.addr}, "e", x.hash.String()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.remote(ctx, x.hash, 0, -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("relay with a canceled context = %v, want context.Canceled", err)
	}
	newHolder(t, reg, ranked[1], x)
	sk, err := st.remote(context.Background(), x.hash, 0, -1)
	if err != nil {
		t.Fatalf("relay past the silent holder = %v, want the live holder ranked after it", err)
	}
	_ = sk.Close()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the silent holder, first in rank order and not cooled by the canceled relay, to be dialed", func() bool { return dark.conns.Load() > 0 })
}

// Holders still cooling are tried again when no other holder is left, so the error names each of them.
func TestRelayRetriesCooledHolders(t *testing.T) {
	x := makeXorb(t, 32, 2, 4<<10)
	reg := newRegistry(time.Minute)
	st, err := newStore(t.TempDir(), reg, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"h1", "h2", "h3"}
	for _, id := range ids {
		dark := newDarkHolder(t)
		reg.announce(fullReq(apis.Peer{ID: id, URL: "http://" + dark.addr}, "e", x.hash.String()))
	}
	for _, when := range []string{"that cools every holder", "while every holder cools"} {
		_, err := st.remote(context.Background(), x.hash, 0, -1)
		for _, id := range ids {
			if want := "holder " + id + ": "; err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("relay %s = %v, want an error naming %q", when, err, want)
			}
		}
	}
}

// Only the cooling holder moves back; the others keep their rank order, which here runs against ID order.
func TestRelayKeepsRankOrderBesideCooledHolder(t *testing.T) {
	x := makeXorb(t, 33, 2, 4<<10)
	ranked := rankOrder(x.hash, "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8")
	var first, second string
	for i := 1; i < len(ranked) && first == ""; i++ {
		if ranked[i-1] > ranked[i] {
			first, second = ranked[i-1], ranked[i]
		}
	}
	if first == "" {
		t.Fatalf("holders of xorb %s rank in ID order %v, want a pair ranked against it", x.hash, ranked)
	}
	st, reg := newTestStore(t, t.TempDir())
	stale := newHolder(t, reg, "h0", x)
	stale.drop(x.hash)
	if _, err := st.remote(context.Background(), x.hash, 0, -1); err == nil {
		t.Fatal("relay through a holder that lost the xorb succeeded, want it to fail and cool")
	}
	newHolder(t, reg, first, x)
	late := newHolder(t, reg, second, x)
	sk, err := st.remote(context.Background(), x.hash, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	_ = sk.Close()
	if n := len(late.requests()); n != 0 {
		t.Fatalf("holder %s ranked after %s saw %d requests, want 0: holders beside a cooling one keep their rank order", second, first, n)
	}
}

// A cooling holder that relays again stops cooling at once, so it is tried first in rank order again.
func TestRelaySuccessEndsCooldown(t *testing.T) {
	x := makeXorb(t, 34, 2, 4<<10)
	ranked := rankOrder(x.hash, "h1", "h2")
	st, reg := newTestStore(t, t.TempDir())
	h := newHolder(t, reg, ranked[0], x)
	h.drop(x.hash)
	if _, err := st.remote(context.Background(), x.hash, 0, -1); err == nil {
		t.Fatal("relay through a holder that lost the xorb succeeded, want it to fail and cool")
	}
	h.mu.Lock()
	h.objs[x.hash] = x.body
	h.mu.Unlock()
	relay := func(when string) {
		t.Helper()
		sk, err := st.remote(context.Background(), x.hash, 0, -1)
		if err != nil {
			t.Fatalf("relay %s = %v, want success", when, err)
		}
		_ = sk.Close()
	}
	relay("through the cooling holder alone")
	late := newHolder(t, reg, ranked[1], x)
	relay("once the cooling holder relayed again")
	if n := len(late.requests()); n != 0 {
		t.Fatalf("holder %s ranked after %s saw %d requests, want 0: a successful relay ends the cooldown", ranked[1], ranked[0], n)
	}
}

// Only a holder that starts cooling is warned about: failing again while it cools stays quiet, failing once the cooldown expired starts a new one.
func TestRelayWarnsOncePerCooldown(t *testing.T) {
	x := makeXorb(t, 35, 2, 4<<10)
	reg, now := clockedRegistry(time.Minute)
	st, err := newStore(t.TempDir(), reg, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stale := newHolder(t, reg, "h1", x)
	stale.drop(x.hash)
	warned := cooledWarnings(t, x.hash)
	fail := func(when string, want int64) {
		t.Helper()
		if _, err := st.remote(context.Background(), x.hash, 0, -1); err == nil {
			t.Fatalf("relay %s succeeded, want the stale holder to fail", when)
		}
		if n := warned.Load(); n != want {
			t.Fatalf("cooled warnings after the relay %s = %d, want %d", when, n, want)
		}
	}
	fail("that starts the cooldown", 1)
	fail("during the cooldown", 1)
	*now = now.Add(time.Minute) // one PeerTTL: both the cooldown and the holder's announce have expired
	reg.announce(fullReq(stale.peer, "e", x.hash.String()))
	fail("after the cooldown", 2)
}

func TestReconstructedFileFromRemoteOnlyXorb(t *testing.T) {
	x := makeXorb(t, 12, 4, 8<<10)
	st, reg := newTestStore(t, t.TempDir())
	fi := x.file(0, 1, 4)
	ingest(t, st, x, shard.BuildShard([]shard.FileInfo{fi}, x.infos(true)))
	ctx := context.Background()
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetReconstructedFile(ctx, fi.SHA256); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetReconstructedFile with all copies lost = %v, want fs.ErrNotExist", err)
	}
	h := newHolder(t, reg, "h1", x)
	f, err := st.GetReconstructedFile(ctx, fi.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	want := x.content(1, 4)
	if got, err := io.ReadAll(f); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reconstructed bytes differ (%d bytes, %v)", len(got), err)
	}
	if _, err := f.Seek(int64(len(want))-1000, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if tail, err := io.ReadAll(f); err != nil || !bytes.Equal(tail, want[len(want)-1000:]) {
		t.Fatalf("reconstructed tail differs: %v", err)
	}
	h.assertRelayed(t, 1)
	if ok, _ := st.Storage.HasXorb(ctx, x.hash); ok {
		t.Fatal("bridge staged the xorb locally")
	}
}

func TestPutShardStagesRemoteOnlyXorb(t *testing.T) {
	x := makeXorb(t, 13, 3, 8<<10)
	st, reg := newTestStore(t, t.TempDir())
	ctx := context.Background()
	a := x.file(0, 0, 3)
	first := shard.BuildShard([]shard.FileInfo{a}, x.infos(true))
	ingest(t, st, x, first)
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	b := x.file(0, 1, 3)
	second := shard.BuildShard([]shard.FileInfo{b}, x.infos(false))
	if _, err := st.PutShard(ctx, second); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("PutShard with no copy of the referenced xorb = %v, want fs.ErrNotExist", err)
	}
	h := newHolder(t, reg, "h1", x)
	if inserted, err := st.PutShard(ctx, second); err != nil || !inserted {
		t.Fatalf("PutShard over remote-only xorb = %v, %v, want inserted", inserted, err)
	}
	h.assertRelayed(t, 1)
	if ok, err := st.Storage.HasXorb(ctx, x.hash); err != nil || !ok {
		t.Fatalf("referenced xorb not staged locally: %v, %v", ok, err)
	}
	if _, err := st.GetShard(ctx, b.Hash); err != nil {
		t.Fatalf("GetShard of the new file: %v", err)
	}
	if len(st.pins) != 0 {
		t.Fatalf("pins held after PutShard: %v", st.pins)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	bogus := x.file(0, 0, 2)
	bogus.SHA256[0] ^= 1
	if _, err := st.PutShard(ctx, shard.BuildShard([]shard.FileInfo{bogus}, x.infos(false))); !errors.Is(err, storage.ErrInvalidShard) {
		t.Fatalf("PutShard with a wrong SHA-256 over a staged xorb = %v, want ErrInvalidShard", err)
	}
	if len(st.pins) != 0 {
		t.Fatalf("pins held after failed PutShard: %v", st.pins)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	h.drop(x.hash)
	before := len(h.requests())
	if inserted, err := st.PutShard(ctx, first); err != nil || inserted {
		t.Fatalf("PutShard of an indexed file = %v, %v, want keep-first no-op", inserted, err)
	}
	if after := len(h.requests()); after != before {
		t.Fatalf("no-op PutShard contacted holders (%d requests)", after-before)
	}
}

func TestPutShardPinsStagedXorbsDuringVerification(t *testing.T) {
	x, y := makeXorb(t, 15, 2, 4<<10), makeXorb(t, 16, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	h := newHolder(t, reg, "h1", y)
	errs := make(chan error, 8)
	h.hook = func() { errs <- st.DeleteXorb(ctx, x.hash) }
	files := []shard.FileInfo{x.file(0, 0, 2), y.file(2, 0, 2)}
	sh := shard.BuildShard(files, slices.Concat(x.infos(false), y.infos(false)))
	if inserted, err := st.PutShard(ctx, sh); err != nil || !inserted {
		t.Fatalf("PutShard = %v, %v, want inserted", inserted, err)
	}
	if len(errs) == 0 {
		t.Fatal("holder was never asked for the remote-only xorb")
	}
	for len(errs) > 0 {
		if err := <-errs; !errors.Is(err, errPinned) {
			t.Fatalf("DeleteXorb during PutShard = %v, want errPinned", err)
		}
	}
	for _, f := range []*fixture{x, y} {
		if ok, err := st.Storage.HasXorb(ctx, f.hash); err != nil || !ok {
			t.Fatalf("xorb %s not local after PutShard: %v, %v", f.hash, ok, err)
		}
	}
	if len(st.pins) != 0 {
		t.Fatalf("pins held after PutShard: %v", st.pins)
	}
}

func TestDeleteXorbRefusedWhilePinned(t *testing.T) {
	x := makeXorb(t, 14, 1, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	ctx := context.Background()
	if _, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil {
		t.Fatal(err)
	}
	st.pin(x.hash)
	if err := st.DeleteXorb(ctx, x.hash); !errors.Is(err, errPinned) {
		t.Fatalf("DeleteXorb while pinned = %v, want errPinned", err)
	}
	if ok, _ := st.Storage.HasXorb(ctx, x.hash); !ok {
		t.Fatal("pinned xorb was deleted")
	}
	st.unpin(x.hash)
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Storage.HasXorb(ctx, x.hash); ok {
		t.Fatal("unpinned xorb survived DeleteXorb")
	}
	if _, err := st.readSidecar(x.hash); err != nil {
		t.Fatalf("sidecar must survive DeleteXorb: %v", err)
	}
	if len(st.pins) != 0 {
		t.Fatalf("pins leaked: %v", st.pins)
	}
}

// Each holder a relay tries is one attempt and each failed attempt one failure; a xorb without holders tries none.
func TestRelayCounters(t *testing.T) {
	x, y := makeXorb(t, 68, 2, 4<<10), makeXorb(t, 69, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	stale, live := newHolder(t, reg, "h1", x), newHolder(t, reg, "h2", x)
	if reg.holders(x.hash.String())[0].ID != stale.peer.ID {
		stale, live = live, stale
	}
	stale.drop(x.hash)
	relays := func() apis.RelayCounters {
		return apis.RelayCounters{Attempts: st.relayTries.Load(), Failures: st.relayErrs.Load()}
	}
	ctx := context.Background()
	rs, err := st.GetXorbReadSeekCloser(ctx, x.hash)
	if err != nil {
		t.Fatal(err)
	}
	_ = rs.Close()
	if got := relays(); got != (apis.RelayCounters{Attempts: 2, Failures: 1}) {
		t.Fatalf("relay counters past the stale holder = %+v, want 2 attempts and 1 failure", got)
	}
	live.drop(x.hash)
	before := relays()
	if _, err := st.GetXorbReadSeekCloser(ctx, x.hash); err == nil {
		t.Fatal("GetXorbReadSeekCloser with every holder stale must fail")
	}
	// The stale holder now cools and is tried last, so only the increments are order-free.
	if got := relays(); got.Attempts-before.Attempts != 2 || got.Failures-before.Failures != 2 {
		t.Fatalf("relay counters %+v -> %+v with every holder stale, want 2 more attempts and 2 more failures", before, got)
	}
	before = relays()
	if _, err := st.GetXorbReadSeekCloser(ctx, y.hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetXorbReadSeekCloser of a xorb without holders = %v, want fs.ErrNotExist", err)
	}
	if got := relays(); got != before {
		t.Fatalf("relay counters %+v -> %+v for a xorb without holders, want no attempt", before, got)
	}
}
