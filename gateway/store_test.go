package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
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

func TestGetXorbURLPrefersClientNodePeer(t *testing.T) {
	x := makeXorb(t, 1, 2, 4<<10)
	st, reg := newTestStore(t, t.TempDir())
	onN1 := withClientNode(context.Background(), "n1")
	want := "/v1/xorbs/default/" + x.hash.String()
	if u, err := st.GetXorbURL(onN1, x.hash); err != nil || u != want {
		t.Fatalf("GetXorbURL without peers = %q, %v, want relative %q", u, err, want)
	}
	reg.announce(fullReq(nodePeer(1, "n1"), "e"))
	reg.announce(fullReq(nodePeer(2, "n2"), "e", x.hash.String()))
	if u, err := st.GetXorbURL(onN1, x.hash); err != nil || u != "http://p1"+want {
		t.Fatalf("GetXorbURL for a client on n1 = %q, %v, want the peer on n1", u, err)
	}
	for _, ctx := range []context.Context{withClientNode(context.Background(), "n9"), context.Background()} {
		if u, err := st.GetXorbURL(ctx, x.hash); err != nil || u != "http://p2"+want {
			t.Fatalf("GetXorbURL for a client on %q = %q, %v, want the holder", clientNode(ctx), u, err)
		}
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
	if err := os.Remove(sidecarPath(st, x.hash)); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.PutXorb(ctx, x.hash, bytes.NewReader(x.body)); err != nil || inserted {
		t.Fatalf("dedup PutXorb = %v, %v, want existing", inserted, err)
	}
	if got, err := sidecarOffsets(st, x.hash); err != nil || !slices.Equal(got, want) {
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
	if got, err := sidecarOffsets(st, x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar after re-insertion = %v, %v, want refreshed %v", got, err, want)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	held := *x
	held.body = raw
	newHolder(t, reg, "h1", &held)
	st, err := newStore(dir, reg) // restart: only the sidecar and the holder remain
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
	if got, err := sidecarOffsets(st, x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar = %v, %v, want the committed file's %v", got, err, want)
	}
	if p := pins(st); len(p) != 0 {
		t.Fatalf("pins leaked: %v", p)
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
	if got, err := sidecarOffsets(st, x.hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar after remote discovery = %v, %v, want committed %v", got, err, want)
	}
}

func TestMalformedSidecarIsRejected(t *testing.T) {
	x := makeXorb(t, 8, 1, 4<<10)
	st, _ := newTestStore(t, t.TempDir())
	ctx := context.Background()
	long := make([]uint64, xet.MaxChunksPerXorb+1)
	for i := range long {
		long[i] = uint64(i+1) * 8
	}
	bad := [][]uint64{{16, 8}, {4}, long, {math.MaxUint64}, {math.MaxUint64 - 3, 4}, {apis.MaxXorbBytes + 1}, {8, apis.MaxXorbBytes + 1}}
	for _, offsets := range bad {
		if err := putSidecar(st, x.hash, offsets); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetXorbChunkOffsets(ctx, x.hash); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("GetXorbChunkOffsets with sidecar %v... = %v, want validation error", offsets[:1], err)
		}
		if start, end, err := st.GetXorbDataRange(ctx, x.hash, 0, 1); err == nil || start < 0 || end < 0 {
			t.Errorf("GetXorbDataRange with sidecar %v... = %d, %d, %v, want an error and no negative bound", offsets[:1], start, end, err)
		}
	}
	malformed := []string{"[8,16]x", "[8,16][24]", "[8,16]" + strings.Repeat(" ", maxSidecarBytes), `{"a":8}`, "[-8]", "[8.5]", "[1e3]"}
	for _, raw := range malformed {
		if err := os.WriteFile(sidecarPath(st, x.hash), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetXorbChunkOffsets(ctx, x.hash); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("GetXorbChunkOffsets with sidecar %.12q = %v, want decode error", raw, err)
		}
	}
	accepted := map[string][]uint64{"[]": {}, "null": nil, "[8]": {8}, "[8]" + strings.Repeat(" ", maxSidecarBytes-3): {8}, fmt.Sprintf("[%d]", apis.MaxXorbBytes): {apis.MaxXorbBytes}}
	for raw, want := range accepted {
		if err := os.WriteFile(sidecarPath(st, x.hash), []byte(raw), 0o644); err != nil {
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
	if got, err := sidecarOffsets(st, x.hash); err != nil || !slices.Equal(got, want) {
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
	if p := pins(st); len(p) != 0 {
		t.Fatalf("pins held after PutShard: %v", p)
	}
	if err := st.DeleteXorb(ctx, x.hash); err != nil {
		t.Fatal(err)
	}
	bogus := x.file(0, 0, 2)
	bogus.SHA256[0] ^= 1
	if _, err := st.PutShard(ctx, shard.BuildShard([]shard.FileInfo{bogus}, x.infos(false))); !errors.Is(err, storage.ErrInvalidShard) {
		t.Fatalf("PutShard with a wrong SHA-256 over a staged xorb = %v, want ErrInvalidShard", err)
	}
	if p := pins(st); len(p) != 0 {
		t.Fatalf("pins held after failed PutShard: %v", p)
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
	if p := pins(st); len(p) != 0 {
		t.Fatalf("pins held after PutShard: %v", p)
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
	if _, err := sidecarOffsets(st, x.hash); err != nil {
		t.Fatalf("sidecar must survive DeleteXorb: %v", err)
	}
	if p := pins(st); len(p) != 0 {
		t.Fatalf("pins leaked: %v", p)
	}
}
