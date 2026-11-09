package peer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const (
	selfID     = "self"
	selfURL    = "http://self.test"
	testSecret = "s3cret"
)

// newTestStore opens a store on dir as the peer selfID at selfURL that looks holders up at tracker with testSecret.
func newTestStore(t *testing.T, dir, tracker string) *store {
	t.Helper()
	st, err := newStore(storeConfig{
		Dir:         dir,
		Self:        apis.Peer{ID: selfID, URL: selfURL},
		Tracker:     tracker,
		Secret:      testSecret,
		Client:      apis.NewClient(apis.NewTransport()),
		MaxPulls:    4,
		PullTimeout: time.Minute,
		Metrics:     newPeerMetrics(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// newTracker returns a tracker requiring testSecret.
func newTracker(t *testing.T) *clustertest.FakeTracker {
	t.Helper()
	tracker := clustertest.NewFakeTracker(t)
	tracker.Secret = testSecret
	return tracker
}

// testFile is a file spanning one xorb per seed, as ingest stores it and a complete holder serves it.
type testFile struct {
	file  apis.File
	hash  xet.FileHash
	xorbs map[xet.XorbHash][]byte
	resp  apis.ShardResponse
}

// makeFile ingests a file of two chunks per seed, one xorb per seed, into a scratch local store and keeps what a complete holder serves for it.
func makeFile(t *testing.T, seeds ...byte) testFile {
	t.Helper()
	ctx := context.Background()
	src, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	tf := testFile{xorbs: map[xet.XorbHash][]byte{}, resp: apis.ShardResponse{Offsets: map[string][]uint64{}}}
	var infos []shard.ChunkInfo
	var hashes []xet.ChunkHash
	var sizes []uint64
	sum := sha256.New()
	for _, seed := range seeds {
		chunks := clustertest.RandomChunks(seed, 2, 8<<10)
		hash, body := clustertest.PackXorb(t, chunks)
		if _, err := src.PutXorb(ctx, hash, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		offsets, err := src.GetXorbChunkOffsets(ctx, hash)
		if err != nil {
			t.Fatal(err)
		}
		tf.xorbs[hash], tf.resp.Offsets[hash.String()] = body, offsets
		for i, c := range chunks {
			h := xet.ComputeChunkHash(c)
			infos = append(infos, shard.ChunkInfo{Hash: h, Size: uint32(len(c)), IsNew: true, XorbHash: hash, ChunkIndex: uint32(i)})
			hashes, sizes = append(hashes, h), append(sizes, uint64(len(c)))
			sum.Write(c)
		}
	}
	fi := shard.FileInfo{Hash: xet.ComputeFileHash(hashes, sizes), SHA256: [32]byte(sum.Sum(nil))}
	for i := range infos {
		fi.ChunkIndices = append(fi.ChunkIndices, i)
	}
	if _, err := src.PutShard(ctx, shard.BuildShard([]shard.FileInfo{fi}, infos)); err != nil {
		t.Fatal(err)
	}
	sh, err := src.GetShard(ctx, fi.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if tf.resp.Shard, _, err = storage.EncodeShard(sh); err != nil {
		t.Fatal(err)
	}
	tf.hash, tf.file = fi.Hash, apis.File{Hash: fi.Hash.String(), SHA256: hex.EncodeToString(fi.SHA256[:])}
	return tf
}

// shard decodes a fresh copy of the file's shard.
func (tf testFile) shard(t *testing.T) *shard.Shard {
	t.Helper()
	sh, err := storage.DecodeStoredShard(bytes.NewReader(tf.resp.Shard))
	if err != nil {
		t.Fatal(err)
	}
	return sh
}

// xorbHashes lists the file's xorbs sorted.
func (tf testFile) xorbHashes() []xet.XorbHash {
	return sortedXorbs(slices.Collect(maps.Keys(tf.xorbs)))
}

func sortedXorbs(hashes []xet.XorbHash) []xet.XorbHash {
	return slices.SortedFunc(slices.Values(hashes), func(a, b xet.XorbHash) int { return strings.Compare(a.String(), b.String()) })
}

// ingest stores the file into st as an upload does: its xorbs, then its shard.
func ingest(t *testing.T, st *store, tf testFile) {
	t.Helper()
	ctx := context.Background()
	for hash, body := range tf.xorbs {
		if _, err := st.PutXorb(ctx, hash, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.PutShard(ctx, tf.shard(t)); err != nil {
		t.Fatal(err)
	}
}

// localBytes reads the local copy of hash in st.
func localBytes(t *testing.T, st *store, hash xet.XorbHash) []byte {
	t.Helper()
	rs, err := st.Storage.GetXorbReadSeekCloser(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs.Close() }()
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestGetXorbURLNamesSelf(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	hash, _ := clustertest.MakeXorb(t, 1, 1, 4<<10)
	want := selfURL + apis.XorbPathPrefix + hash.String()
	if got, err := st.GetXorbURL(context.Background(), hash); err != nil || got != want {
		t.Fatalf("GetXorbURL = %q, %v, want %q", got, err, want)
	}
}

func TestHasXorbLooksUpOnlyNonLocalXorbs(t *testing.T) {
	tracker := newTracker(t)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	own, body := clustertest.MakeXorb(t, 1, 2, 4<<10)
	remote, _ := clustertest.MakeXorb(t, 2, 2, 4<<10)
	missing, _ := clustertest.MakeXorb(t, 3, 2, 4<<10)
	if _, err := st.PutXorb(context.Background(), own, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if !clustertest.HasXorb(t, st, own) || len(tracker.Lookups()) != 0 {
		t.Fatalf("HasXorb(local) = false or looked up (%d lookups), want true without a lookup", len(tracker.Lookups()))
	}
	tracker.Hold(clustertest.NewFakePeer(t, "holder").Peer(), remote)
	if !clustertest.HasXorb(t, st, remote) {
		t.Fatal("HasXorb of a held xorb = false, want true")
	}
	lookups := tracker.Lookups()
	if len(lookups) != 1 || lookups[0].Req.Peer != selfID || !slices.Equal(lookups[0].Req.Xorbs, []string{remote.String()}) {
		t.Fatalf("lookups = %+v, want one for %s as %s", lookups, remote, selfID)
	}
	if clustertest.HasXorb(t, st, missing) {
		t.Fatal("HasXorb of an unheld xorb = true, want false")
	}
}

func TestPutXorbStoresBytesAndOffsetsThenReports(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	ev := watch(st)
	hash, body := clustertest.MakeXorb(t, 4, 3, 8<<10)
	if inserted, err := st.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil || !inserted {
		t.Fatalf("PutXorb = %v, %v, want inserted", inserted, err)
	}
	if got := localBytes(t, st, hash); !bytes.Equal(got, body) {
		t.Fatalf("stored %d bytes differ from the %d put", len(got), len(body))
	}
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(body))
	if got, err := sidecarOffsets(st, hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar = %v, %v, want %v", got, err, want)
	}
	if got := ev.storedXorbs(); !slices.Equal(got, []storedEvent{{hash, int64(len(body)), true}}) {
		t.Fatalf("onStored calls = %+v, want one inserted %s of %d bytes", got, hash, len(body))
	}
}

func TestPutShardReportsItsFiles(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	ev := watch(st)
	tf := makeFile(t, 5, 6)
	ingest(t, st, tf)
	files := ev.knownFiles()
	if len(files) != 1 || files[0].file != tf.file || !slices.Equal(sortedXorbs(files[0].xorbs), tf.xorbHashes()) {
		t.Fatalf("onFile calls = %+v, want one for %+v over %v", files, tf.file, tf.xorbHashes())
	}
}

func TestLocalReadReportsReadThenIdleOnClose(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	hash, body := clustertest.MakeXorb(t, 7, 2, 8<<10)
	if _, err := st.PutXorb(context.Background(), hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	ev := watch(st)
	rs, err := st.GetXorbReadSeekCloser(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if got := ev.readXorbs(); !slices.Equal(got, []xet.XorbHash{hash}) || ev.idleCount() != 0 {
		t.Fatalf("open: onRead %v, onIdle %d, want [%s] and 0", got, ev.idleCount(), hash)
	}
	_ = rs.Close()
	if n := ev.idleCount(); n != 1 {
		t.Fatalf("onIdle after Close = %d, want 1", n)
	}
}

// newHolder returns a peer requiring testSecret on shard GETs.
func newHolder(t *testing.T, id string) *clustertest.FakePeer {
	t.Helper()
	fp := clustertest.NewFakePeer(t, id)
	fp.Secret = testSecret
	return fp
}

// digestOf parses the SHA-256 of file.
func digestOf(t *testing.T, file apis.File) [32]byte {
	t.Helper()
	raw, err := hex.DecodeString(file.SHA256)
	if err != nil || len(raw) != sha256.Size {
		t.Fatalf("file %+v has no SHA-256", file)
	}
	return [32]byte(raw)
}

func TestGetFileHashBySHA256FetchesShardOfCompleteHolder(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	tf := makeFile(t, 11, 12)
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldFile(holder.Peer(), tf.file)
	tracker.Hold(holder.Peer(), tf.xorbHashes()...)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	if fh, err := st.GetFileHashBySHA256(ctx, digestOf(t, tf.file)); err != nil || fh != tf.hash {
		t.Fatalf("GetFileHashBySHA256 = %s, %v, want %s", fh, err, tf.hash)
	}
	if sh, err := st.GetShard(ctx, tf.hash); err != nil || sh.File(tf.hash) == nil {
		t.Fatalf("GetShard of the fetched file = %v, want its shard", err)
	}
	for _, h := range tf.xorbHashes() {
		if got, err := sidecarOffsets(st, h); err != nil || !slices.Equal(got, tf.resp.Offsets[h.String()]) {
			t.Fatalf("sidecar of %s = %v, %v, want the holder's offsets", h, got, err)
		}
	}
	reqs := holder.Requests()
	if len(reqs) != 1 || reqs[0].Method != http.MethodGet || reqs[0].Path != apis.ShardPathPrefix+tf.file.Hash {
		t.Fatalf("holder requests = %+v, want one shard GET and no xorb GET", reqs)
	}
	if h := reqs[0].Header; h.Get(apis.PeerHeader) != selfID || h.Get("Authorization") != "Bearer "+testSecret {
		t.Fatalf("shard GET headers = %v, want %s %s and the bearer", h, apis.PeerHeader, selfID)
	}
}

func TestShardOfAnotherSHA256IsRefused(t *testing.T) {
	tracker, liar, honest := newTracker(t), newHolder(t, "liar"), newHolder(t, "honest")
	tf := makeFile(t, 13)
	forged := tf.shard(t)
	forged.Files[0].MetadataExt.SHA256Hash[0] ^= 1
	data, _, err := storage.EncodeShard(forged)
	if err != nil {
		t.Fatal(err)
	}
	liar.ServeShard(tf.file, apis.ShardResponse{Shard: data, Offsets: tf.resp.Offsets})
	honest.ServeShard(tf.file, tf.resp)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	tracker.HoldFile(liar.Peer(), tf.file)
	if _, err := st.GetFileHashBySHA256(ctx, digestOf(t, tf.file)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetFileHashBySHA256 with only a mismatching shard = %v, want fs.ErrNotExist", err)
	}
	tracker.HoldFile(honest.Peer(), tf.file)
	if fh, err := st.GetFileHashBySHA256(ctx, digestOf(t, tf.file)); err != nil || fh != tf.hash {
		t.Fatalf("GetFileHashBySHA256 past the mismatching shard = %s, %v, want %s", fh, err, tf.hash)
	}
	if n, m := len(liar.Requests()), len(honest.Requests()); n != 2 || m != 1 {
		t.Fatalf("shard GETs: liar %d, honest %d, want 2 and 1", n, m)
	}
}

func TestGetShardNeedsEveryXorbLocalOrHeld(t *testing.T) {
	tracker := newTracker(t)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	tf := makeFile(t, 9, 10)
	ingest(t, st, tf)
	gone := tf.xorbHashes()[0]
	if err := st.DeleteXorb(ctx, gone); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetShard(ctx, tf.hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard with xorb %s neither local nor held = %v, want fs.ErrNotExist", gone, err)
	}
	if lookups := tracker.Lookups(); len(lookups) != 1 || !slices.Equal(lookups[0].Req.Xorbs, []string{gone.String()}) {
		t.Fatalf("lookups = %+v, want one batch of the non-local %s", lookups, gone)
	}
	tracker.Hold(clustertest.NewFakePeer(t, "holder").Peer(), gone)
	if _, err := st.GetShard(ctx, tf.hash); err != nil {
		t.Fatalf("GetShard with the missing xorb held = %v", err)
	}
}

func TestShardResponseCarriesShardAndOffsetsOfEveryXorb(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	tf := makeFile(t, 14, 15)
	ingest(t, st, tf)
	resp, err := st.shardResponse(context.Background(), tf.hash)
	if err != nil {
		t.Fatal(err)
	}
	if sh, err := storage.DecodeStoredShard(bytes.NewReader(resp.Shard)); err != nil || sh.File(tf.hash) == nil {
		t.Fatalf("served shard decodes to %v, want the file's shard", err)
	}
	if !maps.EqualFunc(resp.Offsets, tf.resp.Offsets, slices.Equal[[]uint64]) {
		t.Fatalf("served offsets = %v, want %v", resp.Offsets, tf.resp.Offsets)
	}
}

func TestShardResponseNeedsOffsetsOfEveryXorb(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	ctx := context.Background()
	tf := makeFile(t, 16, 17)
	ingest(t, st, tf)
	gone := tf.xorbHashes()[0]
	if err := st.DeleteXorb(ctx, gone); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(sidecarPath(st, gone)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.shardResponse(ctx, tf.hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("shardResponse without bytes or offsets of %s = %v, want fs.ErrNotExist", gone, err)
	}
}

func TestInventoryListsLocalXorbsAndKnownFilesAcrossReopen(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	ingested, fetched := makeFile(t, 18, 19), makeFile(t, 20)
	holder.ServeShard(fetched.file, fetched.resp)
	tracker.HoldFile(holder.Peer(), fetched.file)
	dir := t.TempDir()
	st := newTestStore(t, dir, tracker.URL)
	ctx := context.Background()
	ingest(t, st, ingested)
	if _, err := st.GetFileHashBySHA256(ctx, digestOf(t, fetched.file)); err != nil {
		t.Fatal(err)
	}
	wantFiles := map[apis.File][]xet.XorbHash{ingested.file: ingested.xorbHashes(), fetched.file: fetched.xorbHashes()}
	for i, st := range []*store{st, newTestStore(t, dir, tracker.URL)} {
		xorbs, files, err := st.inventory(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var gotXorbs []xet.XorbHash
		for _, x := range xorbs {
			if x.Size != int64(len(ingested.xorbs[x.Hash])) {
				t.Fatalf("open %d: inventory lists %s of %d bytes, want %d", i, x.Hash, x.Size, len(ingested.xorbs[x.Hash]))
			}
			gotXorbs = append(gotXorbs, x.Hash)
		}
		if !slices.Equal(sortedXorbs(gotXorbs), ingested.xorbHashes()) {
			t.Fatalf("open %d: inventory xorbs = %v, want the local %v", i, gotXorbs, ingested.xorbHashes())
		}
		gotFiles := map[apis.File][]xet.XorbHash{}
		for _, f := range files {
			if _, dup := gotFiles[f.File]; dup {
				t.Fatalf("open %d: inventory lists %+v twice", i, f.File)
			}
			gotFiles[f.File] = sortedXorbs(f.Xorbs)
		}
		if !maps.EqualFunc(gotFiles, wantFiles, slices.Equal[[]xet.XorbHash]) {
			t.Fatalf("open %d: inventory files = %v, want %v", i, gotFiles, wantFiles)
		}
	}
}

// rawHolder names a server answering through serve, counting the requests, to tracker as the holder id of hash.
func rawHolder(t *testing.T, tracker *clustertest.FakeTracker, id string, hash xet.XorbHash, serve http.HandlerFunc) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		serve(w, r)
	}))
	t.Cleanup(srv.Close)
	tracker.Hold(apis.Peer{ID: id, URL: srv.URL}, hash)
	return &n
}

// withStatus answers every request with code alone.
func withStatus(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

func TestRemoteReadStreamsPullThenStoresXorb(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	hash, body := clustertest.MakeXorb(t, 21, 4, 16<<10)
	const prefix = 20 << 10
	release := make(chan struct{})
	holder.GateXorb(hash, body, prefix, release)
	tracker.Hold(holder.Peer(), hash)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ev := watch(st)
	rs, err := st.GetXorbReadSeekCloser(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if size, err := rs.Seek(0, io.SeekEnd); err != nil || size != int64(len(body)) {
		t.Fatalf("Seek to the end while gated = %d, %v, want %d", size, err, len(body))
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	head := make([]byte, prefix)
	if _, err := io.ReadFull(rs, head); err != nil || !bytes.Equal(head, body[:prefix]) {
		t.Fatalf("prefix read while gated: %v", err)
	}
	close(release)
	if rest, err := io.ReadAll(rs); err != nil || !bytes.Equal(rest, body[prefix:]) {
		t.Fatalf("rest after release = %d bytes, %v, want %d", len(rest), err, len(body)-prefix)
	}
	_ = rs.Close()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the pulled xorb stored", func() bool { return len(ev.storedXorbs()) > 0 })
	if got := ev.storedXorbs(); !slices.Equal(got, []storedEvent{{hash, int64(len(body)), true}}) {
		t.Fatalf("onStored calls = %+v, want one inserted %s of %d bytes", got, hash, len(body))
	}
	if got := localBytes(t, st, hash); !bytes.Equal(got, body) {
		t.Fatalf("stored %d bytes differ from the holder's %d", len(got), len(body))
	}
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(body))
	if got, err := sidecarOffsets(st, hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("sidecar = %v, %v, want %v", got, err, want)
	}
}

func TestPullFailsOverToNextHolder(t *testing.T) {
	tracker := newTracker(t)
	hash, body := clustertest.MakeXorb(t, 22, 2, 8<<10)
	missing, serving := newHolder(t, "missing"), newHolder(t, "serving")
	tracker.Hold(missing.Peer(), hash)
	broken := rawHolder(t, tracker, "broken", hash, withStatus(http.StatusInternalServerError))
	serving.ServeXorb(hash, body)
	tracker.Hold(serving.Peer(), hash)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	rs, err := st.GetXorbReadSeekCloser(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rs)
	_ = rs.Close()
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read past failing holders = %d bytes, %v, want %d", len(got), err, len(body))
	}
	if a, b, c := len(missing.Requests()), broken.Load(), len(serving.Requests()); a != 1 || b != 1 || c != 1 {
		t.Fatalf("xorb GETs: missing %d, broken %d, serving %d, want 1 each", a, b, c)
	}
}

func TestPullWithEveryHolderFailingCoolsDown(t *testing.T) {
	tracker := newTracker(t)
	hash, _ := clustertest.MakeXorb(t, 23, 2, 8<<10)
	tracker.Hold(newHolder(t, "missing").Peer(), hash)
	rawHolder(t, tracker, "broken", hash, withStatus(http.StatusInternalServerError))
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	if _, err := st.GetXorbReadSeekCloser(ctx, hash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("read with every holder failing = %v, want fs.ErrNotExist", err)
	}
	if n := len(tracker.Lookups()); n != 1 {
		t.Fatalf("lookups = %d, want 1", n)
	}
	if _, err := st.GetXorbReadSeekCloser(ctx, hash); err == nil {
		t.Fatal("read in the cooldown succeeded")
	}
	if n := len(tracker.Lookups()); n != 1 {
		t.Fatalf("lookups after a read in the cooldown = %d, want still 1", n)
	}
}

func TestPullTriesAtMostThreeHolders(t *testing.T) {
	tracker := newTracker(t)
	hash, _ := clustertest.MakeXorb(t, 24, 2, 8<<10)
	var holders []*clustertest.FakePeer
	for _, id := range []string{"h1", "h2", "h3", "h4"} {
		holders = append(holders, newHolder(t, id))
		tracker.Hold(holders[len(holders)-1].Peer(), hash)
	}
	st := newTestStore(t, t.TempDir(), tracker.URL)
	if _, err := st.GetXorbReadSeekCloser(context.Background(), hash); err == nil {
		t.Fatal("read with no holder serving succeeded")
	}
	var asked []int
	for _, h := range holders {
		asked = append(asked, len(h.Requests()))
	}
	if !slices.Equal(asked, []int{1, 1, 1, 0}) {
		t.Fatalf("xorb GETs per holder = %v, want [1 1 1 0]", asked)
	}
}

func TestConcurrentRemoteReadersShareOneGet(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	hash, body := clustertest.MakeXorb(t, 25, 4, 16<<10)
	release := make(chan struct{})
	holder.GateXorb(hash, body, 4<<10, release)
	tracker.Hold(holder.Peer(), hash)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	var readers []io.ReadSeekCloser
	for range 2 {
		rs, err := st.GetXorbReadSeekCloser(context.Background(), hash)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, rs)
	}
	close(release)
	for i, rs := range readers {
		got, err := io.ReadAll(rs)
		_ = rs.Close()
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("reader %d = %d bytes, %v, want %d", i, len(got), err, len(body))
		}
	}
	if n := len(holder.Requests()); n != 1 {
		t.Fatalf("xorb GETs on the holder = %d, want 1", n)
	}
}

func TestChunkOffsetsComeFromSidecarBeforeAnyPull(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	tf := makeFile(t, 26)
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldFile(holder.Peer(), tf.file)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	if _, err := st.GetFileHashBySHA256(ctx, digestOf(t, tf.file)); err != nil {
		t.Fatal(err)
	}
	h := tf.xorbHashes()[0]
	if got, err := st.GetXorbChunkOffsets(ctx, h); err != nil || !slices.Equal(got, tf.resp.Offsets[h.String()]) {
		t.Fatalf("GetXorbChunkOffsets = %v, %v, want the sidecar's %v", got, err, tf.resp.Offsets[h.String()])
	}
	if reqs := holder.Requests(); len(reqs) != 1 {
		t.Fatalf("holder requests = %+v, want only the shard GET", reqs)
	}
}

func TestChunkOffsetsOfUnknownXorbPullIt(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	hash, body := clustertest.MakeXorb(t, 27, 3, 8<<10)
	holder.ServeXorb(hash, body)
	tracker.Hold(holder.Peer(), hash)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	want, _ := storage.ReadXorbChunkOffsets(bytes.NewReader(body))
	if got, err := st.GetXorbChunkOffsets(context.Background(), hash); err != nil || !slices.Equal(got, want) {
		t.Fatalf("GetXorbChunkOffsets = %v, %v, want %v", got, err, want)
	}
	if !bytes.Equal(localBytes(t, st, hash), body) || len(holder.Requests()) != 1 {
		t.Fatalf("after GetXorbChunkOffsets: %d holder GETs, want 1 and the xorb local", len(holder.Requests()))
	}
}

func TestPullFileMakesFileComplete(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	tf := makeFile(t, 28, 29)
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldFile(holder.Peer(), tf.file)
	for hash, body := range tf.xorbs {
		holder.ServeXorb(hash, body)
		tracker.Hold(holder.Peer(), hash)
	}
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ev := watch(st)
	ctx := context.Background()
	if err := st.pullFile(ctx, tf.file.Hash); err != nil {
		t.Fatal(err)
	}
	for hash, body := range tf.xorbs {
		if !bytes.Equal(localBytes(t, st, hash), body) {
			t.Fatalf("xorb %s not stored as the holder serves it", hash)
		}
	}
	if files := ev.knownFiles(); len(files) != 1 || files[0].file != tf.file || !slices.Equal(sortedXorbs(files[0].xorbs), tf.xorbHashes()) {
		t.Fatalf("onFile calls = %+v, want one for %+v over %v", files, tf.file, tf.xorbHashes())
	}
	n := len(tracker.Lookups())
	if _, err := st.GetShard(ctx, tf.hash); err != nil {
		t.Fatalf("GetShard of the pulled file = %v", err)
	}
	if m := len(tracker.Lookups()); m != n {
		t.Fatalf("GetShard of the pulled file looked up %d times, want none", m-n)
	}
}

func TestGetShardBySHA256NeedsEveryXorbHeld(t *testing.T) {
	tracker, holder := newTracker(t), newHolder(t, "holder")
	tf := makeFile(t, 30)
	holder.ServeShard(tf.file, tf.resp)
	tracker.HoldFile(holder.Peer(), tf.file)
	st := newTestStore(t, t.TempDir(), tracker.URL)
	ctx := context.Background()
	if _, err := st.GetShardBySHA256(ctx, digestOf(t, tf.file)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShardBySHA256 with no holder of its xorbs = %v, want fs.ErrNotExist", err)
	}
	tracker.Hold(holder.Peer(), tf.xorbHashes()...)
	if sh, err := st.GetShardBySHA256(ctx, digestOf(t, tf.file)); err != nil || sh.File(tf.hash) == nil {
		t.Fatalf("GetShardBySHA256 with its xorbs held = %v, want the file's shard", err)
	}
}

func TestChunkDedupAlwaysMisses(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	tf := makeFile(t, 31)
	ingest(t, st, tf)
	ctx := context.Background()
	chunk := tf.shard(t).GlobalDedupEligibleChunks()[0].Hash
	if _, err := st.Storage.GetShardByChunkHash(ctx, chunk); err != nil {
		t.Fatalf("local chunk index lacks the file's first chunk: %v", err)
	}
	if _, err := st.GetShardByChunkHash(ctx, chunk); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShardByChunkHash = %v, want fs.ErrNotExist", err)
	}
}

func TestDeleteXorbRefusesWhileReaderOpen(t *testing.T) {
	st := newTestStore(t, t.TempDir(), "http://tracker.test")
	ctx := context.Background()
	hash, body := clustertest.MakeXorb(t, 8, 2, 8<<10)
	if _, err := st.PutXorb(ctx, hash, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	rs, err := st.GetXorbReadSeekCloser(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteXorb(ctx, hash); !errors.Is(err, errPinned) {
		t.Fatalf("DeleteXorb with a reader open = %v, want errPinned", err)
	}
	_ = rs.Close()
	if err := st.DeleteXorb(ctx, hash); err != nil {
		t.Fatalf("DeleteXorb after Close = %v", err)
	}
	if ok, _ := st.Storage.HasXorb(ctx, hash); ok {
		t.Fatal("deleted xorb still local")
	}
	if _, err := os.Stat(sidecarPath(st, hash)); err != nil {
		t.Fatalf("sidecar after DeleteXorb: %v, want kept", err)
	}
}
