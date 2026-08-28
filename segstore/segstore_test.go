package segstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"

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
	fixtureOnce sync.Once
	fixture     xorbFixture
	fixtureErr  error
)

// buildXorbFixture uploads a few MiB of seeded pseudo-random data through
// the real chunker and encoder, yielding one multi-chunk xorb.
func buildXorbFixture(t *testing.T) xorbFixture {
	t.Helper()
	fixtureOnce.Do(func() {
		src := make([]byte, 2<<20)
		_, _ = mrand.New(mrand.NewSource(7)).Read(src)
		cas := &memCAS{xorbs: map[string][]byte{}}
		if _, err := upload.UploadFile(context.Background(), cas, bytes.NewReader(src)); err != nil {
			fixtureErr = fmt.Errorf("upload fixture file: %w", err)
			return
		}
		if len(cas.xorbs) != 1 {
			fixtureErr = fmt.Errorf("got %d fixture xorbs, want 1", len(cas.xorbs))
			return
		}
		for hash, data := range cas.xorbs {
			fixture.hash, fixture.data = hash, data
		}
		offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(fixture.data))
		if err != nil {
			fixtureErr = fmt.Errorf("scan fixture xorb: %w", err)
			return
		}
		if len(offsets) < 8 {
			fixtureErr = fmt.Errorf("fixture xorb has %d chunks, want at least 8", len(offsets))
			return
		}
		fixture.offsets = offsets
	})
	if fixtureErr != nil {
		t.Fatal(fixtureErr)
	}
	return fixture
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

func newStore(t *testing.T) (*segstore.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func putSlice(t *testing.T, s *segstore.Store, fx xorbFixture, chunkStart, chunkEnd uint32) segstore.Segment {
	t.Helper()
	byteStart, byteEnd, data := fx.slice(t, chunkStart, chunkEnd)
	if err := s.Put(fx.hash, chunkStart, chunkEnd, byteStart, byteEnd, data); err != nil {
		t.Fatalf("Put(%d, %d) = %v, want nil", chunkStart, chunkEnd, err)
	}
	return segstore.Segment{
		XorbHash:   fx.hash,
		ChunkStart: chunkStart,
		ChunkEnd:   chunkEnd,
		ByteStart:  byteStart,
		ByteEnd:    byteEnd,
		Size:       int64(len(data)),
	}
}

// encodeChunks packs raw chunks with the real xorb encoder (chunk-only
// format), standing in for a source that encoded the same xorb differently.
func encodeChunks(t *testing.T, chunks [][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, false)
	for _, c := range chunks {
		if _, err := enc.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestNewRequiresDir(t *testing.T) {
	if _, err := segstore.New(); err == nil || !strings.Contains(err.Error(), "dir is required") {
		t.Fatalf("New() error = %v, want dir is required", err)
	}
}

func TestPutFind(t *testing.T) {
	fx := buildXorbFixture(t)
	s, _ := newStore(t)
	wide := putSlice(t, s, fx, 0, 6)
	narrow := putSlice(t, s, fx, 1, 4)

	tests := []struct {
		name                 string
		hash                 string
		chunkStart, chunkEnd uint32
		want                 segstore.Segment
		wantOK               bool
	}{
		{"exact wide", fx.hash, 0, 6, wide, true},
		{"exact narrow beats containing wide", fx.hash, 1, 4, narrow, true},
		{"contained picks smallest", fx.hash, 2, 3, narrow, true},
		{"contained only by wide", fx.hash, 0, 2, wide, true},
		{"not covered", fx.hash, 4, 7, segstore.Segment{}, false},
		{"unknown hash", strings.Repeat("f", 64), 1, 4, segstore.Segment{}, false},
		{"inverted range", fx.hash, 3, 3, segstore.Segment{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := s.Find(tt.hash, tt.chunkStart, tt.chunkEnd)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("Find(%d, %d) = %+v, %v, want %+v, %v", tt.chunkStart, tt.chunkEnd, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestFindByteRange(t *testing.T) {
	fx := buildXorbFixture(t)
	s, _ := newStore(t)
	wide := putSlice(t, s, fx, 0, 6)
	narrow := putSlice(t, s, fx, 1, 4)

	tests := []struct {
		name               string
		byteStart, byteEnd int64
		want               segstore.Segment
		wantOK             bool
	}{
		{"exact narrow", narrow.ByteStart, narrow.ByteEnd, narrow, true},
		{"interior picks smallest", narrow.ByteStart + 1, narrow.ByteEnd - 1, narrow, true},
		{"only wide covers", 0, narrow.ByteStart, wide, true},
		{"beyond stored", wide.ByteEnd + 1, wide.ByteEnd + 10, segstore.Segment{}, false},
		{"negative start", -1, 10, segstore.Segment{}, false},
		{"inverted range", 10, 5, segstore.Segment{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := s.FindByteRange(fx.hash, tt.byteStart, tt.byteEnd)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("FindByteRange(%d, %d) = %+v, %v, want %+v, %v", tt.byteStart, tt.byteEnd, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

// TestFindPrefersNarrowestChunkRange pins the containment metric: segments
// captured from different encodings can order differently by byte size than
// by chunk count, and chunk-keyed lookups must pick the narrowest chunk
// range.
func TestFindPrefersNarrowestChunkRange(t *testing.T) {
	s, _ := newStore(t)
	hash := strings.Repeat("cd", 32)
	zero := make([]byte, 8192)
	wideData := encodeChunks(t, [][]byte{zero, zero, zero, zero, zero, zero})
	rnd1 := make([]byte, 65536)
	_, _ = mrand.New(mrand.NewSource(11)).Read(rnd1)
	rnd2 := make([]byte, 65536)
	_, _ = mrand.New(mrand.NewSource(12)).Read(rnd2)
	narrowData := encodeChunks(t, [][]byte{rnd1, rnd2})
	if len(wideData) >= len(narrowData) {
		t.Fatalf("fixture broken: wide is %d bytes, want smaller than narrow %d", len(wideData), len(narrowData))
	}
	if err := s.Put(hash, 0, 6, 0, int64(len(wideData))-1, wideData); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(hash, 1, 3, 0, int64(len(narrowData))-1, narrowData); err != nil {
		t.Fatal(err)
	}

	got, ok := s.Find(hash, 1, 2)
	if !ok || got.ChunkStart != 1 || got.ChunkEnd != 3 {
		t.Fatalf("Find(1, 2) = %+v, %v, want the [1, 3) segment despite its larger byte size", got, ok)
	}
}

func TestPutValidation(t *testing.T) {
	fx := buildXorbFixture(t)
	byteStart, byteEnd, data := fx.slice(t, 1, 4)
	trailing := append(append([]byte{}, data...), make([]byte, 16)...)

	tests := []struct {
		name                 string
		hash                 string
		chunkStart, chunkEnd uint32
		byteStart, byteEnd   int64
		data                 []byte
	}{
		{"bad hash", "../../etc/passwd", 1, 4, byteStart, byteEnd, data},
		{"uppercase hash", strings.ToUpper(fx.hash), 1, 4, byteStart, byteEnd, data},
		{"inverted chunk range", fx.hash, 4, 4, byteStart, byteEnd, data},
		{"negative byte start", fx.hash, 1, 4, -1, int64(len(data)) - 2, data},
		{"byte length mismatch", fx.hash, 1, 4, byteStart, byteEnd + 1, data},
		{"truncated data", fx.hash, 1, 4, byteStart, byteEnd - 1, data[:len(data)-1]},
		{"wrong chunk count", fx.hash, 1, 3, byteStart, byteEnd, data},
		{"trailing garbage", fx.hash, 1, 4, byteStart, byteStart + int64(len(trailing)) - 1, trailing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dir := newStore(t)
			if err := s.Put(tt.hash, tt.chunkStart, tt.chunkEnd, tt.byteStart, tt.byteEnd, tt.data); err == nil {
				t.Fatal("Put() error = nil, want error")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("store dir has %d entries after rejected put, want 0", len(entries))
			}
		})
	}
}

func TestPutIdempotent(t *testing.T) {
	fx := buildXorbFixture(t)
	s, dir := newStore(t)
	byteStart, byteEnd, data := fx.slice(t, 1, 4)
	if err := s.Put(fx.hash, 1, 4, byteStart, byteEnd, data); err != nil {
		t.Fatalf("first Put() = %v, want nil", err)
	}
	before, err := os.Stat(filepath.Join(dir, fx.hash, "1-4"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(fx.hash, 1, 4, byteStart, byteEnd, data); err != nil {
		t.Fatalf("second Put() = %v, want nil", err)
	}
	after, err := os.Stat(filepath.Join(dir, fx.hash, "1-4"))
	if err != nil {
		t.Fatal(err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("identical re-put rewrote the data file, want no-op")
	}
	stored, err := os.ReadFile(filepath.Join(dir, fx.hash, "1-4"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, data) {
		t.Fatal("stored data differs from put data after re-put")
	}
	count := 0
	if err := s.Walk(func(segstore.Segment) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Walk() visited %d segments, want 1", count)
	}
}

// TestPutHealsDamagedData pins that the idempotent no-op path checks the
// stored bytes, not just the metadata: a re-put over a damaged data file
// must rewrite it.
func TestPutHealsDamagedData(t *testing.T) {
	fx := buildXorbFixture(t)
	byteStart, byteEnd, data := fx.slice(t, 1, 4)
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
			if err := os.WriteFile(dataPath, data[:10], 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"equal-size corrupted data file", func(t *testing.T, dataPath string) {
			if err := os.WriteFile(dataPath, make([]byte, len(data)), 0644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dir := newStore(t)
			if err := s.Put(fx.hash, 1, 4, byteStart, byteEnd, data); err != nil {
				t.Fatal(err)
			}
			dataPath := filepath.Join(dir, fx.hash, "1-4")
			tt.damage(t, dataPath)
			if err := s.Put(fx.hash, 1, 4, byteStart, byteEnd, data); err != nil {
				t.Fatalf("re-put over damaged data = %v, want nil", err)
			}
			got, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatal("re-put did not restore the data file")
			}
		})
	}
}

func TestConcurrentPut(t *testing.T) {
	fx := buildXorbFixture(t)
	s, dir := newStore(t)
	byteStart, byteEnd, data := fx.slice(t, 1, 4)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Put(fx.hash, 1, 4, byteStart, byteEnd, data)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Put() = %v, want nil", err)
		}
	}
	got, err := os.ReadFile(filepath.Join(dir, fx.hash, "1-4"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data file differs from put data after concurrent puts")
	}
	entries, err := os.ReadDir(filepath.Join(dir, fx.hash))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("segment dir has %d entries after concurrent puts, want data + meta", len(entries))
	}
}

// randChunks builds n seeded pseudo-random raw chunks of 64 KiB.
func randChunks(seed int64, n int) [][]byte {
	rng := mrand.New(mrand.NewSource(seed))
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = make([]byte, 65536)
		_, _ = rng.Read(chunks[i])
	}
	return chunks
}

// TestPutKeepsFirstEncoding pins key immutability: a different encoding of
// an already committed chunk range — equal-sized, the case no size check can
// catch — must not replace the stored data.
func TestPutKeepsFirstEncoding(t *testing.T) {
	s, dir := newStore(t)
	hash := strings.Repeat("ef", 32)
	first := encodeChunks(t, randChunks(21, 2))
	second := encodeChunks(t, randChunks(22, 2))
	if len(first) != len(second) {
		t.Fatalf("fixture broken: encodings are %d and %d bytes, want equal", len(first), len(second))
	}
	if bytes.Equal(first, second) {
		t.Fatal("fixture broken: encodings are identical")
	}
	if err := s.Put(hash, 0, 2, 0, int64(len(first))-1, first); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(hash, 0, 2, 0, int64(len(second))-1, second); err != nil {
		t.Fatalf("re-put of a different encoding = %v, want keep-first nil", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, hash, "0-2"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, first) {
		t.Fatal("a different encoding replaced the committed segment data")
	}
}

// TestPutQuarantinedReportsOwnership pins the serve-gating contract of the
// quarantine put: true means the store now holds exactly the given bytes
// under the key — created, healed, or already stored identically — while a
// key owned by a different segment (foreign meta, foreign bytes behind
// identical meta, or a released segment) reports false and keeps the first
// encoding.
func TestPutQuarantinedReportsOwnership(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	owner := strings.Repeat("0f", 32)
	const peer = "http://peer.example"
	first := encodeChunks(t, randChunks(31, 2))
	foreign := encodeChunks(t, randChunks(32, 2))
	if len(first) != len(foreign) || bytes.Equal(first, foreign) {
		t.Fatal("fixture broken: want equal-length distinct encodings")
	}
	end := int64(len(first)) - 1

	s, dir := newStore(t)
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, first, owner, peer); err != nil || !got {
		t.Fatalf("got fresh PutQuarantined = (%v, %v), want (true, nil)", got, err)
	}
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, first, owner, peer); err != nil || !got {
		t.Fatalf("got identical re-put = (%v, %v), want (true, nil)", got, err)
	}
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, foreign, owner, peer); err != nil || got {
		t.Fatalf("got foreign bytes behind identical meta = (%v, %v), want (false, nil)", got, err)
	}
	if got, err := s.PutQuarantined(hash, 0, 2, 1, end+1, first, owner, peer); err != nil || got {
		t.Fatalf("got foreign byte-range re-put = (%v, %v), want (false, nil)", got, err)
	}
	dataPath := filepath.Join(dir, hash, "0-2")
	stored, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, first) {
		t.Fatal("a rejected put replaced the committed segment data")
	}
	// Damaged data behind identical meta is healed and reported owned.
	if err := os.WriteFile(dataPath, first[:10], 0644); err != nil {
		t.Fatal(err)
	}
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, first, owner, peer); err != nil || !got {
		t.Fatalf("got healing re-put = (%v, %v), want (true, nil)", got, err)
	}
	// A released segment owns its key: a quarantined re-put is a conflict.
	if released, err := s.Release(hash, 0, 2); err != nil || !released {
		t.Fatalf("got Release = (%v, %v), want (true, nil)", released, err)
	}
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, first, owner, peer); err != nil || got {
		t.Fatalf("got re-put over a released segment = (%v, %v), want (false, nil)", got, err)
	}
}

// readMetaFile loads one segment meta JSON straight from disk.
func readMetaFile(t *testing.T, dir, hash, key string) segstore.Segment {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, hash, key+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var seg segstore.Segment
	if err := json.Unmarshal(data, &seg); err != nil {
		t.Fatal(err)
	}
	return seg
}

// TestQuarantineOwnershipPersisted pins the durable ownership lifecycle: a
// quarantined put persists the file hash, peer and quarantine time; an
// identical-bytes re-store refreshes them to the new flight; Release clears
// them — ownership describes quarantine only.
func TestQuarantineOwnershipPersisted(t *testing.T) {
	hash := strings.Repeat("cd", 32)
	ownerA, ownerB := strings.Repeat("aa", 32), strings.Repeat("bb", 32)
	const peerA, peerB = "http://peer-a.example", "http://peer-b.example"
	data := encodeChunks(t, randChunks(41, 2))
	end := int64(len(data)) - 1

	s, dir := newStore(t)
	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, data, ownerA, peerA); err != nil || !got {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", got, err)
	}
	seg := readMetaFile(t, dir, hash, "0-2")
	if seg.FileHash != ownerA || seg.Peer != peerA || seg.QuarantinedAt <= 0 {
		t.Fatalf("got persisted ownership (%q, %q, %d), want (%q, %q, >0)", seg.FileHash, seg.Peer, seg.QuarantinedAt, ownerA, peerA)
	}

	if got, err := s.PutQuarantined(hash, 0, 2, 0, end, data, ownerB, peerB); err != nil || !got {
		t.Fatalf("got identical re-store = (%v, %v), want (true, nil)", got, err)
	}
	refreshed := readMetaFile(t, dir, hash, "0-2")
	if refreshed.FileHash != ownerB || refreshed.Peer != peerB || refreshed.QuarantinedAt < seg.QuarantinedAt {
		t.Fatalf("got ownership after re-store (%q, %q, %d), want refreshed to (%q, %q, >=%d)",
			refreshed.FileHash, refreshed.Peer, refreshed.QuarantinedAt, ownerB, peerB, seg.QuarantinedAt)
	}

	if released, err := s.Release(hash, 0, 2); err != nil || !released {
		t.Fatalf("got Release = (%v, %v), want (true, nil)", released, err)
	}
	cleared := readMetaFile(t, dir, hash, "0-2")
	if cleared.Quarantined || cleared.FileHash != "" || cleared.Peer != "" || cleared.QuarantinedAt != 0 {
		t.Fatalf("got released meta %+v, want quarantine ownership cleared", cleared)
	}
}

// TestMetaWithoutOwnershipFieldsLoads pins backward compatibility: metas
// written before the ownership fields load with zero values — a verified
// one is found and served exactly as today, a quarantined one stays
// invisible and reads back with an unknown (zero) quarantine time.
func TestMetaWithoutOwnershipFieldsLoads(t *testing.T) {
	s, dir := newStore(t)
	data := encodeChunks(t, randChunks(51, 2))
	okHash := strings.Repeat("aa", 32)
	quarHash := strings.Repeat("bb", 32)
	for hash, quarantined := range map[string]bool{okHash: false, quarHash: true} {
		if err := os.MkdirAll(filepath.Join(dir, hash), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, hash, "0-2"), data, 0644); err != nil {
			t.Fatal(err)
		}
		// The pre-upgrade meta shape: no ownership keys at all.
		meta := fmt.Sprintf(`{"xorb_hash":%q,"chunk_start":0,"chunk_end":2,"byte_start":0,"byte_end":%d,"size":%d`,
			hash, len(data)-1, len(data))
		if quarantined {
			meta += `,"quarantined":true`
		}
		meta += `}`
		if err := os.WriteFile(filepath.Join(dir, hash, "0-2.json"), []byte(meta), 0644); err != nil {
			t.Fatal(err)
		}
	}

	seg, ok := s.Find(okHash, 0, 2)
	if !ok || seg.Quarantined || seg.FileHash != "" || seg.Peer != "" || seg.QuarantinedAt != 0 {
		t.Fatalf("got Find(old verified meta) = (%+v, %v), want it served with zero ownership", seg, ok)
	}
	if got, err := s.Open(seg); err != nil {
		t.Fatalf("got Open(old verified meta) error = %v, want nil", err)
	} else {
		_ = got.Close()
	}
	if seg, ok := s.Find(quarHash, 0, 2); ok {
		t.Fatalf("got Find(old quarantined meta) = %+v, want a miss", seg)
	}
	var walked []segstore.Segment
	if err := s.Walk(func(seg segstore.Segment) error {
		if seg.XorbHash == quarHash {
			walked = append(walked, seg)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(walked) != 1 || !walked[0].Quarantined || walked[0].QuarantinedAt != 0 {
		t.Fatalf("got walked old quarantined meta %+v, want quarantined with unknown time", walked)
	}
}

// TestDeleteQuarantinedBefore pins the conditional purge: only a segment
// still quarantined no later than the cutoff — re-read under the store lock
// — is removed, so a stale sweep decision can never delete a segment a
// concurrent flight re-owned or a verdict released. A missing timestamp
// counts as before any cutoff.
func TestDeleteQuarantinedBefore(t *testing.T) {
	s, dir := newStore(t)
	data := encodeChunks(t, randChunks(61, 2))
	end := int64(len(data)) - 1
	owner := strings.Repeat("0a", 32)
	const peer = "http://peer.example"

	fresh := strings.Repeat("1b", 32)
	if got, err := s.PutQuarantined(fresh, 0, 2, 0, end, data, owner, peer); err != nil || !got {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", got, err)
	}
	if removed, err := s.DeleteQuarantinedBefore(fresh, 0, 2, time.Now().Add(-time.Hour)); err != nil || removed {
		t.Fatalf("got DeleteQuarantinedBefore(fresh meta, old cutoff) = (%v, %v), want (false, nil)", removed, err)
	}
	if removed, err := s.DeleteQuarantinedBefore(fresh, 0, 2, time.Now().Add(time.Hour)); err != nil || !removed {
		t.Fatalf("got DeleteQuarantinedBefore(fresh meta, future cutoff) = (%v, %v), want (true, nil)", removed, err)
	}
	if removed, err := s.DeleteQuarantinedBefore(fresh, 0, 2, time.Now().Add(time.Hour)); err != nil || removed {
		t.Fatalf("got DeleteQuarantinedBefore(missing segment) = (%v, %v), want (false, nil)", removed, err)
	}

	released := strings.Repeat("2c", 32)
	if got, err := s.PutQuarantined(released, 0, 2, 0, end, data, owner, peer); err != nil || !got {
		t.Fatalf("got PutQuarantined = (%v, %v), want (true, nil)", got, err)
	}
	if ok, err := s.Release(released, 0, 2); err != nil || !ok {
		t.Fatalf("got Release = (%v, %v), want (true, nil)", ok, err)
	}
	if removed, err := s.DeleteQuarantinedBefore(released, 0, 2, time.Now().Add(time.Hour)); err != nil || removed {
		t.Fatalf("got DeleteQuarantinedBefore(released segment) = (%v, %v), want (false, nil)", removed, err)
	}
	if _, ok := s.Find(released, 0, 2); !ok {
		t.Fatal("got released segment gone after refused conditional delete, want it kept")
	}

	// A quarantined meta without a timestamp (pre-upgrade) is unsettleable:
	// it is before any cutoff.
	stale := strings.Repeat("3d", 32)
	if err := os.MkdirAll(filepath.Join(dir, stale), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stale, "0-2"), data, 0644); err != nil {
		t.Fatal(err)
	}
	meta, err := json.Marshal(segstore.Segment{XorbHash: stale, ChunkStart: 0, ChunkEnd: 2, ByteStart: 0, ByteEnd: end, Size: end + 1, Quarantined: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stale, "0-2.json"), meta, 0644); err != nil {
		t.Fatal(err)
	}
	if removed, err := s.DeleteQuarantinedBefore(stale, 0, 2, time.Now().Add(-time.Hour)); err != nil || !removed {
		t.Fatalf("got DeleteQuarantinedBefore(timestampless meta) = (%v, %v), want (true, nil)", removed, err)
	}
}

// TestPutCommitsOverCrashLeftoverData pins the commit point: data without
// meta (a crash between the two renames) is invisible, and the next put of
// the key commits cleanly over it.
func TestPutCommitsOverCrashLeftoverData(t *testing.T) {
	s, dir := newStore(t)
	hash := strings.Repeat("ba", 32)
	orphan := encodeChunks(t, randChunks(31, 2))
	if err := os.MkdirAll(filepath.Join(dir, hash), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hash, "0-2"), orphan, 0644); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Find(hash, 0, 2); ok {
		t.Fatal("orphan data without meta must be invisible")
	}
	fresh := encodeChunks(t, randChunks(32, 2))
	if err := s.Put(hash, 0, 2, 0, int64(len(fresh))-1, fresh); err != nil {
		t.Fatalf("put over crash leftover = %v, want nil", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, hash, "0-2"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatal("put over crash leftover did not commit the new data")
	}
	if seg, ok := s.Find(hash, 0, 2); !ok || seg.Size != int64(len(fresh)) {
		t.Fatalf("Find() after commit = %+v, %v, want size %d", seg, ok, len(fresh))
	}
}

func TestRestartRecovery(t *testing.T) {
	fx := buildXorbFixture(t)
	s1, dir := newStore(t)
	want := putSlice(t, s1, fx, 2, 5)
	_, _, wantData := fx.slice(t, 2, 5)

	s2, err := segstore.New(segstore.WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Find(fx.hash, 2, 5)
	if !ok || got != want {
		t.Fatalf("Find() after restart = %+v, %v, want %+v, true", got, ok, want)
	}
	f, err := s2.Open(got)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	gotData, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotData, wantData) {
		t.Fatal("segment data after restart differs from origin xorb slice")
	}
}

func TestChunkRange(t *testing.T) {
	fx := buildXorbFixture(t)
	s, _ := newStore(t)
	seg := putSlice(t, s, fx, 1, 5)
	_, _, segData := fx.slice(t, 1, 5)
	f, err := s.Open(seg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	absStart, absEnd, want := fx.slice(t, 2, 4)
	relStart, relEnd, err := seg.ChunkRange(f, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if relStart != absStart-seg.ByteStart || relEnd != absEnd-seg.ByteStart {
		t.Fatalf("ChunkRange(2, 4) = [%d, %d], want [%d, %d]", relStart, relEnd, absStart-seg.ByteStart, absEnd-seg.ByteStart)
	}
	if got := segData[relStart : relEnd+1]; !bytes.Equal(got, want) {
		t.Fatal("sub-range bytes differ from origin xorb slice")
	}

	fullStart, fullEnd, err := seg.ChunkRange(f, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if fullStart != 0 || fullEnd != seg.Size-1 {
		t.Fatalf("ChunkRange(full segment) = [%d, %d], want [0, %d]", fullStart, fullEnd, seg.Size-1)
	}

	for _, tt := range []struct {
		name                 string
		chunkStart, chunkEnd uint32
	}{
		{"before segment", 0, 2},
		{"past segment", 4, 6},
		{"inverted", 3, 3},
	} {
		if _, _, err := seg.ChunkRange(f, tt.chunkStart, tt.chunkEnd); err == nil {
			t.Fatalf("%s: ChunkRange(%d, %d) error = nil, want error", tt.name, tt.chunkStart, tt.chunkEnd)
		}
	}
}

func TestWalk(t *testing.T) {
	fx := buildXorbFixture(t)
	s, _ := newStore(t)
	if err := s.Walk(func(segstore.Segment) error { return fmt.Errorf("unexpected visit") }); err != nil {
		t.Fatalf("Walk() on empty store = %v, want nil", err)
	}

	segA := putSlice(t, s, fx, 0, 2)
	// Structural validation is content-hash agnostic, so the same bytes can
	// register under another xorb id.
	otherHash := strings.Repeat("ab", 32)
	byteStart, byteEnd, data := fx.slice(t, 0, 2)
	if err := s.Put(otherHash, 0, 2, byteStart, byteEnd, data); err != nil {
		t.Fatal(err)
	}

	got := map[string]segstore.Segment{}
	if err := s.Walk(func(seg segstore.Segment) error {
		got[seg.XorbHash] = seg
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Walk() visited %d xorbs, want 2", len(got))
	}
	if got[fx.hash] != segA {
		t.Fatalf("Walk() saw %+v, want %+v", got[fx.hash], segA)
	}

	wantErr := fmt.Errorf("stop")
	calls := 0
	if err := s.Walk(func(segstore.Segment) error { calls++; return wantErr }); err != wantErr {
		t.Fatalf("Walk() error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("Walk() kept going after error: %d calls, want 1", calls)
	}
}
