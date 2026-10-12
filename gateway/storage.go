package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

// relayAttempts bounds the holders tried per remote open.
const relayAttempts = 3

// store keeps shards and indexes in the local store while xorb bytes may live only on peers; reads fall back to live holders.
type store struct {
	*local.Storage
	reg       *registry
	offsetDir string
	transport *http.Transport // relay fetches; carries no upstream credentials

	mu      sync.Mutex // guards pins, writers and local xorb deletion; never held across network I/O
	pins    map[xet.XorbHash]int
	writers map[xet.XorbHash]*writerLock

	// staging hooks, set once before concurrent use; nil without a staging manager
	onStored func(hash xet.XorbHash, size int64, inserted bool) // bytes and offsets durable, still pinned
	onRead   func(hash xet.XorbHash)                            // a local copy was opened
	onIdle   func()                                             // some xorb lost its last pin
}

// writerLock serializes the writers of one xorb's local bytes and sidecar.
type writerLock struct {
	sync.Mutex
	users int // guarded by store.mu
}

func newStore(dir string, reg *registry) (*store, error) {
	st, err := local.NewStorage(local.WithBasePath(dir))
	if err != nil {
		return nil, err
	}
	offsetDir := filepath.Join(dir, "offsets")
	if err := os.MkdirAll(offsetDir, 0o755); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &store{Storage: st, reg: reg, offsetDir: offsetDir, transport: transport, pins: map[xet.XorbHash]int{}, writers: map[xet.XorbHash]*writerLock{}}, nil
}

// lockWriter blocks until this caller is the only writer of hash; the result releases it.
func (s *store) lockWriter(hash xet.XorbHash) func() {
	s.mu.Lock()
	w := s.writers[hash]
	if w == nil {
		w = &writerLock{}
		s.writers[hash] = w
	}
	w.users++
	s.mu.Unlock()
	w.Lock()
	return func() {
		w.Unlock()
		s.mu.Lock()
		if w.users--; w.users == 0 {
			delete(s.writers, hash)
		}
		s.mu.Unlock()
	}
}

func xorbURL(origin string, hash xet.XorbHash) string {
	return origin + apis.XorbPathPrefix + hash.String()
}

func (s *store) GetXorbURL(ctx context.Context, hash xet.XorbHash) (string, error) {
	if p, ok := s.reg.pick(hash.String()); ok {
		return xorbURL(p.URL, hash), nil
	}
	return s.Storage.GetXorbURL(ctx, hash)
}

// HasXorb reports a local copy or a live holder; local errors are never masked by holders.
func (s *store) HasXorb(ctx context.Context, hash xet.XorbHash) (bool, error) {
	ok, err := s.Storage.HasXorb(ctx, hash)
	if err != nil || ok {
		return ok, err
	}
	return len(s.reg.holders(hash.String())) > 0, nil
}

// xorbsOf lists the distinct xorbs referenced by the file entries and CAS blocks.
func xorbsOf(files []shard.FileBlock, cas []shard.CASBlock) []xet.XorbHash {
	seen := map[xet.XorbHash]bool{}
	var out []xet.XorbHash
	add := func(h xet.XorbHash) {
		if !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	for _, f := range files {
		for _, e := range f.Entries {
			add(e.CASHash)
		}
	}
	for _, c := range cas {
		add(c.CASHash)
	}
	return out
}

// GetShard fails with fs.ErrNotExist when any xorb of the requested file has no copy left, so the mirror re-ingests it.
func (s *store) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	sh, err := s.Storage.GetShard(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(sh.Files, func(f shard.FileBlock) bool { return f.FileHash == fileHash })
	if i < 0 {
		return nil, fmt.Errorf("shard of file %s lacks its file block", fileHash)
	}
	for _, h := range xorbsOf(sh.Files[i:i+1], nil) {
		ok, err := s.HasXorb(ctx, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("xorb %s of file %s has no copy: %w", h, fileHash, fs.ErrNotExist)
		}
	}
	return sh, nil
}

func (s *store) GetFileHashBySHA256(ctx context.Context, digest [32]byte) (xet.FileHash, error) {
	fh, err := s.Storage.GetFileHashBySHA256(ctx, digest)
	if err == nil {
		_, err = s.GetShard(ctx, fh)
	}
	if err != nil {
		return xet.FileHash{}, err
	}
	return fh, nil
}

// maxSidecarBytes bounds a JSON array of xet.MaxChunksPerXorb 20-digit offsets.
const maxSidecarBytes = (xet.MaxChunksPerXorb + 1) * 21

// maxPackedBytes is the largest packed xorb the peers accept, so no larger offset can be served.
const maxPackedBytes = 128 << 20

func (s *store) sidecar(hash xet.XorbHash) string {
	return filepath.Join(s.offsetDir, filepath.FromSlash(storage.ObjectKey("", hash.String())))
}

// validOffsets accepts what a footer read would: a bounded count, each end within the packed cap and at least a header past the previous.
func validOffsets(offsets []uint64) error {
	if len(offsets) > xet.MaxChunksPerXorb {
		return fmt.Errorf("%d chunk offsets exceed %d", len(offsets), xet.MaxChunksPerXorb)
	}
	prev := uint64(0)
	for _, off := range offsets {
		if off > maxPackedBytes {
			return fmt.Errorf("chunk offset %d exceeds %d", off, maxPackedBytes)
		}
		if off < prev || off-prev < 8 {
			return fmt.Errorf("chunk offset %d does not follow %d", off, prev)
		}
		prev = off
	}
	return nil
}

// readSidecar accepts exactly one bounded JSON array of valid offsets.
func (s *store) readSidecar(hash xet.XorbHash) ([]uint64, error) {
	f, err := os.Open(s.sidecar(hash))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxSidecarBytes+1))
	if err == nil && len(data) > maxSidecarBytes {
		err = fmt.Errorf("exceeds %d bytes", maxSidecarBytes)
	}
	var offsets []uint64
	if err == nil {
		err = json.Unmarshal(data, &offsets)
	}
	if err == nil {
		err = validOffsets(offsets)
	}
	if err != nil {
		return nil, fmt.Errorf("offsets sidecar of xorb %s: %w", hash, err)
	}
	return offsets, nil
}

func (s *store) writeSidecar(hash xet.XorbHash, offsets []uint64) error {
	path := s.sidecar(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".offsets-*")
	if err != nil {
		return err
	}
	data, _ := json.Marshal(offsets)
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

// saveOffsets records the offsets of the committed local bytes and reports their size; the hash does not bind their packing.
func (s *store) saveOffsets(ctx context.Context, hash xet.XorbHash) (int64, error) {
	f, err := s.Storage.GetXorbReadSeekCloser(ctx, hash)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	offsets, err := storage.ReadXorbChunkOffsets(f)
	if err != nil {
		return 0, fmt.Errorf("read chunk offsets of xorb %s: %w", hash, err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	return size, s.writeSidecar(hash, offsets)
}

// PutXorb serializes writers of one hash because the local store lets a second one rename over the first.
func (s *store) PutXorb(ctx context.Context, hash xet.XorbHash, r io.Reader) (bool, error) {
	s.pin(hash)
	defer s.unpin(hash)
	defer s.lockWriter(hash)()
	inserted, err := s.Storage.PutXorb(ctx, hash, r)
	if err != nil {
		return false, err
	}
	size, err := s.saveOffsets(ctx, hash)
	if err != nil {
		return false, err
	}
	if s.onStored != nil {
		s.onStored(hash, size, inserted)
	}
	return inserted, nil
}

// localOffsets answers from the local bytes, then the sidecar; a wrapped fs.ErrNotExist means neither exists.
func (s *store) localOffsets(ctx context.Context, hash xet.XorbHash) ([]uint64, error) {
	offsets, err := s.Storage.GetXorbChunkOffsets(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return offsets, err
	}
	return s.readSidecar(hash)
}

// GetXorbChunkOffsets tries local bytes, sidecar, then a holder, persisting what it learns unless local bytes were committed meanwhile.
func (s *store) GetXorbChunkOffsets(ctx context.Context, hash xet.XorbHash) ([]uint64, error) {
	offsets, err := s.localOffsets(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return offsets, err
	}
	sk, err := s.remote(ctx, hash, 0, -1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sk.Close() }()
	if offsets, err = storage.ReadXorbChunkOffsets(sk); err != nil {
		return nil, fmt.Errorf("read chunk offsets of xorb %s: %w", hash, err)
	}
	defer s.lockWriter(hash)()
	if local, err := s.localOffsets(ctx, hash); err == nil {
		return local, nil
	}
	return offsets, s.writeSidecar(hash, offsets)
}

// remote opens [start, end] of the xorb on a live holder, end -1 meaning EOF; failed holders are skipped before any handle is handed out.
func (s *store) remote(ctx context.Context, hash xet.XorbHash, start, end int64) (*httpseek.Seeker, error) {
	holders := s.reg.holders(hash.String())
	if len(holders) == 0 {
		return nil, fmt.Errorf("xorb %s has no live holder: %w", hash, fs.ErrNotExist)
	}
	var err error
	for _, p := range holders[:min(len(holders), relayAttempts)] {
		var sk *httpseek.Seeker
		if sk, err = s.open(ctx, p, hash, start, end); err == nil {
			return sk, nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
	}
	return nil, fmt.Errorf("xorb %s: %w", hash, err)
}

func (s *store) open(ctx context.Context, p apis.Peer, hash xet.XorbHash, start, end int64) (*httpseek.Seeker, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, xorbURL(p.URL, hash), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(apis.PeerHeader, "gateway")
	sk := httpseek.NewRangeSeeker(ctx, s.transport, req, start, end)
	if end < 0 {
		if _, err = sk.Seek(0, io.SeekEnd); err == nil {
			_, err = sk.Seek(start, io.SeekStart)
		}
	} else if resp, rerr := sk.Response(); rerr != nil {
		err = rerr
	} else if resp.StatusCode != http.StatusPartialContent {
		err = errors.New(resp.Status)
	}
	if err != nil {
		_ = sk.Close()
		return nil, fmt.Errorf("holder %s: %w", p.ID, err)
	}
	return sk, nil
}

func (s *store) GetXorbReadSeekCloser(ctx context.Context, hash xet.XorbHash) (io.ReadSeekCloser, error) {
	f, err := s.Storage.GetXorbReadSeekCloser(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		if err == nil && s.onRead != nil {
			s.onRead(hash)
		}
		return f, err
	}
	sk, err := s.remote(ctx, hash, 0, -1)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

func (s *store) GetXorbRangeReadCloser(ctx context.Context, hash xet.XorbHash, start, end int64) (io.ReadCloser, error) {
	rc, err := s.Storage.GetXorbRangeReadCloser(ctx, hash, start, end)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		if err == nil && s.onRead != nil {
			s.onRead(hash)
		}
		return rc, err
	}
	sk, err := s.remote(ctx, hash, start, end)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

// GetReconstructedFile reads through this store, so remote-only xorbs are relayed.
func (s *store) GetReconstructedFile(ctx context.Context, digest [32]byte) (io.ReadSeekCloser, error) {
	fh, err := s.Storage.GetFileHashBySHA256(ctx, digest)
	if err != nil {
		return nil, err
	}
	sh, err := s.GetShard(ctx, fh)
	if err != nil {
		return nil, err
	}
	return storage.NewReconstructedFile(ctx, s, sh, digest)
}

var errPinned = errors.New("xorb pinned by an in-flight shard verification")

// pin keeps the local copies of hashes safe from DeleteXorb until unpin.
func (s *store) pin(hashes ...xet.XorbHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hashes {
		s.pins[h]++
	}
}

// unpin releases pins and, once any hash is free, wakes the staging manager outside s.mu.
func (s *store) unpin(hashes ...xet.XorbHash) {
	s.mu.Lock()
	idle := false
	for _, h := range hashes {
		if s.pins[h]--; s.pins[h] == 0 {
			delete(s.pins, h)
			idle = true
		}
	}
	s.mu.Unlock()
	if idle && s.onIdle != nil {
		s.onIdle()
	}
}

// DeleteXorb drops the local copy and its offset caches; the sidecar stays for later relays.
func (s *store) DeleteXorb(ctx context.Context, hash xet.XorbHash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[hash] > 0 {
		return fmt.Errorf("xorb %s: %w", hash, errPinned)
	}
	return s.Storage.DeleteXorb(ctx, hash)
}

// stage copies a xorb only holders have into the local store so local verification can read it.
func (s *store) stage(ctx context.Context, hash xet.XorbHash) error {
	ok, err := s.Storage.HasXorb(ctx, hash)
	if err != nil || ok {
		return err
	}
	sk, err := s.remote(ctx, hash, 0, -1)
	if err != nil {
		return err
	}
	defer func() { _ = sk.Close() }()
	_, err = s.PutXorb(ctx, hash, sk)
	return err
}

// PutShard stages and pins every referenced xorb so the local store's full verification can run; fully indexed shards skip staging.
func (s *store) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	indexed := len(sh.Files) > 0
	for i := range sh.Files {
		entry, err := s.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
		if err != nil {
			return false, err
		}
		indexed = indexed && entry != ""
	}
	if indexed {
		return s.Storage.PutShard(ctx, sh)
	}
	hashes := xorbsOf(sh.Files, sh.CASInfos)
	s.pin(hashes...)
	defer s.unpin(hashes...)
	for _, h := range hashes {
		if err := s.stage(ctx, h); err != nil {
			return false, err
		}
	}
	return s.Storage.PutShard(ctx, sh)
}

func (s *store) GetXorbDataRange(ctx context.Context, hash xet.XorbHash, chunkStart, chunkEnd uint32) (int64, int64, error) {
	offsets, err := s.GetXorbChunkOffsets(ctx, hash)
	if err != nil {
		return 0, 0, err
	}
	return xorb.ChunkDataRangeFromOffsets(offsets, chunkStart, chunkEnd)
}

// GetShardByChunkHash always misses: cross-file dedup would bind new files to xorbs no peer may hold anymore.
func (s *store) GetShardByChunkHash(context.Context, xet.ChunkHash) (*shard.Shard, error) {
	return nil, fmt.Errorf("chunk dedup disabled: %w", fs.ErrNotExist)
}

var _ storage.Storage = (*store)(nil)
