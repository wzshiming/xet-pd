package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/inventory"
)

// storeConfig configures newStore.
type storeConfig struct {
	Dir         string    // data directory: data/ holds the local store, spool/ in-flight pulls
	Self        apis.Peer // ID sent to the tracker and to holders, URL where this peer serves xorbs
	Tracker     string    // tracker origin answering lookups
	Secret      string    // bearer token of lookups and shard fetches
	Client      *http.Client
	MaxPulls    int
	PullTimeout time.Duration
	Metrics     *peerMetrics
}

// store is the peer's storage.Storage: local bytes first, then shards and xorbs of the holders the tracker names.
type store struct {
	*local.Storage
	cfg      storeConfig
	fetch    fetcher
	sidecar  sidecar
	shards   *shardCache
	spoolDir string
	sem      chan struct{} // bounds parallel pulls

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex // guards pins, writers, pulls, cooldown and each pull's refs, queued and keep; never held across network I/O
	pins     map[xet.XorbHash]int
	writers  map[xet.XorbHash]*writerLock
	pulls    map[xet.XorbHash]*pull
	cooldown map[xet.XorbHash]cooled

	// hooks, set once by the owner before concurrent use and run outside s.mu
	onStored func(hash xet.XorbHash, size int64, inserted bool) // bytes and offsets durable, still pinned
	onRead   func(hash xet.XorbHash)                            // a local copy was opened
	onFile   func(file apis.File, xorbs []xet.XorbHash)         // a shard describing file was stored
	onIdle   func()                                             // some xorb lost its last pin
}

// writerLock serializes the writers of one xorb's local bytes and sidecar.
type writerLock struct {
	sync.Mutex
	users int // guarded by store.mu
}

// holderAttempts bounds the holders asked per shard fetch or pull.
const holderAttempts = 3

func newStore(cfg storeConfig) (*store, error) {
	dir := filepath.Join(cfg.Dir, "data")
	st, err := local.NewStorage(local.WithBasePath(dir))
	if err != nil {
		return nil, err
	}
	sc, err := newSidecar(filepath.Join(dir, "offsets"))
	if err != nil {
		return nil, err
	}
	shards, err := openShardCache(filepath.Join(dir, "remote-shards"))
	if err != nil {
		return nil, err
	}
	spoolDir, err := openSpool(cfg.Dir)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &store{
		Storage: st, cfg: cfg, fetch: fetcher{client: cfg.Client, self: cfg.Self.ID}, sidecar: sc, shards: shards,
		spoolDir: spoolDir, sem: make(chan struct{}, cfg.MaxPulls), ctx: ctx, cancel: cancel,
		pins: map[xet.XorbHash]int{}, writers: map[xet.XorbHash]*writerLock{}, pulls: map[xet.XorbHash]*pull{}, cooldown: map[xet.XorbHash]cooled{},
	}, nil
}

// Close cancels the pulls, queued ones included, waits for them and drops idle connections.
func (s *store) Close() error {
	s.mu.Lock()
	s.cancel() // acquire checks s.ctx under s.mu, so no pull starts after Close
	s.mu.Unlock()
	s.wg.Wait()
	s.cfg.Client.CloseIdleConnections()
	return nil
}

// stats leaves out cooldowns past their end that acquire has yet to sweep.
func (s *store) stats() (pulls, cooldowns int) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.cooldown {
		if now.Before(c.until) {
			cooldowns++
		}
	}
	return len(s.pulls), cooldowns
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

// GetXorbURL always names this peer, which serves or pulls every xorb it is asked for.
func (s *store) GetXorbURL(_ context.Context, hash xet.XorbHash) (string, error) {
	return apis.XorbURL(s.cfg.Self.URL, hash.String()), nil
}

// lookup asks the tracker, as this peer, which live peers hold what req lists.
func (s *store) lookup(ctx context.Context, req apis.LookupRequest) (apis.LookupResponse, error) {
	req.Peer = s.cfg.Self.ID
	return apis.Lookup(ctx, s.cfg.Client, s.cfg.Tracker, s.cfg.Secret, req)
}

// HasXorb reports a local copy, else whether the tracker names a live holder; local errors are never masked by holders.
func (s *store) HasXorb(ctx context.Context, hash xet.XorbHash) (bool, error) {
	ok, err := s.Storage.HasXorb(ctx, hash)
	if err != nil || ok {
		return ok, err
	}
	resp, err := s.lookup(ctx, apis.LookupRequest{Xorbs: []string{hash.String()}})
	if err != nil {
		return false, err
	}
	return len(resp.Xorbs[hash.String()]) > 0, nil
}

// xorbsOf lists the distinct xorbs the entries of f reference.
func xorbsOf(f *shard.FileBlock) []xet.XorbHash {
	seen := map[xet.XorbHash]bool{}
	var out []xet.XorbHash
	for _, e := range f.Entries {
		if !seen[e.CASHash] {
			seen[e.CASHash] = true
			out = append(out, e.CASHash)
		}
	}
	return out
}

// fileOf names f as announces do; a block without metadata has no SHA-256.
func fileOf(f *shard.FileBlock) apis.File {
	file := apis.File{Hash: f.FileHash.String()}
	if f.MetadataExt != nil {
		file.SHA256 = f.MetadataExt.SHA256Hash.String()
	}
	return file
}

// knownShard finds the shard of fileHash in the local index, then the shard cache, with the file's block.
func (s *store) knownShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, *shard.FileBlock, error) {
	sh, err := s.Storage.GetShard(ctx, fileHash)
	if errors.Is(err, fs.ErrNotExist) {
		sh, err = s.shards.get(fileHash)
	}
	if err != nil {
		return nil, nil, err
	}
	f := sh.File(fileHash)
	if f == nil {
		return nil, nil, fmt.Errorf("shard of file %s lacks its file block", fileHash)
	}
	return sh, f, nil
}

// GetShard fetches an unknown file's shard from a complete holder, and fails with fs.ErrNotExist while a xorb of the file is neither local nor held by a live peer, so the mirror re-ingests it.
func (s *store) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	sh, f, err := s.knownShard(ctx, fileHash)
	if errors.Is(err, fs.ErrNotExist) {
		var resp apis.LookupResponse
		if resp, err = s.lookup(ctx, apis.LookupRequest{Files: []string{fileHash.String()}}); err == nil {
			loc := resp.Files[fileHash.String()]
			sh, err = s.fetchShard(ctx, apis.File{Hash: fileHash.String(), SHA256: loc.SHA256}, loc.Peers)
		}
		if err == nil {
			f = sh.File(fileHash)
		}
	}
	if err != nil {
		return nil, err
	}
	var remote []string
	for _, h := range xorbsOf(f) {
		ok, err := s.Storage.HasXorb(ctx, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			remote = append(remote, h.String())
		}
	}
	if len(remote) == 0 {
		return sh, nil
	}
	resp, err := s.lookup(ctx, apis.LookupRequest{Xorbs: remote})
	if err != nil {
		return nil, err
	}
	for _, h := range remote {
		if len(resp.Xorbs[h]) == 0 {
			return nil, fmt.Errorf("xorb %s of file %s has no copy: %w", h, fileHash, fs.ErrNotExist)
		}
	}
	return sh, nil
}

// GetFileHashBySHA256 resolves digest through the local index, the shard cache, then the shard of a complete holder, which it caches.
func (s *store) GetFileHashBySHA256(ctx context.Context, digest [32]byte) (xet.FileHash, error) {
	fh, err := s.Storage.GetFileHashBySHA256(ctx, digest)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return fh, err
	}
	if fh, ok := s.shards.fileHash(digest); ok {
		return fh, nil
	}
	sha := hex.EncodeToString(digest[:])
	resp, err := s.lookup(ctx, apis.LookupRequest{SHA256s: []string{sha}})
	if err != nil {
		return xet.FileHash{}, err
	}
	loc := resp.SHA256s[sha]
	if _, err := s.fetchShard(ctx, apis.File{Hash: loc.Hash, SHA256: sha}, loc.Peers); err != nil {
		return xet.FileHash{}, err
	}
	return xet.ParseFileHash(loc.Hash)
}

// GetShardBySHA256 goes through GetShard, so a file whose xorbs have no copy left is fs.ErrNotExist.
func (s *store) GetShardBySHA256(ctx context.Context, digest [32]byte) (*shard.Shard, error) {
	fh, err := s.GetFileHashBySHA256(ctx, digest)
	if err != nil {
		return nil, err
	}
	return s.GetShard(ctx, fh)
}

// GetShardByChunkHash always misses: cross-file dedup would bind new files to xorbs no peer may hold anymore.
func (s *store) GetShardByChunkHash(context.Context, xet.ChunkHash) (*shard.Shard, error) {
	return nil, fmt.Errorf("chunk dedup disabled: %w", fs.ErrNotExist)
}

// fetchShard keeps the first shard describing file that one of its best holders serves: cached, its offsets in the sidecar, its files reported.
func (s *store) fetchShard(ctx context.Context, file apis.File, holders []apis.Peer) (*shard.Shard, error) {
	for _, p := range holders[:min(len(holders), holderAttempts)] {
		resp, err := s.fetch.shard(ctx, p, s.cfg.Secret, file.Hash)
		if err != nil {
			continue
		}
		sh, err := storage.DecodeStoredShard(bytes.NewReader(resp.Shard))
		if err != nil || !slices.ContainsFunc(sh.Files, func(f shard.FileBlock) bool { return fileOf(&f) == file }) {
			continue
		}
		if err := s.shards.add(resp.Shard, sh); err != nil {
			return nil, err
		}
		for h, offsets := range resp.Offsets {
			hash, err := xet.ParseXorbHash(h)
			if err != nil {
				continue
			}
			if err := s.sidecar.write(hash, offsets); err != nil {
				return nil, err
			}
		}
		s.report(sh)
		return sh, nil
	}
	return nil, fmt.Errorf("no holder served the shard of file %s: %w", file.Hash, fs.ErrNotExist)
}

// report hands every file sh describes to onFile.
func (s *store) report(sh *shard.Shard) {
	if s.onFile == nil {
		return
	}
	for i := range sh.Files {
		s.onFile(fileOf(&sh.Files[i]), xorbsOf(&sh.Files[i]))
	}
}

// localOffsets answers from the local bytes, then the sidecar; a wrapped fs.ErrNotExist means neither exists.
func (s *store) localOffsets(ctx context.Context, hash xet.XorbHash) ([]uint64, error) {
	offsets, err := s.Storage.GetXorbChunkOffsets(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return offsets, err
	}
	return s.sidecar.read(hash)
}

// GetXorbChunkOffsets answers from the local bytes or the sidecar, else pulls the xorb and answers from what the pull recorded.
func (s *store) GetXorbChunkOffsets(ctx context.Context, hash xet.XorbHash) ([]uint64, error) {
	offsets, err := s.localOffsets(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return offsets, err
	}
	f, err := s.acquire(hash, false)
	if f != nil {
		err = f.wait(ctx)
	}
	if err != nil {
		return nil, err
	}
	return s.localOffsets(ctx, hash)
}

// pullFile makes the file complete here: its shard known and every xorb it references local, pulled whether or not anyone reads it.
func (s *store) pullFile(ctx context.Context, fileHash string) error {
	fh, err := xet.ParseFileHash(fileHash)
	if err != nil {
		return err
	}
	sh, err := s.GetShard(ctx, fh)
	if err != nil {
		return err
	}
	var pulls []*pull
	for _, h := range xorbsOf(sh.File(fh)) {
		f, aerr := s.acquire(h, true)
		if f != nil {
			pulls = append(pulls, f)
		}
		if err == nil {
			err = aerr
		}
	}
	for _, f := range pulls {
		if werr := f.wait(ctx); err == nil {
			err = werr
		}
	}
	return err
}

// shardResponse is what this peer serves for fileHash: the shard, footerless, and the offsets of every xorb of the file; fs.ErrNotExist when a xorb has neither local bytes nor a sidecar.
func (s *store) shardResponse(ctx context.Context, fileHash xet.FileHash) (apis.ShardResponse, error) {
	sh, f, err := s.knownShard(ctx, fileHash)
	if err != nil {
		return apis.ShardResponse{}, err
	}
	resp := apis.ShardResponse{Offsets: map[string][]uint64{}}
	for _, h := range xorbsOf(f) {
		if resp.Offsets[h.String()], err = s.localOffsets(ctx, h); err != nil {
			return apis.ShardResponse{}, err
		}
	}
	resp.Shard, _, err = storage.EncodeShard(sh)
	return resp, err
}

// fileEntry is a known file and the xorbs it references.
type fileEntry struct {
	File  apis.File
	Xorbs []xet.XorbHash
}

// inventory lists the local xorbs oldest first and, once each, the files a local or cached shard describes.
func (s *store) inventory(ctx context.Context) ([]inventory.Xorb, []fileEntry, error) {
	xorbs, err := inventory.Scan(ctx, s.Storage)
	if err != nil {
		return nil, nil, err
	}
	indexed := map[string][]xet.FileHash{}
	err = s.Storage.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		if fh, err := xet.ParseFileHash(fileHash); err == nil {
			indexed[shardHash] = append(indexed[shardHash], fh)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	var files []fileEntry
	seen := map[xet.FileHash]bool{}
	add := func(sh *shard.Shard, hashes []xet.FileHash) {
		for _, fh := range hashes {
			if f := sh.File(fh); f != nil && !seen[fh] {
				seen[fh] = true
				files = append(files, fileEntry{fileOf(f), xorbsOf(f)})
			}
		}
	}
	for shardHash, hashes := range indexed {
		sh, err := s.Storage.LoadShard(ctx, shardHash)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		add(sh, hashes)
	}
	for name, hashes := range s.shards.byShard() {
		sh, err := s.shards.load(name)
		if err != nil {
			return nil, nil, err
		}
		add(sh, hashes)
	}
	return xorbs, files, nil
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
	// Record the offsets of the committed local bytes and their size; the hash does not bind their packing.
	f, err := s.Storage.GetXorbReadSeekCloser(ctx, hash)
	if err != nil {
		return false, err
	}
	offsets, err := storage.ReadXorbChunkOffsets(f)
	if err != nil {
		_ = f.Close()
		return false, fmt.Errorf("read chunk offsets of xorb %s: %w", hash, err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err == nil {
		err = s.sidecar.write(hash, offsets)
	}
	_ = f.Close()
	if err != nil {
		return false, err
	}
	if s.onStored != nil {
		s.onStored(hash, size, inserted)
	}
	return inserted, nil
}

// releaser reads until Close, which then lets go of what kept the bytes readable.
type releaser struct {
	io.ReadSeekCloser
	release func()
}

func (r releaser) Close() error {
	err := r.ReadSeekCloser.Close()
	r.release()
	return err
}

// GetXorbReadSeekCloser opens the local copy, else streams the shared pull of hash as it lands; hash stays pinned until Close.
func (s *store) GetXorbReadSeekCloser(ctx context.Context, hash xet.XorbHash) (io.ReadSeekCloser, error) {
	s.pin(hash)
	rs, err := s.Storage.GetXorbReadSeekCloser(ctx, hash)
	var f *pull
	if errors.Is(err, fs.ErrNotExist) {
		if f, err = s.acquire(hash, false); err == nil && f == nil {
			rs, err = s.Storage.GetXorbReadSeekCloser(ctx, hash) // committed meanwhile
		}
	}
	if f != nil {
		var size int64
		if size, err = f.servable(ctx); err == nil {
			rs, err = f.stream.NewReadSeeker(0, int(size))
		}
		if err != nil {
			f.release()
		}
	}
	if err != nil {
		s.unpin(hash)
		return nil, err
	}
	if f == nil {
		if s.onRead != nil {
			s.onRead(hash)
		}
		return releaser{rs, func() { s.unpin(hash) }}, nil
	}
	// Spool reads block on the holder regardless of the caller; closing wakes them.
	stop := context.AfterFunc(ctx, func() { _ = rs.Close() })
	return releaser{rs, func() { stop(); f.release(); s.unpin(hash) }}, nil
}

var errPinned = errors.New("xorb pinned by a reader or writer")

// pin keeps the local copy of hash safe from DeleteXorb until unpin.
func (s *store) pin(hash xet.XorbHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins[hash]++
}

// unpin releases a pin and, once hash is free, tells the owner outside s.mu.
func (s *store) unpin(hash xet.XorbHash) {
	s.mu.Lock()
	idle := false
	if s.pins[hash]--; s.pins[hash] == 0 {
		delete(s.pins, hash)
		idle = true
	}
	s.mu.Unlock()
	if idle && s.onIdle != nil {
		s.onIdle()
	}
}

// DeleteXorb drops the local copy and its offset caches under s.mu, atomically with the pin check; the sidecar stays.
func (s *store) DeleteXorb(ctx context.Context, hash xet.XorbHash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[hash] > 0 {
		return fmt.Errorf("xorb %s: %w", hash, errPinned)
	}
	return s.Storage.DeleteXorb(ctx, hash)
}

// PutShard stores sh locally, then reports every file it describes.
func (s *store) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	inserted, err := s.Storage.PutShard(ctx, sh)
	if err == nil {
		s.report(sh)
	}
	return inserted, err
}

var _ storage.Storage = (*store)(nil)
