// Package segstore stores partial xorb chunk ranges (segments) as files, so
// an agent can serve pieces of xorbs it has not fully ingested. A segment is
// a contiguous packed chunk sequence sliced from a xorb stream, keyed by
// (xorbHash, chunkStart, chunkEnd) and remembered with the byte range it
// occupied in the source encoding.
package segstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/wzshiming/xet/xorb"
)

// xorbHashRe guards every path derivation: only a canonical lowercase hex
// hash may become a directory name.
var xorbHashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Segment describes one stored chunk range: chunks [ChunkStart, ChunkEnd) of
// xorb XorbHash, which occupied packed bytes [ByteStart, ByteEnd] (inclusive)
// in the source encoding it was captured from. Size is the stored data
// length, always ByteEnd-ByteStart+1. Quarantined marks a segment whose
// content is structurally valid but not yet verified against a file
// checksum: it is stored but never found for serving and never announced
// until released. Metas written before the field read back as verified.
//
// FileHash, Peer and QuarantinedAt persist a quarantined segment's
// ownership — the reconstruction file whose checksum verdict settles it,
// the peer that supplied it, and when it was quarantined (unix seconds) —
// so it can be settled or expired without any in-memory record surviving a
// restart. They are empty on verified segments; metas written before the
// fields read back with zero values, a zero QuarantinedAt meaning the
// quarantine time is unknown.
type Segment struct {
	XorbHash      string `json:"xorb_hash"`
	ChunkStart    uint32 `json:"chunk_start"`
	ChunkEnd      uint32 `json:"chunk_end"`
	ByteStart     int64  `json:"byte_start"`
	ByteEnd       int64  `json:"byte_end"`
	Size          int64  `json:"size"`
	Quarantined   bool   `json:"quarantined,omitempty"`
	FileHash      string `json:"file_hash,omitempty"`
	Peer          string `json:"peer,omitempty"`
	QuarantinedAt int64  `json:"quarantined_at,omitempty"`
}

// core returns the identity a committed key is immutable by — everything
// but the quarantine ownership, which an identical-bytes re-store may
// refresh.
func (seg Segment) core() Segment {
	seg.FileHash, seg.Peer, seg.QuarantinedAt = "", "", 0
	return seg
}

// Store is a file-backed segment store: one data file plus one meta JSON per
// segment, the files being the index so restarts need no recovery step. A
// directory is owned by one live Store at a time; concurrent stores over the
// same directory are not supported.
type Store struct {
	dir string

	// mu serializes writers so concurrent puts of one key cannot interleave
	// their data and meta renames.
	mu sync.Mutex
}

// Option configures the Store.
type Option func(*Store)

// WithDir sets the root directory segments are stored under. Required.
func WithDir(dir string) Option {
	return func(s *Store) { s.dir = dir }
}

// New creates a Store rooted at the configured directory, which is created
// lazily on the first Put.
func New(opts ...Option) (*Store, error) {
	s := &Store{}
	for _, opt := range opts {
		opt(s)
	}
	if s.dir == "" {
		return nil, fmt.Errorf("segstore: dir is required")
	}
	return s, nil
}

func (s *Store) dataPath(xorbHash string, chunkStart, chunkEnd uint32) string {
	return filepath.Join(s.dir, xorbHash, fmt.Sprintf("%d-%d", chunkStart, chunkEnd))
}

// Put validates and atomically stores one verified segment. The data must
// be a self-contained packed chunk sequence: walking its 8-byte chunk
// headers must yield exactly chunkEnd-chunkStart chunks and land on the end
// of the data.
// A committed key is immutable: re-putting an identical segment is a no-op
// (healing a damaged data file with the same bytes), and re-putting a
// different encoding for an existing chunk range keeps the first one, so a
// reader can never pair a segment's meta with foreign-encoding data.
func (s *Store) Put(xorbHash string, chunkStart, chunkEnd uint32, byteStart, byteEnd int64, data []byte) error {
	_, err := s.put(xorbHash, chunkStart, chunkEnd, byteStart, byteEnd, data, false, "", "")
	return err
}

// PutQuarantined stores one segment in quarantine: kept on disk but invisible
// to Find and FindByteRange until Release verifies it. fileHash and peer are
// persisted in the meta together with the quarantine time, so the segment can
// be settled or expired after a restart; an identical-bytes re-store refreshes
// them — the new flight owns the segment now. It reports whether the store
// now holds exactly the given bytes under this key — created, healed, or
// already stored identically — so the caller may serve and settle them;
// false means a different segment owns the key (keep-first) and the given
// bytes were dropped.
func (s *Store) PutQuarantined(xorbHash string, chunkStart, chunkEnd uint32, byteStart, byteEnd int64, data []byte, fileHash, peer string) (bool, error) {
	return s.put(xorbHash, chunkStart, chunkEnd, byteStart, byteEnd, data, true, fileHash, peer)
}

func (s *Store) put(xorbHash string, chunkStart, chunkEnd uint32, byteStart, byteEnd int64, data []byte, quarantined bool, fileHash, peer string) (bool, error) {
	if !xorbHashRe.MatchString(xorbHash) {
		return false, fmt.Errorf("segstore: invalid xorb hash %q", xorbHash)
	}
	if chunkStart >= chunkEnd {
		return false, fmt.Errorf("segstore: invalid chunk range [%d, %d)", chunkStart, chunkEnd)
	}
	if byteStart < 0 || byteEnd < byteStart {
		return false, fmt.Errorf("segstore: invalid byte range [%d, %d]", byteStart, byteEnd)
	}
	if got, want := int64(len(data)), byteEnd-byteStart+1; got != want {
		return false, fmt.Errorf("segstore: data length %d does not match byte range length %d", got, want)
	}
	if err := ValidateChunkSequence(data, chunkEnd-chunkStart); err != nil {
		return false, fmt.Errorf("segstore: %w", err)
	}

	seg := Segment{
		XorbHash:    xorbHash,
		ChunkStart:  chunkStart,
		ChunkEnd:    chunkEnd,
		ByteStart:   byteStart,
		ByteEnd:     byteEnd,
		Size:        int64(len(data)),
		Quarantined: quarantined,
	}
	if quarantined {
		seg.FileHash, seg.Peer, seg.QuarantinedAt = fileHash, peer, time.Now().Unix()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dataPath := s.dataPath(xorbHash, chunkStart, chunkEnd)
	metaPath := dataPath + ".json"
	if existing, ok := readMeta(metaPath); ok {
		if existing.core() != seg.core() {
			return false, nil
		}
		// Meta-identical: an intact stored file wins — byte-identical data is
		// a no-op success, foreign bytes behind identical meta a conflict.
		// Only missing or damaged data is rewritten, with bytes the committed
		// meta equally describes.
		if stored, err := os.ReadFile(dataPath); err == nil &&
			int64(len(stored)) == seg.Size && ValidateChunkSequence(stored, chunkEnd-chunkStart) == nil {
			if !bytes.Equal(stored, data) {
				return false, nil
			}
			if !quarantined {
				return true, nil
			}
			// Identical quarantined re-store: refresh the persisted ownership
			// so the new flight's verdict and TTL apply to the segment.
			meta, err := json.Marshal(seg)
			if err != nil {
				return false, fmt.Errorf("segstore: marshal segment meta: %w", err)
			}
			if err := writeFileAtomic(metaPath, meta); err != nil {
				return false, fmt.Errorf("segstore: write segment meta: %w", err)
			}
			return true, nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0755); err != nil {
		return false, fmt.Errorf("segstore: create segment dir: %w", err)
	}
	// Data lands before meta: the meta rename is the commit point, and a
	// crash between the two leaves data without meta, which readers miss.
	if err := writeFileAtomic(dataPath, data); err != nil {
		return false, fmt.Errorf("segstore: write segment data: %w", err)
	}
	meta, err := json.Marshal(seg)
	if err != nil {
		return false, fmt.Errorf("segstore: marshal segment meta: %w", err)
	}
	if err := writeFileAtomic(metaPath, meta); err != nil {
		return false, fmt.Errorf("segstore: write segment meta: %w", err)
	}
	return true, nil
}

// Release lifts the quarantine on one stored segment via an atomic meta
// rewrite, reporting whether it flipped a quarantined segment; missing or
// already verified segments report false.
func (s *Store) Release(xorbHash string, chunkStart, chunkEnd uint32) (bool, error) {
	if !xorbHashRe.MatchString(xorbHash) {
		return false, fmt.Errorf("segstore: invalid xorb hash %q", xorbHash)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	metaPath := s.dataPath(xorbHash, chunkStart, chunkEnd) + ".json"
	seg, ok := readMeta(metaPath)
	if !ok || !seg.Quarantined {
		return false, nil
	}
	// Ownership describes quarantine only; a verified segment carries none.
	seg.Quarantined = false
	seg.FileHash, seg.Peer, seg.QuarantinedAt = "", "", 0
	meta, err := json.Marshal(seg)
	if err != nil {
		return false, fmt.Errorf("segstore: marshal segment meta: %w", err)
	}
	if err := writeFileAtomic(metaPath, meta); err != nil {
		return false, fmt.Errorf("segstore: release segment meta: %w", err)
	}
	return true, nil
}

// Delete removes one stored segment. The meta goes first — it is the commit
// point readers index by, so once it is gone no new reader can resurrect the
// key — then the data file. Missing segments delete cleanly.
func (s *Store) Delete(xorbHash string, chunkStart, chunkEnd uint32) error {
	if !xorbHashRe.MatchString(xorbHash) {
		return fmt.Errorf("segstore: invalid xorb hash %q", xorbHash)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dataPath := s.dataPath(xorbHash, chunkStart, chunkEnd)
	if err := os.Remove(dataPath + ".json"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("segstore: delete segment meta: %w", err)
	}
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("segstore: delete segment data: %w", err)
	}
	return nil
}

// DeleteQuarantinedBefore removes one stored segment only if its meta still
// marks it quarantined no later than cutoff — a missing timestamp counts as
// before any cutoff — re-read under the store lock, so a segment a
// concurrent flight just re-owned or released is never lost to a stale
// sweep decision. It reports whether the segment was removed: the meta
// unlink is the commit point, after which the data file is best-effort — an
// orphan without meta is invisible and healed over by a future put — while
// a failed meta unlink keeps the segment intact for a later retry.
func (s *Store) DeleteQuarantinedBefore(xorbHash string, chunkStart, chunkEnd uint32, cutoff time.Time) (bool, error) {
	if !xorbHashRe.MatchString(xorbHash) {
		return false, fmt.Errorf("segstore: invalid xorb hash %q", xorbHash)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dataPath := s.dataPath(xorbHash, chunkStart, chunkEnd)
	seg, ok := readMeta(dataPath + ".json")
	if !ok || !seg.Quarantined {
		return false, nil
	}
	if seg.QuarantinedAt > 0 && !time.Unix(seg.QuarantinedAt, 0).Before(cutoff) {
		return false, nil
	}
	if err := os.Remove(dataPath + ".json"); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("segstore: delete segment meta: %w", err)
	}
	_ = os.Remove(dataPath)
	return true, nil
}

// ValidateChunkSequence checks that data is a self-contained packed chunk
// sequence of exactly count chunks ending on the last byte.
func ValidateChunkSequence(data []byte, count uint32) error {
	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("scan segment chunks: %w", err)
	}
	if got := uint32(len(offsets)); got != count {
		return fmt.Errorf("segment has %d chunks, want %d", got, count)
	}
	if end := offsets[len(offsets)-1]; end != uint64(len(data)) {
		return fmt.Errorf("segment chunk data ends at %d, want %d", end, len(data))
	}
	return nil
}

// writeFileAtomic writes via a uniquely named temp file and renames it into
// place, so a reader never sees a partial file and concurrent writers never
// share a temp path.
func writeFileAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// readMeta loads one meta file, rejecting anything internally inconsistent.
func readMeta(path string) (Segment, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Segment{}, false
	}
	var seg Segment
	if json.Unmarshal(data, &seg) != nil || !xorbHashRe.MatchString(seg.XorbHash) ||
		seg.ChunkStart >= seg.ChunkEnd || seg.ByteStart < 0 || seg.Size != seg.ByteEnd-seg.ByteStart+1 {
		return Segment{}, false
	}
	return seg, true
}

// segments lists every stored segment of one xorb. A missing directory is
// an empty list.
func (s *Store) segments(xorbHash string) ([]Segment, error) {
	if !xorbHashRe.MatchString(xorbHash) {
		return nil, nil
	}
	entries, err := os.ReadDir(filepath.Join(s.dir, xorbHash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var segs []Segment
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".json" {
			continue
		}
		seg, ok := readMeta(filepath.Join(s.dir, xorbHash, de.Name()))
		if ok && seg.XorbHash == xorbHash {
			segs = append(segs, seg)
		}
	}
	return segs, nil
}

// Find returns the stored verified segment for chunks [chunkStart,
// chunkEnd): an exact match if present, otherwise the containing segment
// spanning the fewest chunks (segments from different source encodings can
// order differently by byte size than by chunk count). Quarantined segments
// are never found: unverified bytes must not be served.
func (s *Store) Find(xorbHash string, chunkStart, chunkEnd uint32) (Segment, bool) {
	if chunkStart >= chunkEnd {
		return Segment{}, false
	}
	var best Segment
	found := false
	segs, _ := s.segments(xorbHash)
	for _, seg := range segs {
		if seg.Quarantined {
			continue
		}
		if seg.ChunkStart == chunkStart && seg.ChunkEnd == chunkEnd {
			return seg, true
		}
		if seg.ChunkStart > chunkStart || chunkEnd > seg.ChunkEnd {
			continue
		}
		span, bestSpan := seg.ChunkEnd-seg.ChunkStart, best.ChunkEnd-best.ChunkStart
		if !found || span < bestSpan || (span == bestSpan && seg.Size < best.Size) {
			best, found = seg, true
		}
	}
	return best, found
}

// FindByteRange returns the stored verified segment covering packed bytes
// [byteStart, byteEnd] (inclusive): an exact match if present, otherwise the
// smallest containing segment. Byte offsets only match segments captured
// from the same source encoding, by construction of ByteStart/ByteEnd.
// Quarantined segments are never found.
func (s *Store) FindByteRange(xorbHash string, byteStart, byteEnd int64) (Segment, bool) {
	if byteStart < 0 || byteEnd < byteStart {
		return Segment{}, false
	}
	var best Segment
	found := false
	segs, _ := s.segments(xorbHash)
	for _, seg := range segs {
		if seg.Quarantined {
			continue
		}
		if seg.ByteStart == byteStart && seg.ByteEnd == byteEnd {
			return seg, true
		}
		if seg.ByteStart <= byteStart && byteEnd <= seg.ByteEnd && (!found || seg.Size < best.Size) {
			best, found = seg, true
		}
	}
	return best, found
}

// Open opens the segment's data file.
func (s *Store) Open(seg Segment) (io.ReadSeekCloser, error) {
	if !xorbHashRe.MatchString(seg.XorbHash) {
		return nil, fmt.Errorf("segstore: invalid xorb hash %q", seg.XorbHash)
	}
	f, err := os.Open(s.dataPath(seg.XorbHash, seg.ChunkStart, seg.ChunkEnd))
	if err != nil {
		return nil, fmt.Errorf("segstore: open segment data: %w", err)
	}
	return f, nil
}

// ChunkRange locates the byte range (inclusive) of chunks
// [chunkStart, chunkEnd) within the segment data read from r, walking the
// chunk headers from the segment start. Taking the caller's open handle
// keeps the offsets and the bytes later served from it coherent even if the
// segment file is concurrently replaced.
func (seg Segment) ChunkRange(r io.ReadSeeker, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	if chunkStart >= chunkEnd || chunkStart < seg.ChunkStart || chunkEnd > seg.ChunkEnd {
		return 0, 0, fmt.Errorf("segstore: chunk range [%d, %d) not within segment [%d, %d)", chunkStart, chunkEnd, seg.ChunkStart, seg.ChunkEnd)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("segstore: rewind segment data: %w", err)
	}
	startByte, endByte, err = xorb.ChunkDataRange(r, chunkStart-seg.ChunkStart, chunkEnd-seg.ChunkStart)
	if err != nil {
		return 0, 0, fmt.Errorf("segstore: locate chunk range: %w", err)
	}
	return startByte, endByte, nil
}

// Walk calls fn for every stored segment, stopping at the first error.
func (s *Store) Walk(fn func(Segment) error) error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("segstore: read store dir: %w", err)
	}
	for _, de := range entries {
		if !de.IsDir() || !xorbHashRe.MatchString(de.Name()) {
			continue
		}
		segs, err := s.segments(de.Name())
		if err != nil {
			return fmt.Errorf("segstore: list segments: %w", err)
		}
		for _, seg := range segs {
			if err := fn(seg); err != nil {
				return err
			}
		}
	}
	return nil
}
