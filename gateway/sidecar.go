package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"

	"github.com/wzshiming/xet-pd/apis"
)

// maxSidecarBytes bounds a JSON array of xet.MaxChunksPerXorb 20-digit offsets.
const maxSidecarBytes = (xet.MaxChunksPerXorb + 1) * 21

// minChunkBytes is a packed chunk's header, the least one offset may advance past the previous, as the footer reader requires.
const minChunkBytes = 8

// sidecar persists one JSON array of chunk offsets per xorb so ranges stay computable after the local bytes are gone.
type sidecar struct {
	dir string
}

func newSidecar(dir string) (sidecar, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return sidecar{}, err
	}
	return sidecar{dir: dir}, nil
}

func (c sidecar) path(hash xet.XorbHash) string {
	return filepath.Join(c.dir, filepath.FromSlash(storage.ObjectKey("", hash.String())))
}

// read accepts exactly one bounded JSON array of valid offsets.
func (c sidecar) read(hash xet.XorbHash) ([]uint64, error) {
	f, err := os.Open(c.path(hash))
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
	// Accept what a footer read would: a bounded count, each end within the packed cap and at least a header past the previous.
	if err == nil && len(offsets) > xet.MaxChunksPerXorb {
		err = fmt.Errorf("%d chunk offsets exceed %d", len(offsets), xet.MaxChunksPerXorb)
	}
	prev := uint64(0)
	for _, off := range offsets {
		if err != nil {
			break
		}
		if off > apis.MaxXorbBytes {
			err = fmt.Errorf("chunk offset %d exceeds %d", off, apis.MaxXorbBytes)
		} else if off < prev || off-prev < minChunkBytes {
			err = fmt.Errorf("chunk offset %d does not follow %d", off, prev)
		}
		prev = off
	}
	if err != nil {
		return nil, fmt.Errorf("offsets sidecar of xorb %s: %w", hash, err)
	}
	return offsets, nil
}

func (c sidecar) write(hash xet.XorbHash, offsets []uint64) error {
	path := c.path(hash)
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
