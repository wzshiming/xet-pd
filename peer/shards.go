package peer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// shardCache keeps the shards fetched from the peers holding them under dir, named by the SHA-256 of their bytes, with the files they describe indexed in memory.
type shardCache struct {
	dir string

	mu     sync.Mutex
	sha256 map[shard.SHA256Hash]xet.FileHash
	files  map[xet.FileHash]string // name of the shard describing the file
}

// openShardCache indexes the shards already under dir; a name that is no SHA-256 or a shard that fails to decode is skipped.
func openShardCache(dir string) (*shardCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &shardCache{dir: dir, sha256: map[shard.SHA256Hash]xet.FileHash{}, files: map[xet.FileHash]string{}}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		name := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		if _, err := hex.DecodeString(name); err != nil || len(name) != 2*sha256.Size {
			return nil
		}
		if sh, err := c.load(name); err == nil {
			c.index(name, sh)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *shardCache) path(name string) string {
	return filepath.Join(c.dir, filepath.FromSlash(storage.ObjectKey("", name)))
}

// add stores data, the serialized sh, and indexes the files sh describes.
func (c *shardCache) add(data []byte, sh *shard.Shard) error {
	sum := sha256.Sum256(data)
	name := hex.EncodeToString(sum[:])
	path := c.path(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".shard-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	c.index(name, sh)
	return nil
}

// index records the files sh, cached as name, describes.
func (c *shardCache) index(name string, sh *shard.Shard) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range sh.Files {
		c.files[f.FileHash] = name
		if f.MetadataExt != nil {
			c.sha256[f.MetadataExt.SHA256Hash] = f.FileHash
		}
	}
}

// fileHash reports the file a cached shard records under SHA-256 digest.
func (c *shardCache) fileHash(digest [32]byte) (xet.FileHash, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fh, ok := c.sha256[shard.NewSHA256Hash(digest)]
	return fh, ok
}

// get decodes the cached shard describing fileHash, wrapping fs.ErrNotExist when none does.
func (c *shardCache) get(fileHash xet.FileHash) (*shard.Shard, error) {
	c.mu.Lock()
	name, ok := c.files[fileHash]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no cached shard of file %s: %w", fileHash, fs.ErrNotExist)
	}
	return c.load(name)
}

// load decodes the cached shard name.
func (c *shardCache) load(name string) (*shard.Shard, error) {
	f, err := os.Open(c.path(name))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return storage.DecodeStoredShard(f)
}

// byShard lists the cached files under the name of the shard describing them.
func (c *shardCache) byShard() map[string][]xet.FileHash {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string][]xet.FileHash{}
	for fh, name := range c.files {
		out[name] = append(out[name], fh)
	}
	return out
}
