package clustertest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var resolveRe = regexp.MustCompile(`^(/.+?/resolve/)[^/]+(/.+)$`)

// Hub serves the files given to Set; Commit is the 40-hex revision every branch resolves to. It is a plain (non-xet) upstream hub fixture: resolve requests answer hub metadata and redirect to a /cdn path that serves the bytes with Range support.
type Hub struct {
	*httptest.Server
	Commit string

	mu    sync.Mutex
	files map[string][]byte
	gets  int
	bytes int64
	auth  []string
}

func NewHub(t testing.TB) *Hub {
	h := &Hub{Commit: strings.Repeat("ab", 20), files: map[string][]byte{}}
	h.Server = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.Close)
	return h
}

// Set publishes data at a resolve path and, like a hub, at the same path with the revision replaced by Commit.
func (h *Hub) Set(path string, data []byte) {
	data = bytes.Clone(data)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files[path] = data
	if m := resolveRe.FindStringSubmatch(path); m != nil {
		h.files[m[1]+h.Commit+m[2]] = data
	}
}

// CDN reports the GETs and body bytes served from /cdn; HEADs count nothing.
func (h *Hub) CDN() (gets int, bytes int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.gets, h.bytes
}

// Auth lists the Authorization header of every hub request outside /cdn in arrival order.
func (h *Hub) Auth() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.auth)
}

type counter struct {
	http.ResponseWriter
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}

func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	if p, ok := strings.CutPrefix(r.URL.Path, "/cdn"); ok {
		h.mu.Lock()
		data, found := h.files[p]
		h.mu.Unlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		cw := &counter{ResponseWriter: w}
		http.ServeContent(cw, r, "", time.Time{}, bytes.NewReader(data))
		if r.Method == http.MethodGet {
			h.mu.Lock()
			h.gets++
			h.bytes += cw.n
			h.mu.Unlock()
		}
		return
	}
	h.mu.Lock()
	h.auth = append(h.auth, r.Header.Get("Authorization"))
	data, found := h.files[r.URL.Path]
	h.mu.Unlock()
	if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/paths-info/") {
		_ = r.ParseForm()
		out := []map[string]string{}
		for _, p := range r.PostForm["paths"] {
			out = append(out, map[string]string{"type": "file", "path": p})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(data)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Linked-Etag", etag)
	w.Header().Set("X-Linked-Size", strconv.Itoa(len(data)))
	w.Header().Set("X-Repo-Commit", h.Commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}
