// Package clustertest holds the fixtures and helpers the gateway, peer, cmd and e2e tests share.
package clustertest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"

	"github.com/wzshiming/xet-pd/apis"
)

// SHA256 returns the hex SHA-256 digest of data.
func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// RandomData is deterministic incompressible content.
func RandomData(seed byte, n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

// RandomChunks splits RandomData(seed, n*size) into n chunks of size bytes.
func RandomChunks(seed byte, n, size int) [][]byte {
	data := RandomData(seed, n*size)
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = data[i*size : (i+1)*size : (i+1)*size]
	}
	return chunks
}

// WaitTimeout bounds the waits of in-process tests.
const WaitTimeout = 5 * time.Second

// WaitFor polls cond until it holds and fails the test once timeout has passed.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Diag returns the peer diagnostics of the gateway at origin.
func Diag(t testing.TB, origin string) apis.PeersResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, origin+apis.PeersPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var d apis.PeersResponse
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &d) != nil {
		t.Fatalf("GET %s = %d %q", apis.PeersPath, resp.StatusCode, body)
	}
	return d
}

// Scrape returns the samples the Prometheus text exposition at origin+"/metrics" carries, keyed by name{labels} as written.
func Scrape(t testing.TB, origin string) map[string]float64 {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, origin+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d %q", resp.StatusCode, body)
	}
	samples := map[string]float64{}
	for line := range strings.Lines(string(body)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// The value follows the last space; label values may hold spaces themselves.
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if i < 0 || err != nil {
			t.Fatalf("GET /metrics: unparsable sample %q", line)
		}
		samples[line[:i]] = v
	}
	return samples
}

// HasXorb reports whether s stores h and fails the test when the lookup fails.
func HasXorb(t testing.TB, s interface {
	HasXorb(context.Context, xet.XorbHash) (bool, error)
}, h xet.XorbHash) bool {
	t.Helper()
	ok, err := s.HasXorb(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// PackXorb packs chunks with the real encoder, which picks the smallest encoding per chunk.
func PackXorb(t testing.TB, chunks [][]byte) (xet.XorbHash, []byte) {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, true)
	for _, chunk := range chunks {
		if _, err := enc.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return enc.SummoryHash(), buf.Bytes()
}

// MakeXorb packs deterministic incompressible chunks with the real encoder.
func MakeXorb(t testing.TB, seed byte, chunks, chunkSize int) (xet.XorbHash, []byte) {
	t.Helper()
	return PackXorb(t, RandomChunks(seed, chunks, chunkSize))
}
