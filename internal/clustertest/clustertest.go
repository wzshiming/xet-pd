// Package clustertest holds the fixtures and helpers the gateway, peer, cmd and e2e tests share.
package clustertest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
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

// MakeXorb packs deterministic incompressible chunks with the real encoder.
func MakeXorb(t testing.TB, seed byte, chunks, chunkSize int) (xet.XorbHash, []byte) {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, true)
	rng := rand.NewChaCha8([32]byte{seed})
	chunk := make([]byte, chunkSize)
	for range chunks {
		if _, err := rng.Read(chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := enc.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return enc.SummoryHash(), buf.Bytes()
}
