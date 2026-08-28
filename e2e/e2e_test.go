//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	mrand "math/rand"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/hubtest"
	"github.com/wzshiming/xet-pd/tracker"
)

func deterministicData(n int) []byte {
	b := make([]byte, n)
	_, _ = mrand.New(mrand.NewSource(42)).Read(b)
	return b
}

// getBody GETs the URL, following redirects, and asserts a final 200.
func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status = %d, want 200; body %q", url, resp.StatusCode, body)
	}
	return body
}

// waitReady polls the resolve URL until the agent answers with the
// ready-state redirect and returns that response (body already consumed).
func waitReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusFound {
				return resp
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("agent never reached ready state")
	return nil
}

// waitFileAnnounced polls the tracker's anonymous lookup until peer claims
// the file both under its sha256 and under the resolve-path key.
func waitFileAnnounced(t *testing.T, trackerURL, sha, key, peer string) {
	t.Helper()
	tc, err := tracker.NewClient(trackerURL)
	if err != nil {
		t.Fatal(err)
	}
	req := &tracker.LookupRequest{Files: []string{sha}, Keys: []string{key}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		resp, err := tc.Lookup(ctx, req)
		if err == nil &&
			slices.Contains(resp.Files[sha].Peers, peer) &&
			slices.Contains(resp.Keys[key].Peers, peer) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("tracker never showed %s announced by %s", key, peer)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestColdWarmAndOriginDown: a cold download streams the origin's bytes, a
// warm re-download serves them without any origin xorb traffic, and the
// cached content keeps serving identical bytes after the origin dies.
func TestColdWarmAndOriginDown(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/e2e-cold-warm.bin"
	data := deterministicData(2 << 20)
	hub.Set(t, resolvePath, data)

	ag := startAgent(t, hub.Server.URL)

	if body := getBody(t, ag.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("cold download body mismatch")
	}
	ready := waitReady(t, ag.URL+resolvePath)
	if got := ready.Header.Get("X-Xet-PD-Agent"); got != "1" {
		t.Errorf("X-Xet-PD-Agent = %q, want 1", got)
	}

	fetches, dataBytes := hub.XorbFetchCount(), hub.XorbRouteBytes()
	if body := getBody(t, ag.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("warm download body mismatch")
	}
	if got := hub.XorbFetchCount(); got != fetches {
		t.Errorf("hub xorb fetches after warm download = %d, want %d (origin untouched)", got, fetches)
	}
	if got := hub.XorbRouteBytes(); got != dataBytes {
		t.Errorf("hub xorb bytes after warm download = %d, want %d (origin untouched)", got, dataBytes)
	}

	hub.Server.Close()
	if body := getBody(t, ag.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("download after origin death body mismatch")
	}
}

// TestTrackerPeerRelay: two agents share only a tracker; once A's cold
// download is announced, B's download of the same file moves zero xorb data
// over the origin — the content flows from A.
func TestTrackerPeerRelay(t *testing.T) {
	hub := hubtest.New(t)
	const resolvePath = "/org/repo/resolve/main/e2e-relay.bin"
	data := deterministicData(2 << 20)
	f := hub.Set(t, resolvePath, data)

	tr := startTracker(t)
	agentA := startAgent(t, hub.Server.URL, "-tracker", tr.URL)
	agentB := startAgent(t, hub.Server.URL, "-tracker", tr.URL)

	if body := getBody(t, agentA.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent A cold download body mismatch")
	}
	waitReady(t, agentA.URL+resolvePath)
	waitFileAnnounced(t, tr.URL, f.SHA, resolvePath, agentA.URL)

	fetches, dataBytes := hub.XorbFetchCount(), hub.XorbRouteBytes()
	if body := getBody(t, agentB.URL+resolvePath); !bytes.Equal(body, data) {
		t.Fatal("agent B download via peer body mismatch")
	}
	if got := hub.XorbFetchCount(); got != fetches {
		t.Errorf("hub xorb fetches after B's download = %d, want %d (relayed from A)", got, fetches)
	}
	if got := hub.XorbRouteBytes(); got != dataBytes {
		t.Errorf("hub xorb bytes after B's download = %d, want %d (relayed from A)", got, dataBytes)
	}
}

// runCLI runs the binary once with a scrubbed environment and returns its
// exit code, stdout and stderr.
func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binPath, args...)
	cmd.Dir = t.TempDir()
	cmd.Env = childEnv()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run %v: %v", args, err)
		}
		code = exitErr.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}

// TestCLIErrors: bad invocations fail with the documented exit codes and
// diagnostics on stderr.
func TestCLIErrors(t *testing.T) {
	t.Run("no args prints usage", func(t *testing.T) {
		code, _, stderr := runCLI(t)
		if code != 2 {
			t.Errorf("exit code = %d, want 2", code)
		}
		if !strings.Contains(stderr, "Usage: xet-pd") {
			t.Errorf("stderr = %q, want usage text", stderr)
		}
	})
	t.Run("unknown subcommand", func(t *testing.T) {
		code, _, stderr := runCLI(t, "bogus")
		if code != 2 {
			t.Errorf("exit code = %d, want 2", code)
		}
		if !strings.Contains(stderr, "Usage: xet-pd") {
			t.Errorf("stderr = %q, want usage text", stderr)
		}
	})
	t.Run("agent rejects non-positive peer-timeout", func(t *testing.T) {
		code, _, stderr := runCLI(t, "agent", "-peer-timeout", "0")
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr, "peer-timeout") {
			t.Errorf("stderr = %q, want it to mention peer-timeout", stderr)
		}
	})
}
