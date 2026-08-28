//go:build e2e || e2e_hf

// Package e2e exercises the compiled xet-pd binary at the process boundary:
// real agent and tracker subprocesses on real TCP ports, driven over HTTP.
package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// binPath is the xet-pd binary TestMain compiles once for every test.
var binPath string

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

// runMain wraps m.Run so the temp dir cleanup survives os.Exit.
func runMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "xet-pd-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	binPath = filepath.Join(dir, "xet-pd")
	cmd := exec.Command("go", "build", "-o", binPath, "github.com/wzshiming/xet-pd/cmd/xet-pd")
	cmd.Dir = ".." // Repo root; the test binary starts in e2e/.
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build xet-pd: %v\n%s", err, out)
		return 1
	}
	return m.Run()
}

// childEnv builds a scrubbed subprocess environment, so host XET_PD_* and
// HF_TOKEN state cannot leak into the processes under test.
func childEnv() []string {
	var env []string
	for _, key := range []string{"PATH", "HOME", "TMPDIR"} {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}
	return env
}

// freePort reserves an ephemeral 127.0.0.1 port and releases it for a
// subprocess to bind.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// syncBuffer guards the output buffer shared between the exec pipe goroutine
// and test-side dumps.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// proc is one running xet-pd subprocess; URL is its base HTTP address.
type proc struct {
	URL string
	cmd *exec.Cmd
	out *syncBuffer
}

// startProc spawns the binary and registers a cleanup that SIGTERMs the
// process, waits up to 10s, and asserts a zero exit — graceful shutdown is
// an implicit assertion of every test. Combined output is logged on failure.
func startProc(t *testing.T, name string, args ...string) *proc {
	t.Helper()
	return startProcEnv(t, name, nil, args...)
}

// startProcEnv is startProc with extra child environment entries appended to
// the scrubbed base environment.
func startProcEnv(t *testing.T, name string, extraEnv []string, args ...string) *proc {
	t.Helper()
	p := &proc{out: &syncBuffer{}}
	p.cmd = exec.Command(binPath, args...)
	p.cmd.Dir = t.TempDir()
	p.cmd.Env = append(childEnv(), extraEnv...)
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("signal %s: %v", name, err)
		}
		done := make(chan error, 1)
		go func() { done <- p.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s exit after SIGTERM = %v, want exit code 0", name, err)
			}
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
			<-done
			t.Errorf("%s still running 10s after SIGTERM", name)
		}
		if t.Failed() {
			t.Logf("%s output:\n%s", name, p.out.String())
		}
	})
	return p
}

// startAgent starts an agent subprocess against upstream and waits until it
// answers HTTP. Extra flags (-tracker, -peers, ...) are appended verbatim.
func startAgent(t *testing.T, upstream string, extraFlags ...string) *proc {
	t.Helper()
	return startAgentEnv(t, upstream, nil, extraFlags...)
}

// startAgentEnv is startAgent with extra child environment entries (e.g. a
// forward proxy interposed on the agent's egress via HTTPS_PROXY).
func startAgentEnv(t *testing.T, upstream string, extraEnv []string, extraFlags ...string) *proc {
	t.Helper()
	port := freePort(t)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	args := append([]string{
		"agent",
		"-listen", fmt.Sprintf("127.0.0.1:%d", port),
		"-upstream", upstream,
		"-storage-dir", t.TempDir(),
		"-external-url", url,
	}, extraFlags...)
	p := startProcEnv(t, "agent "+url, extraEnv, args...)
	p.URL = url
	waitServing(t, p, "agent "+url)
	return p
}

// startTracker starts a tracker subprocess and waits until it answers HTTP.
func startTracker(t *testing.T) *proc {
	t.Helper()
	port := freePort(t)
	p := startProc(t, "tracker",
		"tracker",
		"-listen", fmt.Sprintf("127.0.0.1:%d", port),
		"-storage-dir", t.TempDir(),
	)
	p.URL = fmt.Sprintf("http://127.0.0.1:%d", port)
	waitServing(t, p, "tracker")
	return p
}

// waitServing polls /xet-token until any HTTP response arrives: the agent
// answers it locally (no upstream round-trip, which can exceed the poll
// budget against a real internet upstream) and the tracker 404s instantly.
func waitServing(t *testing.T, p *proc, name string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(p.URL + "/xet-token")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s never answered HTTP at %s; output:\n%s", name, p.URL, p.out.String())
}
