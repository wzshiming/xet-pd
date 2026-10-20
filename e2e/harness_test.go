// Package e2e drives race-instrumented xet-pd binaries end to end.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/download"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const (
	stopTimeout   = 15 * time.Second // SIGTERM to exit
	readyTimeout  = 20 * time.Second // process start, registration, control round trips
	ingestTimeout = 60 * time.Second // hub ingest of one fixture file
	seedTimeout   = 60 * time.Second // seeds pulling and the gateway evicting
)

var binPath string // race-instrumented xet-pd built by TestMain

func TestMain(m *testing.M) {
	code, err := buildAndRun(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func buildAndRun(m *testing.M) (int, error) {
	dir, err := os.MkdirTemp("", "xet-pd-e2e-")
	if err != nil {
		return 1, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	root, err := filepath.Abs("..")
	if err != nil {
		return 1, err
	}
	binPath = filepath.Join(dir, "xet-pd")
	build := exec.Command("go", "build", "-race", "-o", binPath, "./cmd/xet-pd")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		return 1, fmt.Errorf("go build -race ./cmd/xet-pd: %v\n%s", err, out)
	}
	return m.Run(), nil
}

// childEnv strips xet-pd and Hugging Face settings from the inherited environment and exempts loopback from proxies.
func childEnv(extra ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(key, "XET_PD_") || strings.HasPrefix(key, "HF_") || key == "HUGGING_FACE_HUB_TOKEN" {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost")
	return append(env, extra...)
}

func secretEnv(secret string) []string {
	if secret == "" {
		return nil
	}
	return []string{"XET_PD_SECRET=" + secret}
}

// testSecret is a per-test shared secret that must never appear in process output.
func testSecret(t *testing.T) string {
	return "e2e-" + strings.ReplaceAll(t.Name(), "/", "-")
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// proc is one running xet-pd child; cleanup sends SIGTERM and expects a clean exit unless kill or discard ran first.
type proc struct {
	t      *testing.T
	name   string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdout logBuf
	stderr logBuf
	done   chan struct{} // closed once Wait returned

	mu      sync.Mutex
	lenient bool // exit status not asserted
	stopped bool
}

// startProc launches the binary; env entries extend childEnv.
func startProc(t *testing.T, name string, args []string, env ...string) *proc {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	p := &proc{t: t, name: name, cancel: cancel, done: make(chan struct{})}
	p.cmd = exec.CommandContext(ctx, binPath, args...)
	p.cmd.Env = childEnv(env...)
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	p.cmd.Cancel = func() error { return p.cmd.Process.Signal(syscall.SIGTERM) }
	p.cmd.WaitDelay = stopTimeout
	if err := p.cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		_ = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("%s output:\n%s%s", name, p.stdout.String(), p.stderr.String())
		}
	})
	t.Cleanup(p.stop)
	return p
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop terminates the process and reports a non-zero exit or a data race.
func (p *proc) stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	lenient := p.lenient
	p.mu.Unlock()
	p.cancel() // SIGTERM; WaitDelay kills a process that ignores it
	select {
	case <-p.done:
	case <-time.After(stopTimeout + 5*time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	if code := p.cmd.ProcessState.ExitCode(); code != 0 && !lenient {
		p.t.Errorf("%s exited with status %d after SIGTERM", p.name, code)
	}
	if strings.Contains(p.stderr.String(), "DATA RACE") {
		p.t.Errorf("%s reported a data race", p.name)
	}
}

// kill ends the process abruptly; the loss is intentional, so stop does not report it.
func (p *proc) kill() {
	p.t.Helper()
	p.discard()
	if err := p.cmd.Process.Kill(); err != nil {
		p.t.Fatalf("kill %s: %v", p.name, err)
	}
	select {
	case <-p.done:
	case <-time.After(stopTimeout):
		p.t.Fatalf("%s survived SIGKILL", p.name)
	}
}

// discard marks any exit status acceptable.
func (p *proc) discard() {
	p.mu.Lock()
	p.lenient = true
	p.mu.Unlock()
}

// awaitBanner returns what follows prefix on the first matching stdout line.
func (p *proc) awaitBanner(prefix string) (string, error) {
	deadline := time.Now().Add(readyTimeout)
	for {
		for _, line := range strings.Split(p.stdout.String(), "\n") {
			if rest, ok := strings.CutPrefix(line, prefix); ok {
				return rest, nil
			}
		}
		if p.exited() {
			return "", fmt.Errorf("%s exited before its banner (status %d):\n%s", p.name, p.cmd.ProcessState.ExitCode(), p.stderr.String())
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("%s printed no banner within %v:\n%s", p.name, readyTimeout, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitHealthy polls GET origin/healthz until it answers 200; an early exit fails at once.
func (p *proc) awaitHealthy(origin string) error {
	hc := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(readyTimeout)
	for {
		if resp, err := hc.Get(origin + "/healthz"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if p.exited() {
			return fmt.Errorf("%s exited before becoming healthy (status %d):\n%s", p.name, p.cmd.ProcessState.ExitCode(), p.stderr.String())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy within %v:\n%s", p.name, readyTimeout, p.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type gatewayProc struct {
	*proc
	URL        string
	StorageDir string
}

// startGateway runs a gateway on a free loopback port; the secret travels in the environment.
func startGateway(t *testing.T, upstream, secret string, flags []string, env ...string) *gatewayProc {
	t.Helper()
	dir := t.TempDir()
	args := append([]string{"gateway", "-listen", "127.0.0.1:0", "-storage-dir", dir, "-upstream", upstream}, flags...)
	p := startProc(t, "gateway", args, append(secretEnv(secret), env...)...)
	addr, err := p.awaitBanner("xet-pd gateway listening on ")
	if err != nil {
		t.Fatal(err)
	}
	g := &gatewayProc{proc: p, URL: "http://" + addr, StorageDir: dir}
	if err := p.awaitHealthy(g.URL); err != nil {
		t.Fatal(err)
	}
	return g
}

type peerProc struct {
	*proc
	URL      string
	CacheDir string
}

// startPeer runs a seed on a reserved loopback port it also advertises, retrying if the port is taken meanwhile.
func startPeer(t *testing.T, gatewayURL, secret string, flags []string, env ...string) *peerProc {
	t.Helper()
	for attempt := 1; ; attempt++ {
		addr, dir := reserveAddr(t), t.TempDir()
		args := append([]string{"peer", "-listen", addr, "-advertise", "http://" + addr, "-gateway", gatewayURL, "-cache-dir", dir}, flags...)
		p := startProc(t, "peer "+addr, args, append(secretEnv(secret), env...)...)
		err := p.awaitHealthy("http://" + addr)
		if err == nil {
			return &peerProc{proc: p, URL: "http://" + addr, CacheDir: dir}
		}
		if attempt < 3 && p.exited() && strings.Contains(p.stderr.String(), "address already in use") {
			p.discard()
			continue
		}
		t.Fatal(err)
	}
}

// reserveAddr finds a free loopback address by binding and releasing it.
func reserveAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// runCLI runs the binary to completion.
func runCLI(t *testing.T, args []string, env ...string) (code int, stdout, stderr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), stopTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Env = childEnv(env...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	var exit *exec.ExitError
	if err := cmd.Run(); err != nil && !errors.As(err, &exit) {
		t.Fatalf("run xet-pd: %v", err) // args may carry a secret
	}
	return cmd.ProcessState.ExitCode(), out.String(), errOut.String()
}

var noRedirect = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// request performs one request without following redirects and returns the response and body.
func request(t *testing.T, method, url, body string, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

type result struct {
	resp *http.Response
	body []byte
	err  error
}

// requestAsync performs a GET, redirects followed, and delivers the fully read result.
func requestAsync(t *testing.T, url string, hdr ...string) <-chan result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ingestTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	ch := make(chan result, 1)
	go func() {
		defer cancel()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			ch <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		ch <- result{resp, body, err}
	}()
	return ch
}

// plainGet downloads through the hub route like a non-xet client.
func plainGet(t *testing.T, url string) []byte {
	t.Helper()
	r := <-requestAsync(t, url)
	if r.err != nil || r.resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %v, %v", url, r.resp, r.err)
	}
	return r.body
}

// headReady polls a resolve HEAD until the mirror advertises the xet hash.
func headReady(t *testing.T, resolveURL string) http.Header {
	t.Helper()
	var hdr http.Header
	clustertest.WaitFor(t, ingestTimeout, "ingest of "+resolveURL, func() bool {
		resp, _ := request(t, http.MethodHead, resolveURL, "")
		hdr = resp.Header
		return resp.StatusCode == http.StatusOK && hdr.Get("X-Xet-Hash") != ""
	})
	return hdr
}

// seeded reports whether the gateway stages nothing while each of n live peers holds count xorbs.
func seeded(d apis.PeersResponse, n, count int) bool {
	if d.XorbBytes != 0 || len(d.Peers) != n {
		return false
	}
	for _, p := range d.Peers {
		if p.Holdings != count {
			return false
		}
	}
	return true
}

// reconstructionURLs lists the xorb fetch URLs of the gateway's V1 reconstruction.
func reconstructionURLs(t *testing.T, gw, fileHash string) []string {
	t.Helper()
	resp, body := request(t, http.MethodGet, gw+"/v1/reconstructions/"+fileHash, "")
	var v1 download.ReconstructionResponseV1
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &v1) != nil {
		t.Fatalf("reconstruction of %s = %d %q", fileHash, resp.StatusCode, body)
	}
	var urls []string
	for _, entries := range v1.FetchInfo {
		for _, e := range entries {
			urls = append(urls, e.URL)
		}
	}
	slices.Sort(urls)
	return urls
}

// xetDownload resolves like a hub client and downloads through a fresh xet client and cache.
func xetDownload(t *testing.T, resolveURL string) (xet.FileHash, *download.ReconstructionResponseV2, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ingestTimeout)
	defer cancel()
	repo, path, err := hf.ParseResolveURL(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := hf.NewClient(repo, hf.WithClientOptions(client.WithCache(client.NewCache(t.TempDir(), 0, 0))))
	if err != nil {
		t.Fatal(err)
	}
	rf, err := c.Resolve(ctx, path)
	if err != nil {
		t.Fatalf("resolve %s: %v", resolveURL, err)
	}
	fh := rf.Hash
	v2, err := c.GetReconstructionV2(ctx, fh, nil)
	if err != nil {
		t.Fatalf("reconstruction v2 of %s: %v", fh, err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DownloadFile(ctx, fh, f); err != nil {
		t.Fatalf("xet download of %s: %v", fh, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return fh, v2, out
}

// fetchURLs lists the xorb URLs of a V2 reconstruction, sorted.
func fetchURLs(v2 *download.ReconstructionResponseV2) []string {
	var urls []string
	for _, fetches := range v2.Xorbs {
		for _, f := range fetches {
			urls = append(urls, f.URL)
		}
	}
	slices.Sort(urls)
	return urls
}

// xorbHashes lists the xorbs a local store under base holds, sorted.
func xorbHashes(t *testing.T, base string) []string {
	t.Helper()
	root := filepath.Join(base, "xorbs")
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		if _, err := xet.ParseXorbHash(name); err == nil {
			out = append(out, name)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}
