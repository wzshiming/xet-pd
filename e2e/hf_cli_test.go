//go:build e2e_hf

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Pinned tiny public xet-backed file on the real huggingface.co.
const (
	hfRepo     = "hf-internal-testing/tiny-random-LlamaForCausalLM"
	hfFile     = "model.safetensors"
	hfRevision = "9fb191250dd56d0ba7ec9785a025ed29c03d5998"
	hfFileSHA  = "49c20f32c6c597480fcaec5df2f86c645eabea765cbea1e67886dbae45e5c992"
	hfFileSize = 4131280
)

// countingWriter adds every byte written through it to an atomic counter.
type countingWriter struct {
	n *atomic.Int64
	w io.Writer
}

func (cw countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n.Add(int64(n))
	return n, err
}

// countingProxy is a forward HTTP proxy that counts origin→client bytes. It
// is the only interception point seeing all agent egress: the agent fetches
// data-plane content from absolute CAS/CDN URLs, so a reverse proxy on
// -upstream would never see that traffic.
type countingProxy struct {
	originBytes atomic.Int64
}

func (p *countingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.tunnel(w, r)
		return
	}
	// Trivial absolute-URI proxying for plain-http targets (normally unused).
	r.RequestURI = ""
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(countingWriter{n: &p.originBytes, w: w}, resp.Body)
}

func (p *countingProxy) tunnel(w http.ResponseWriter, r *http.Request) {
	up, err := dialOrigin(r.Host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = up.Close()
		http.Error(w, "cannot hijack", http.StatusInternalServerError)
		return
	}
	client, bufrw, err := hj.Hijack()
	if err != nil {
		_ = up.Close()
		return
	}
	_, _ = bufrw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
	_ = bufrw.Flush()
	go func() {
		_, _ = io.Copy(up, bufrw)
		_ = up.Close()
	}()
	_, _ = io.Copy(countingWriter{n: &p.originBytes, w: client}, up)
	_ = client.Close()
}

// dialOrigin reaches host:port honoring this process's own proxy environment
// (nested CONNECT), so the test also runs on machines without direct egress.
func dialOrigin(host string) (net.Conn, error) {
	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "https", Host: host}})
	if err != nil || proxyURL == nil {
		return net.DialTimeout("tcp", host, 15*time.Second)
	}
	conn, err := net.DialTimeout("tcp", proxyURL.Host, 15*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", host, host); err != nil {
		_ = conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	// Parse the CONNECT reply by hand: http.ReadResponse would model a body
	// and its Close would block draining the not-yet-started tunnel.
	status, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.Contains(status, " 200 ") {
		_ = conn.Close()
		return nil, fmt.Errorf("upstream proxy CONNECT %s: %s", host, strings.TrimSpace(status))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	_ = conn.SetDeadline(time.Time{})
	// br may have over-read tunnel bytes; keep reading through it.
	return prefixConn{br, conn}, nil
}

// prefixConn replays bytes the CONNECT handshake over-read before the conn.
type prefixConn struct {
	r io.Reader
	net.Conn
}

func (c prefixConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// runHF downloads the pinned file through the agent with the given fresh
// HF_HOME and returns the local path the CLI printed.
func runHF(t *testing.T, hfPath, agentURL, hfHome string, extraEnv []string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, hfPath, "download", hfRepo, hfFile, "--revision", hfRevision)
	cmd.Env = append(childEnv(),
		"HF_ENDPOINT="+agentURL,
		"HF_HOME="+hfHome,
		"HF_HUB_DISABLE_TELEMETRY=1",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("hf download (HF_HOME=%s) stdout:\n%s\nstderr:\n%s", hfHome, stdout.String(), stderr.String())
		}
	})
	if err := cmd.Run(); err != nil {
		t.Fatalf("hf download: %v", err)
	}
	out := strings.TrimSpace(stdout.String())
	// hf prints the location last, either bare or as "  path: /...".
	path := strings.TrimSpace(out[strings.LastIndexByte(out, '\n')+1:])
	path = strings.TrimSpace(strings.TrimPrefix(path, "path:"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("hf download printed %q, not a readable file: %v", path, err)
	}
	return path
}

// fileDigest returns the sha256 hex digest and size of the file at path.
func fileDigest(t *testing.T, path string) (string, int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

// TestHFCLI drives the unmodified hf CLI through a real agent against the
// real huggingface.co: a cold download must tunnel roughly the file's bytes
// from the origin, a warm re-download (fresh client cache, same agent
// storage) must not move the data again.
func TestHFCLI(t *testing.T) {
	hfPath, err := exec.LookPath("hf")
	if err != nil {
		t.Skipf("hf CLI not in PATH: %v", err)
	}
	probe := &http.Client{Timeout: 5 * time.Second}
	resp, err := probe.Head("https://huggingface.co")
	if err != nil {
		t.Skipf("no outbound network, huggingface.co unreachable: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 500 {
		t.Skipf("huggingface.co unhealthy: probe status %s", resp.Status)
	}

	for _, mode := range []struct {
		name  string
		extra []string
	}{
		{"xet", nil},
		{"no-xet", []string{"HF_HUB_DISABLE_XET=1"}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			// Separate agent per mode: agent storage is content-keyed, so a
			// shared agent would make the second mode's cold run warm.
			proxy := &countingProxy{}
			proxySrv := httptest.NewServer(proxy)
			t.Cleanup(proxySrv.Close)
			ag := startAgentEnv(t, "https://huggingface.co", []string{
				"HTTPS_PROXY=" + proxySrv.URL,
				"HTTP_PROXY=" + proxySrv.URL,
			})

			coldStart := proxy.originBytes.Load()
			coldPath := runHF(t, hfPath, ag.URL, t.TempDir(), mode.extra)
			coldDelta := proxy.originBytes.Load() - coldStart
			coldSHA, coldSize := fileDigest(t, coldPath)
			if coldSHA != hfFileSHA {
				t.Errorf("cold sha256 = %s, want %s", coldSHA, hfFileSHA)
			}
			if coldSize != hfFileSize {
				t.Errorf("cold size = %d, want %d", coldSize, hfFileSize)
			}
			// Xorbs travel LZ4-compressed, so wire bytes may undershoot the
			// logical size; half the file is still decisively origin data.
			if coldDelta < hfFileSize/2 {
				t.Errorf("cold run tunneled %d origin bytes, want >= %d", coldDelta, hfFileSize/2)
			}

			warmStart := proxy.originBytes.Load()
			warmPath := runHF(t, hfPath, ag.URL, t.TempDir(), mode.extra)
			warmDelta := proxy.originBytes.Load() - warmStart
			warmSHA, _ := fileDigest(t, warmPath)
			if warmSHA != coldSHA {
				t.Errorf("warm sha256 = %s, want %s", warmSHA, coldSHA)
			}
			t.Logf("origin bytes tunneled: cold=%d warm=%d (file %d)", coldDelta, warmDelta, hfFileSize)
			// Hub API chatter (revision revalidation, token minting, TLS
			// handshakes) legitimately tunnels a little on a warm run; the
			// bound stays far below the data volume.
			if limit := max(int64(256<<10), coldDelta/5); warmDelta >= limit {
				t.Errorf("warm run tunneled %d origin bytes, want < %d (agent must serve from local storage)", warmDelta, limit)
			}
		})
	}
}
