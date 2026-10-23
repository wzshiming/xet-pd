package e2e

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// The fixture is a pinned public xet-backed file on huggingface.co.
const (
	hfUpstream = "https://huggingface.co"
	hfRepo     = "hf-internal-testing/tiny-random-LlamaForCausalLM"
	hfFile     = "model.safetensors"
	hfRevision = "9fb191250dd56d0ba7ec9785a025ed29c03d5998"
	hfFileSHA  = "49c20f32c6c597480fcaec5df2f86c645eabea765cbea1e67886dbae45e5c992"
	hfFileSize = 4131280
	hfTimeout  = 3 * time.Minute // one hf download
)

// TestHFCLI runs the real hf CLI through the gateway: cold tunnels the origin; warm (fresh cache, bridge refused) can only come from the seeds.
func TestHFCLI(t *testing.T) {
	hfPath := hfBinary(t)
	preflight(t)
	origin := newOriginProxy(t)
	secret := testSecret(t)
	gw := startGateway(t, hfUpstream, secret, []string{"-xorb-cache-size", "1", "-replicas", "2", "-min-seeds", "2"},
		"HTTPS_PROXY="+origin.URL, "HTTP_PROXY="+origin.URL, "https_proxy="+origin.URL, "http_proxy="+origin.URL)
	var peers []*peerProc
	for range 2 {
		peers = append(peers, startPeer(t, gw.URL, secret, []string{"-heartbeat", "100ms"}))
	}
	clustertest.WaitFor(t, readyTimeout, "both seeds to register", func() bool { return len(clustertest.Diag(t, gw.URL).Peers) == 2 })
	front := newFront(t, gw.URL)
	guard := newRefuser(t)
	resolveURL := gw.URL + "/" + hfRepo + "/resolve/" + hfRevision + "/" + hfFile

	coldPath, coldLog := runHF(t, hfPath, front.URL, guard.URL)
	if d, n := fileDigest(t, coldPath); d != hfFileSHA || n != hfFileSize {
		t.Fatalf("cold download: sha256 %s, %d bytes; want %s, %d", d, n, hfFileSHA, hfFileSize)
	}
	fileHash := headReady(t, resolveURL).Get("X-Xet-Hash")
	xorbs := hashesOf(reconstructionURLs(t, gw.URL, fileHash))
	clustertest.WaitFor(t, seedTimeout, "both seeds to hold every xorb and the gateway to evict them", func() bool {
		return seeded(clustertest.Diag(t, gw.URL), 2, len(xorbs))
	})
	for _, p := range peers {
		if got := xorbHashes(t, p.DataDir); !slices.Equal(got, xorbs) {
			t.Fatalf("seed %s stores %v, want %v", p.URL, got, xorbs)
		}
	}
	if left := xorbHashes(t, filepath.Join(gw.DataDir, "data")); len(left) != 0 {
		t.Fatalf("gateway still stores %v after seeding", left)
	}
	if urls := reconstructionURLs(t, gw.URL, fileHash); !fromSeeds(peers, urls) {
		t.Fatalf("reconstruction URLs %v do not all point at seeds", urls)
	}
	coldBytes := origin.bytes.Load()
	if coldBytes < hfFileSize/2 {
		t.Errorf("cold run tunneled %d origin bytes, want at least %d", coldBytes, hfFileSize/2)
	}
	coldHits := front.reset()
	before := clustertest.Diag(t, gw.URL)
	front.noBridge.Store(true)

	warmPath, warmLog := runHF(t, hfPath, front.URL, guard.URL)
	if d, n := fileDigest(t, warmPath); d != hfFileSHA || n != hfFileSize {
		t.Fatalf("warm download: sha256 %s, %d bytes; want %s, %d", d, n, hfFileSHA, hfFileSize)
	}
	warmBytes := origin.bytes.Load() - coldBytes
	warmHits := front.reset()
	after := clustertest.Diag(t, gw.URL)
	if after.XorbGets != before.XorbGets || after.XorbBytes != 0 {
		t.Errorf("gateway xorb gets %+v -> %+v and %d staged bytes during the warm run", before.XorbGets, after.XorbGets, after.XorbBytes)
	}
	if limit := max(int64(256<<10), coldBytes/5); warmBytes >= limit {
		t.Errorf("warm run tunneled %d origin bytes, want under %d", warmBytes, limit)
	}
	if warmHits["bridge"] != 0 || warmHits["xorb"] != 0 || warmHits["reconstruction"] == 0 {
		t.Errorf("warm requests at the front = %v, want a reconstruction and neither bridge nor gateway xorb GET", warmHits)
	}
	if !strings.Contains(warmLog, "Downloading file from Xet Storage") {
		t.Errorf("warm hf log does not announce Xet Storage:\n%s", warmLog)
	}
	if hosts := guard.seen(); len(hosts) != 0 {
		t.Errorf("hf reached beyond loopback: %v", hosts)
	}
	if strings.Contains(coldLog+warmLog, secret) {
		t.Error("hf output echoes the secret")
	}
	if left := xorbHashes(t, filepath.Join(gw.DataDir, "data")); len(left) != 0 {
		t.Errorf("gateway stores %v after the warm run", left)
	}
	if urls := reconstructionURLs(t, gw.URL, fileHash); !fromSeeds(peers, urls) {
		t.Errorf("reconstruction URLs %v after the warm run do not all point at seeds", urls)
	}
	t.Logf("file %s = xorbs %v on both seeds; origin bytes cold %d, warm %d; front requests cold %v, warm %v; gateway xorb gets %+v\nwarm hf stderr:\n%s",
		fileHash, xorbs, coldBytes, warmBytes, coldHits, warmHits, after.XorbGets, warmLog)
}

// hfBinary finds hf and skips unless hf_xet is installed in its Python environment.
func hfBinary(t *testing.T) string {
	t.Helper()
	hfPath, err := exec.LookPath("hf")
	if err != nil {
		t.Skipf("hf CLI not in PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, hfPath, "env")
	cmd.Env = childEnv("HOME="+t.TempDir(), "HF_HOME="+t.TempDir(), "HF_HUB_DISABLE_TELEMETRY=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hf env: %v\n%s", err, out)
	}
	hub, xet := envValue(string(out), "huggingface_hub version"), envValue(string(out), "hf_xet")
	if xet == "" || xet == "N/A" {
		t.Skipf("hf_xet not installed: hf env reports %q", xet)
	}
	t.Logf("%s: huggingface_hub %s, hf_xet %s", hfPath, hub, xet)
	return hfPath
}

// envValue reads one "- key: value" line of hf env.
func envValue(out, key string) string {
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "- "+key+": "); ok {
			return v
		}
	}
	return ""
}

// preflight resolves the fixture through this process's proxy settings: network unavailable or hub outage skips, anything else fails.
func preflight(t *testing.T) {
	t.Helper()
	if err := validateProxyEnvironment(os.Getenv); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, hfUpstream+"/"+hfRepo+"/resolve/"+hfRevision+"/"+hfFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := http.ProxyFromEnvironment(req)
	if err != nil {
		t.Fatal("invalid proxy URL in the environment") // the error quotes it, credentials included
	}
	if proxyURL != nil && proxyURL.Scheme != "http" && proxyURL.Scheme != "https" {
		t.Fatalf("proxy scheme %q is not supported by the counting origin proxy", proxyURL.Scheme)
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		if networkUnavailable(err) {
			t.Skipf("huggingface.co unreachable: %v", err)
		}
		t.Fatalf("upstream HEAD of the fixture: %v", err)
	}
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 500:
		t.Skipf("huggingface.co unhealthy: %s", resp.Status)
	case resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusFound:
		t.Fatalf("upstream HEAD of the fixture = %s", resp.Status)
	}
}

// networkUnavailable reports whether the network never carried the request; anything the far end answered is a failure instead.
func networkUnavailable(err error) bool {
	var dnsErr *net.DNSError
	var opErr *net.OpError
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &dnsErr):
		return true
	case errors.As(err, &opErr):
		return opErr.Timeout() || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH)
	}
	return false
}

// runHF downloads the fixture with throwaway HOME and caches; every proxy variable points at refuser so only loopback is reachable.
func runHF(t *testing.T, hfPath, endpoint, refuser string) (string, string) {
	t.Helper()
	home, local := t.TempDir(), t.TempDir()
	env := []string{
		"HOME=" + home, "HF_HOME=" + filepath.Join(home, "hf"), "HF_XET_CACHE=" + filepath.Join(home, "xet"),
		"HF_ENDPOINT=" + endpoint, "HF_HUB_DISABLE_TELEMETRY=1", "HF_HUB_DISABLE_UPDATE_CHECK=1", "HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_DEBUG=1", // the CLI forces info verbosity otherwise
		"HF_HUB_ETAG_TIMEOUT=60", "HF_HUB_DOWNLOAD_TIMEOUT=60",
	}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
		env = append(env, key+"="+refuser, strings.ToLower(key)+"="+refuser)
	}
	ctx, cancel := context.WithTimeout(t.Context(), hfTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, hfPath, "download", hfRepo, hfFile, "--revision", hfRevision, "--local-dir", local)
	cmd.Env = childEnv(env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("hf download: %v\nstdout:\n%s\nstderr:\n%s", err, &stdout, &stderr)
	}
	return filepath.Join(local, hfFile), stderr.String()
}

// fileDigest returns the sha256 hex digest and size of the file at p.
func fileDigest(t *testing.T, p string) (string, int64) {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return clustertest.SHA256(data), int64(len(data))
}

// hashesOf lists the distinct xorbs the fetch URLs name, sorted.
func hashesOf(urls []string) []string {
	var out []string
	for _, u := range urls {
		out = append(out, path.Base(u))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// fromSeeds reports whether every fetch URL points at one of the seeds.
func fromSeeds(peers []*peerProc, urls []string) bool {
	for _, u := range urls {
		if !slices.ContainsFunc(peers, func(p *peerProc) bool { return strings.HasPrefix(u, p.URL+"/v1/xorbs/") }) {
			return false
		}
	}
	return len(urls) > 0
}

// front is the hub hf talks to: a Host-preserving reverse proxy onto the gateway that tallies routes and can refuse the plain xet-bridge.
type front struct {
	*httptest.Server
	proxy    *httputil.ReverseProxy
	noBridge atomic.Bool

	mu   sync.Mutex
	hits map[string]int
}

func newFront(t *testing.T, gatewayURL string) *front {
	t.Helper()
	target, err := url.Parse(gatewayURL)
	if err != nil {
		t.Fatal(err)
	}
	f := &front{hits: map[string]int{}}
	f.proxy = &httputil.ReverseProxy{
		Transport: &http.Transport{}, // loopback, no proxy environment
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host // the gateway derives link origins from Host
		},
	}
	f.Server = httptest.NewServer(f)
	t.Cleanup(f.Close)
	return f
}

func (f *front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := routeOf(r.URL.Path)
	f.mu.Lock()
	f.hits[route]++
	f.mu.Unlock()
	if route == "bridge" && f.noBridge.Load() {
		http.Error(w, "plain download refused by the test", http.StatusServiceUnavailable)
		return
	}
	f.proxy.ServeHTTP(w, r)
}

// reset returns the requests counted per route so far and starts over.
func (f *front) reset() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	hits := f.hits
	f.hits = map[string]int{}
	return hits
}

// routeOf classifies a hub or CAS request path.
func routeOf(p string) string {
	switch {
	case strings.HasPrefix(p, "/xet-bridge/"):
		return "bridge"
	case strings.Contains(p, "/reconstructions"):
		return "reconstruction"
	case strings.HasPrefix(p, "/v1/xorbs/"):
		return "xorb"
	case strings.HasPrefix(p, "/xet-token/"), strings.Contains(p, "-token/"):
		return "token"
	case strings.Contains(p, "/resolve/"):
		return "resolve"
	}
	return "other"
}

// refuser is the proxy hf gets for anything beyond loopback: it refuses and records each target.
type refuser struct {
	*httptest.Server
	mu    sync.Mutex
	hosts []string
}

func newRefuser(t *testing.T) *refuser {
	g := &refuser{}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.hosts = append(g.hosts, r.Method+" "+r.Host)
		g.mu.Unlock()
		http.Error(w, "egress refused by the test", http.StatusForbidden)
	}))
	t.Cleanup(g.Close)
	return g
}

func (g *refuser) seen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.hosts)
}

// originProxy is the CONNECT proxy the gateway egresses through; it counts the bytes tunnels carry back from the origin.
type originProxy struct {
	*httptest.Server
	bytes   atomic.Int64
	ctx     context.Context // canceled by close to end every tunnel
	cancel  context.CancelFunc
	tunnels sync.WaitGroup
}

func newOriginProxy(t *testing.T) *originProxy {
	t.Helper()
	p := &originProxy{}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.Server = httptest.NewServer(p)
	t.Cleanup(p.close)
	return p
}

// close ends the tunnels, which the server stops tracking once hijacked, and waits for their workers.
func (p *originProxy) close() {
	p.cancel()
	p.Close()
	p.tunnels.Wait()
}

func (p *originProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
		return
	}
	up, err := dialOrigin(r.Context(), r.Host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = up.Close() }()
	p.tunnels.Add(1) // before Hijack: Server.Close still waits for this request until then
	defer p.tunnels.Done()
	client, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer func() { _ = client.Close() }()
	stop := context.AfterFunc(p.ctx, func() { _ = client.Close(); _ = up.Close() })
	defer stop()
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		_, _ = io.Copy(up, rw)
		_ = up.Close()
	}()
	defer func() {
		_ = client.Close()
		_ = up.Close()
		<-copied
	}()
	_, _ = io.Copy(countingWriter{n: &p.bytes, w: client}, up)
}

type countingWriter struct {
	n *atomic.Int64
	w io.Writer
}

func (c countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n.Add(int64(n))
	return n, err
}

// dialOrigin connects to the CONNECT target through this process's proxy settings.
func dialOrigin(ctx context.Context, target string) (net.Conn, error) {
	if err := validateProxyEnvironment(os.Getenv); err != nil {
		return nil, err
	}
	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "https", Host: target}})
	if err != nil {
		return nil, errors.New("invalid proxy URL in the environment") // the error quotes it, credentials included
	}
	return dialProxy(ctx, target, proxyURL, nil)
}

func validateProxyEnvironment(getenv func(string) string) error {
	raw := cmp.Or(getenv("HTTPS_PROXY"), getenv("https_proxy"))
	if raw == "" {
		return nil
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || parsed.Opaque != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return errors.New("invalid HTTP proxy configuration")
	}
	if strings.HasSuffix(parsed.Host, ":"+parsed.Port()) {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return errors.New("invalid HTTP proxy configuration")
		}
	}
	return nil
}

// dialProxy dials target directly or through one CONNECT; proxy credentials travel in that request alone, tlsConfig is for the proxy's own TLS.
func dialProxy(ctx context.Context, target string, proxyURL *url.URL, tlsConfig *tls.Config) (net.Conn, error) {
	d := &net.Dialer{Timeout: 15 * time.Second}
	if proxyURL == nil {
		return d.DialContext(ctx, "tcp", target)
	}
	port := "80"
	switch proxyURL.Scheme {
	case "http":
	case "https":
		port = "443"
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q", proxyURL.Scheme)
	}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(proxyURL.Hostname(), cmp.Or(proxyURL.Port(), port)))
	if err != nil {
		return nil, err
	}
	tcpConnection := conn
	stop := context.AfterFunc(ctx, func() { _ = tcpConnection.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if proxyURL.Scheme == "https" {
		cfg := tlsConfig.Clone()
		if cfg == nil {
			cfg = &tls.Config{}
		}
		cfg.ServerName = proxyURL.Hostname()
		tc := tls.Client(conn, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tc
	}
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: target}, Host: target, Header: http.Header{}}
	if u := proxyURL.User; u != nil {
		pw, _ := u.Password()
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.Username()+":"+pw)))
	}
	br := bufio.NewReader(conn)
	var resp *http.Response
	if err = req.Write(conn); err == nil {
		resp, err = http.ReadResponse(br, req) // Body is left alone: it would be the tunnel itself
	}
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("proxy CONNECT: %s", resp.Status)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return prefixConn{r: br, Conn: conn}, nil
}

// prefixConn keeps reading through the reader that parsed the CONNECT reply, so over-read tunnel bytes are not lost.
type prefixConn struct {
	r io.Reader
	net.Conn
}

func (c prefixConn) Read(b []byte) (int, error) { return c.r.Read(b) }

func TestProxyEnvironmentValidation(t *testing.T) {
	for _, raw := range []string{"http://", "https://", "http://:8080", "http://proxy:", "http://proxy:65536", "http://alice:secret@proxy/%zz", "socks5://proxy:1080"} {
		getenv := func(key string) string {
			if key == "HTTPS_PROXY" {
				return raw
			}
			return ""
		}
		err := validateProxyEnvironment(getenv)
		if err == nil || networkUnavailable(err) {
			t.Fatalf("malformed proxy must fail configuration validation, got %v", err)
		}
		if strings.Contains(err.Error(), "alice") || strings.Contains(err.Error(), "secret") {
			t.Fatal("proxy error exposes credentials")
		}
	}
	for _, raw := range []string{"", "proxy:8080", "http://proxy", "https://proxy:443", "http://alice:secret@proxy:8080", "http://[::1]:8080"} {
		getenv := func(key string) string {
			if key == "https_proxy" {
				return raw
			}
			return ""
		}
		if err := validateProxyEnvironment(getenv); err != nil {
			t.Fatalf("valid proxy rejected: %v", err)
		}
	}
	if err := validateProxyEnvironment(func(key string) string {
		if key == "HTTPS_PROXY" {
			return "http://proxy:8080"
		}
		return "http://"
	}); err != nil {
		t.Fatalf("uppercase proxy must take precedence: %v", err)
	}
}

func TestNetworkUnavailable(t *testing.T) {
	ctx := t.Context()
	stalled, err := net.Listen("tcp", "127.0.0.1:0") // never accepts: the connect succeeds, then silence
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stalled.Close() })
	expired, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()
	tlsSrv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsSrv.Close)
	head := func(c *http.Client, ctx context.Context, url string) error {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			t.Fatalf("HEAD %s = %s, want a transport error", url, resp.Status)
		}
		return err
	}
	wrap := func(err error) error { return &url.Error{Op: "Head", URL: hfUpstream, Err: err} }
	op := func(op string, err error) error { return &net.OpError{Op: op, Net: "tcp", Err: err} }
	sys := func(errno syscall.Errno) error { return &os.SyscallError{Syscall: "connect", Err: errno} }
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"refused", head(noRedirect, ctx, "http://"+reserveAddr(t)+"/"), true},
		{"client timeout", head(&http.Client{Timeout: 50 * time.Millisecond}, ctx, "http://"+stalled.Addr().String()+"/"), true},
		{"context deadline", head(noRedirect, expired, "http://"+stalled.Addr().String()+"/"), true},
		{"malformed response", head(noRedirect, ctx, "http://"+rawServer(t, "garbage\r\n\r\n")+"/"), false},
		{"untrusted certificate", head(noRedirect, ctx, tlsSrv.URL), false},
		{"dns", wrap(op("dial", &net.DNSError{Err: "no such host", Name: "huggingface.co", IsNotFound: true})), true},
		{"host unreachable", wrap(op("dial", sys(syscall.EHOSTUNREACH))), true},
		{"network unreachable", wrap(op("dial", sys(syscall.ENETUNREACH))), true},
		{"dial timeout", wrap(op("dial", os.ErrDeadlineExceeded)), true},
		{"proxy refused", wrap(op("proxyconnect", op("dial", sys(syscall.ECONNREFUSED)))), true},
		{"reset", wrap(op("read", &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET})), false},
		{"eof", wrap(io.ErrUnexpectedEOF), false},
		{"proxy status", wrap(errors.New("Forbidden")), false},
		{"tls record", wrap(tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		if got := networkUnavailable(tc.err); got != tc.want {
			t.Errorf("%s: networkUnavailable(%v) = %t, want %t", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestDialProxyTunnels pins credentials reaching the proxy only, tunnel byte counting, HTTPS proxy verification and refusals.
func TestDialProxyTunnels(t *testing.T) {
	origin, take := startOrigin(t, "pong\n")
	proxy := newOriginProxy(t)
	const user, pass = "alice", "s3cret"
	var mu sync.Mutex
	var auth []string
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Proxy-Authorization")
		mu.Lock()
		auth = append(auth, got)
		mu.Unlock()
		if u, p, ok := (&http.Request{Header: http.Header{"Authorization": {got}}}).BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("Proxy-Authenticate", `Basic realm="test"`)
			http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(gate.Close)
	// seen returns and clears the Proxy-Authorization values the gate received.
	seen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		s := auth
		auth = nil
		return s
	}
	basic := func(password string) []string {
		return []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))}
	}
	tlsGate := httptest.NewTLSServer(proxy)
	t.Cleanup(tlsGate.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tlsGate.Certificate())
	gateHost := strings.TrimPrefix(gate.URL, "http://")
	// tunnel opens a tunnel to origin through proxyURL, sends one line and returns the answer.
	tunnel := func(t *testing.T, proxyURL string, cfg *tls.Config) (string, error) {
		t.Helper()
		u, err := url.Parse(proxyURL)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		conn, err := dialProxy(ctx, origin, u, cfg)
		if err != nil {
			return "", err
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.WriteString(conn, "ping\n"); err != nil {
			t.Fatal(err)
		}
		reply, err := io.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
		return string(reply), nil
	}
	credentialed := func(scheme, password, host string) string {
		return (&url.URL{Scheme: scheme, User: url.UserPassword(user, password), Host: host}).String()
	}
	// leaks fails when the proxy credentials show up in err.
	leaks := func(t *testing.T, err error) {
		t.Helper()
		if s := err.Error(); strings.Contains(s, pass) || strings.Contains(s, user) {
			t.Errorf("error exposes the proxy credentials: %v", err)
		}
	}

	t.Run("http basic auth", func(t *testing.T) {
		reply, err := tunnel(t, credentialed("http", pass, gateHost), nil)
		if err != nil || reply != "pong\n" {
			t.Fatalf("tunnel = %q, %v; want the origin's answer", reply, err)
		}
		if got := seen(); !slices.Equal(got, basic(pass)) {
			t.Errorf("proxy saw Proxy-Authorization %q, want %q", got, basic(pass))
		}
		if got := take(); got != "ping\n" {
			t.Fatalf("origin received %q, want only the tunnel payload", got)
		}
		if n := proxy.bytes.Load(); n != int64(len("pong\n")) {
			t.Errorf("counted %d origin bytes, want %d", n, len("pong\n"))
		}
	})
	t.Run("https", func(t *testing.T) {
		before := proxy.bytes.Load()
		reply, err := tunnel(t, tlsGate.URL, &tls.Config{RootCAs: roots})
		if err != nil || reply != "pong\n" {
			t.Fatalf("tunnel = %q, %v; want the origin's answer", reply, err)
		}
		if got := take(); got != "ping\n" {
			t.Fatalf("origin received %q, want only the tunnel payload", got)
		}
		if n := proxy.bytes.Load() - before; n != int64(len("pong\n")) {
			t.Errorf("counted %d origin bytes, want %d", n, len("pong\n"))
		}
	})
	t.Run("https untrusted", func(t *testing.T) {
		_, err := tunnel(t, tlsGate.URL, nil)
		var verify *tls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("tunnel through an untrusted HTTPS proxy = %v, want a certificate verification error", err)
		}
		if networkUnavailable(err) {
			t.Error("a certificate failure counts as network unavailable")
		}
	})
	t.Run("rejected", func(t *testing.T) {
		_, err := tunnel(t, credentialed("http", "wrong-"+pass, gateHost), nil)
		if err == nil || !strings.Contains(err.Error(), "407") {
			t.Fatalf("tunnel with wrong credentials = %v, want the 407 status", err)
		}
		if got := seen(); !slices.Equal(got, basic("wrong-"+pass)) {
			t.Errorf("proxy saw Proxy-Authorization %q, want %q", got, basic("wrong-"+pass))
		}
		leaks(t, err)
		if networkUnavailable(err) {
			t.Error("a proxy refusal counts as network unavailable")
		}
	})
	t.Run("malformed", func(t *testing.T) {
		_, err := tunnel(t, credentialed("http", pass, rawServer(t, "garbage\r\n\r\n")), nil)
		if err == nil {
			t.Fatal("tunnel through a proxy answering garbage succeeded")
		}
		leaks(t, err)
		if networkUnavailable(err) {
			t.Error("a malformed reply counts as network unavailable")
		}
	})
	t.Run("socks", func(t *testing.T) {
		_, err := tunnel(t, credentialed("socks5", pass, gateHost), nil)
		if err == nil || !strings.Contains(err.Error(), "socks5") {
			t.Fatalf("tunnel through a SOCKS proxy = %v, want an unsupported-scheme error", err)
		}
		leaks(t, err)
		if got := seen(); len(got) != 0 {
			t.Errorf("the proxy was contacted anyway: %q", got)
		}
	})
	t.Run("refused", func(t *testing.T) {
		_, err := tunnel(t, credentialed("http", pass, reserveAddr(t)), nil)
		if !networkUnavailable(err) {
			t.Fatalf("tunnel through a dead proxy = %v, want network unavailable", err)
		}
		leaks(t, err)
	})
	t.Run("close ends tunnels", func(t *testing.T) {
		p := newOriginProxy(t)
		u, err := url.Parse(p.URL)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := dialProxy(t.Context(), rawServer(t, ""), u, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		done := make(chan struct{})
		go func() {
			p.close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("close did not end the open tunnel")
		}
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Error("tunnel still open after close")
		}
	})
}

// startOrigin is the tunnel target: it records each connection's first line, answers reply and hangs up.
func startOrigin(t *testing.T, reply string) (addr string, take func() string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var mu sync.Mutex
	var got strings.Builder
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				line, err := bufio.NewReader(c).ReadString('\n')
				mu.Lock()
				got.WriteString(line)
				mu.Unlock()
				if err == nil {
					_, _ = io.WriteString(c, reply)
				}
			}()
		}
	}()
	return ln.Addr().String(), func() string {
		mu.Lock()
		defer mu.Unlock()
		s := got.String()
		got.Reset()
		return s
	}
}

// rawServer answers every connection's request head with reply verbatim, then hangs up.
func rawServer(t *testing.T, reply string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				tp := textproto.NewReader(bufio.NewReader(c))
				if _, err := tp.ReadLine(); err != nil {
					return
				}
				if _, err := tp.ReadMIMEHeader(); err != nil {
					return
				}
				_, _ = io.WriteString(c, reply)
			}()
		}
	}()
	return ln.Addr().String()
}
