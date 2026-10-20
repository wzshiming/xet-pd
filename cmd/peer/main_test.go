package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
	"github.com/wzshiming/xet-pd/peer"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) // the peer warns when Close cancels its first announce
	os.Exit(m.Run())
}

func noEnv(string) string { return "" }

func env(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return func(k string) string { return m[k] }
}

func TestParsePeerDefaults(t *testing.T) {
	// HF_TOKEN and the gateway token variable mean nothing to a peer.
	getenv := env("HF_TOKEN", "tok")
	listen, cfg, err := parse([]string{"-advertise", "http://p:1", "-gateway", "http://g:2"}, getenv, io.Discard)
	want := peer.Config{Advertise: "http://p:1", Gateway: "http://g:2", CacheDir: "./xet-pd-peer", MaxPulls: 8, PullTimeout: 10 * time.Minute, Heartbeat: 10 * time.Second}
	if err != nil || listen != ":8081" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":8081", want)
	}
}

func TestParsePeerEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9001", "XET_PD_ADVERTISE", "http://p:1", "XET_PD_GATEWAY", "http://g:2", "XET_PD_CACHE_DIR", "/cache",
		"XET_PD_XORB_CACHE_SIZE", "2GiB", "XET_PD_MAX_PULLS", "3", "XET_PD_PULL_TIMEOUT", "1m", "XET_PD_HEARTBEAT", "2s", "XET_PD_SECRET", "sec")
	listen, cfg, err := parse(nil, getenv, io.Discard)
	want := peer.Config{Advertise: "http://p:1", Gateway: "http://g:2", CacheDir: "/cache", XorbCacheSize: 2 << 30, MaxPulls: 3, PullTimeout: time.Minute, Heartbeat: 2 * time.Second, Secret: "sec"}
	if err != nil || listen != ":9001" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":9001", want)
	}
}

func TestParsePeerFlagsOverrideEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9001", "XET_PD_ADVERTISE", "http://p:1", "XET_PD_GATEWAY", "http://g:2", "XET_PD_CACHE_DIR", "/cache",
		"XET_PD_XORB_CACHE_SIZE", "bogus", "XET_PD_MAX_PULLS", "x", "XET_PD_PULL_TIMEOUT", "y", "XET_PD_HEARTBEAT", "z", "XET_PD_SECRET", "sec")
	args := []string{"-listen", ":7001", "-advertise", "http://fp:1", "-gateway", "http://fg:2", "-cache-dir", "/flag",
		"-xorb-cache-size", "512MiB", "-max-pulls", "2", "-pull-timeout", "30s", "-heartbeat", "1s", "-secret", "fsec"}
	listen, cfg, err := parse(args, getenv, io.Discard)
	want := peer.Config{Advertise: "http://fp:1", Gateway: "http://fg:2", CacheDir: "/flag", XorbCacheSize: 512 << 20, MaxPulls: 2, PullTimeout: 30 * time.Second, Heartbeat: time.Second, Secret: "fsec"}
	if err != nil || listen != ":7001" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":7001", want)
	}
}

func TestParsePeerErrors(t *testing.T) {
	both := []string{"-advertise", "http://p:1", "-gateway", "http://g:2"}
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"missing advertise", []string{"-gateway", "http://g:2"}, noEnv, "advertise is required"},
		{"missing gateway", []string{"-advertise", "http://p:1"}, noEnv, "gateway is required"},
		{"positional argument", append(both, "extra"), noEnv, `unexpected argument "extra"`},
		{"bad env int", both, env("XET_PD_MAX_PULLS", "x"), "XET_PD_MAX_PULLS"},
		{"bad env size", both, env("XET_PD_XORB_CACHE_SIZE", "1.5GiB"), "XET_PD_XORB_CACHE_SIZE"},
		{"bad env duration", both, env("XET_PD_HEARTBEAT", "soon"), "XET_PD_HEARTBEAT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parse(tt.args, tt.env, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parse error = %v, want mention of %q", err, tt.want)
			}
		})
	}
}

func TestParsePeerHelp(t *testing.T) {
	var help bytes.Buffer
	_, _, err := parse([]string{"--help"}, noEnv, &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"Usage: peer [flags]", "-advertise", "[env XET_PD_ADVERTISE]", "-pull-timeout", "(default 10m0s)", "[env XET_PD_SECRET]"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, help.String())
		}
	}
	for _, unwanted := range []string{"HF_TOKEN", "upstream"} {
		if strings.Contains(help.String(), unwanted) {
			t.Errorf("peer help mentions %q:\n%s", unwanted, help.String())
		}
	}
}

func TestParsePeerSecretFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte(" s3cr3t \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	blank := filepath.Join(t.TempDir(), "blank")
	if err := os.WriteFile(blank, []byte("  \n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, cfg, err := parse([]string{"-advertise", "http://p:1", "-gateway", "http://g:2", "-secret-file", file}, noEnv, io.Discard)
	if err != nil || cfg.Secret != "s3cr3t" {
		t.Fatalf("-secret-file: Secret = %q, %v; want s3cr3t", cfg.Secret, err)
	}
	_, cfg, err = parse([]string{"-advertise", "http://p:1", "-gateway", "http://g:2"}, env("XET_PD_SECRET_FILE", file), io.Discard)
	if err != nil || cfg.Secret != "s3cr3t" {
		t.Fatalf("XET_PD_SECRET_FILE: Secret = %q, %v; want s3cr3t", cfg.Secret, err)
	}
	_, _, err = parse([]string{"-advertise", "http://p:1", "-gateway", "http://g:2"}, env("XET_PD_SECRET_FILE", blank), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "secret file is empty") {
		t.Fatalf("blank XET_PD_SECRET_FILE: err = %v, want mention of %q", err, "secret file is empty")
	}
}

func TestRunUsage(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		code           int
		stdout, stderr string // substrings, "" means empty
	}{
		{name: "--help", args: []string{"--help"}, code: 0, stdout: "Usage: peer [flags]"},
		{name: "missing advertise", args: nil, code: 2, stderr: "peer: advertise is required\nRun \"peer -h\" for flags.\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := command.Run(context.Background(), tt.args, noEnv, &stdout, &stderr); code != tt.code {
				t.Fatalf("Run = %d, want %d; stderr %q", code, tt.code, stderr.String())
			}
			if (stdout.Len() > 0) != (tt.stdout != "") || !strings.Contains(stdout.String(), tt.stdout) {
				t.Fatalf("stdout %q, want %q", stdout.String(), tt.stdout)
			}
			if (stderr.Len() > 0) != (tt.stderr != "") || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("stderr %q, want %q", stderr.String(), tt.stderr)
			}
		})
	}
}

// fakeGateway answers peer announces and records them.
type fakeGateway struct {
	*httptest.Server
	mu        sync.Mutex
	announces int
	auth      string
}

func newFakeGateway(t *testing.T) *fakeGateway {
	g := &fakeGateway{}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != apis.AnnouncePath {
			http.NotFound(w, r)
			return
		}
		g.mu.Lock()
		g.announces++
		g.auth = r.Header.Get("Authorization")
		g.mu.Unlock()
		_, _ = io.WriteString(w, `{"ttl":30}`)
	}))
	t.Cleanup(g.Close)
	return g
}

func (g *fakeGateway) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.announces
}

// chanWriter hands every write to the test as it happens.
type chanWriter chan string

func (c chanWriter) Write(p []byte) (int, error) {
	c <- string(p)
	return len(p), nil
}

// startRun runs the command in the background; stderr may be read once code has been received.
func startRun(ctx context.Context, args []string, getenv func(string) string) (chanWriter, *bytes.Buffer, <-chan int) {
	stdout, stderr, code := make(chanWriter, 8), &bytes.Buffer{}, make(chan int, 1)
	go func() { code <- command.Run(ctx, args, getenv, stdout, stderr) }()
	return stdout, stderr, code
}

func await[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func TestRunListenFailureClosesService(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()
	gw := newFakeGateway(t)
	args := []string{"-listen", taken.Addr().String(), "-advertise", "http://127.0.0.1:1", "-gateway", gw.URL, "-cache-dir", t.TempDir(), "-heartbeat", "20ms"}
	stdout, stderr, code := startRun(context.Background(), args, noEnv)
	if got := await(t, "exit", code); got != 1 {
		t.Fatalf("Run = %d, want 1; stderr %q", got, stderr.String())
	}
	if len(stdout) != 0 || !strings.Contains(stderr.String(), "address already in use") {
		t.Fatalf("stdout %d writes, stderr %q", len(stdout), stderr.String())
	}
	// A closed peer announces no more; an unclosed one would announce every 20ms.
	time.Sleep(50 * time.Millisecond)
	n := gw.count()
	time.Sleep(200 * time.Millisecond)
	if got := gw.count(); got != n {
		t.Fatalf("announces kept coming after exit: %d -> %d", n, got)
	}
}

func TestRunPeerServesUntilCancelled(t *testing.T) {
	gw := newFakeGateway(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := []string{"-listen", "127.0.0.1:0", "-advertise", "http://127.0.0.1:1", "-gateway", gw.URL, "-cache-dir", t.TempDir(), "-secret", "hunter2"}
	stdout, stderr, code := startRun(ctx, args, noEnv)
	banner := await(t, "banner", stdout)
	addr, ok := strings.CutPrefix(strings.TrimSpace(banner), "peer listening on ")
	if !ok {
		t.Fatalf("banner %q", banner)
	}
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	clustertest.WaitFor(t, 10*time.Second, "first announce", func() bool { return gw.count() > 0 })
	cancel()
	if got := await(t, "exit", code); got != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", got, stderr.String())
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("listener still open after exit")
	}
	if strings.Contains(stderr.String(), "hunter2") || strings.Contains(banner, "hunter2") {
		t.Fatalf("secret echoed: %q %q", stderr.String(), banner)
	}
	gw.mu.Lock()
	defer gw.mu.Unlock()
	if gw.auth != "Bearer hunter2" {
		t.Fatalf("announce auth %q", gw.auth)
	}
}
