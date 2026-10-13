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
	"github.com/wzshiming/xet-pd/gateway"
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

func TestParseSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr string
	}{
		{in: "0", want: 0},
		{in: "8589934592", want: 8 << 30},
		{in: "8GiB", want: 8 << 30},
		{in: "64MiB", want: 64 << 20},
		{in: "3KiB", want: 3 << 10},
		{in: "9223372036854775807", want: 1<<63 - 1},
		{in: "8589934591GiB", want: 8589934591 << 30},
		{in: "8589934592GiB", wantErr: "overflow"},
		{in: "9223372036854775808", wantErr: "overflow"},
		{in: "99999999999999999999", wantErr: "overflow"},
		{in: "-1", wantErr: "suffix"},
		{in: "-1GiB", wantErr: "suffix"},
		{in: "", wantErr: "suffix"},
		{in: "GiB", wantErr: "suffix"},
		{in: "1.5GiB", wantErr: "suffix"},
		{in: "8gib", wantErr: "suffix"},
		{in: "8GB", wantErr: "suffix"},
		{in: "8 GiB", wantErr: "suffix"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseSize(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseSize(%q) = %d, %v; want error mentioning %q", tt.in, got, err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("parseSize(%q) = %d, %v; want %d", tt.in, got, err, tt.want)
			}
		})
	}
}

func TestSizeValueString(t *testing.T) {
	for _, tt := range []struct {
		n    int64
		want string
	}{{0, "0"}, {8 << 30, "8GiB"}, {64 << 20, "64MiB"}, {3 << 10, "3KiB"}, {1500, "1500"}, {3<<30 + 1<<20, "3073MiB"}} {
		v := sizeValue(tt.n)
		if got := v.String(); got != tt.want {
			t.Errorf("sizeValue(%d).String() = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestParseGatewayDefaults(t *testing.T) {
	listen, cfg, err := parseGateway(nil, noEnv, io.Discard)
	want := gateway.Config{StorageDir: "./xet-pd-gateway", Upstream: "https://huggingface.co", PeerTTL: 30 * time.Second, Replicas: 2, MinSeeds: 1, XorbCacheSize: 8 << 30}
	if err != nil || listen != ":8080" || cfg != want {
		t.Fatalf("parseGateway() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":8080", want)
	}
}

func TestParseGatewayEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9000", "XET_PD_STORAGE_DIR", "/data", "XET_PD_UPSTREAM", "http://hub:8000",
		"XET_PD_UPSTREAM_TOKEN", "tok", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "45s", "XET_PD_REPLICAS", "3", "XET_PD_MIN_SEEDS", "2",
		"XET_PD_XORB_CACHE_SIZE", "64MiB")
	listen, cfg, err := parseGateway(nil, getenv, io.Discard)
	want := gateway.Config{StorageDir: "/data", Upstream: "http://hub:8000", UpstreamToken: "tok", SigningKey: "sign", ExternalURL: "http://gw", Secret: "sec", PeerTTL: 45 * time.Second, Replicas: 3, MinSeeds: 2, XorbCacheSize: 64 << 20}
	if err != nil || listen != ":9000" || cfg != want {
		t.Fatalf("parseGateway() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":9000", want)
	}
}

// Flags win over the environment, and an unparsable environment value is ignored once its flag is given.
func TestParseGatewayFlagsOverrideEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9000", "XET_PD_STORAGE_DIR", "/data", "XET_PD_UPSTREAM", "http://hub:8000",
		"XET_PD_UPSTREAM_TOKEN", "tok", "HF_TOKEN", "hf", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "bogus", "XET_PD_REPLICAS", "x", "XET_PD_MIN_SEEDS", "1.5",
		"XET_PD_XORB_CACHE_SIZE", "-1")
	args := []string{"-listen", ":7000", "-storage-dir", "/flag", "-upstream", "http://flag", "-upstream-token", "ftok",
		"-signing-key", "fsign", "-external-url", "http://fgw", "-secret", "fsec", "-peer-ttl", "5s", "-replicas", "4",
		"-min-seeds", "3", "-xorb-cache-size", "1GiB"}
	listen, cfg, err := parseGateway(args, getenv, io.Discard)
	want := gateway.Config{StorageDir: "/flag", Upstream: "http://flag", UpstreamToken: "ftok", SigningKey: "fsign", ExternalURL: "http://fgw", Secret: "fsec", PeerTTL: 5 * time.Second, Replicas: 4, MinSeeds: 3, XorbCacheSize: 1 << 30}
	if err != nil || listen != ":7000" || cfg != want {
		t.Fatalf("parseGateway() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":7000", want)
	}
}

func TestParseGatewayUpstreamToken(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"hf token fallback", nil, env("HF_TOKEN", "hf"), "hf"},
		{"xet-pd token first", nil, env("HF_TOKEN", "hf", "XET_PD_UPSTREAM_TOKEN", "tok"), "tok"},
		{"flag wins", []string{"-upstream-token", "ftok"}, env("HF_TOKEN", "hf", "XET_PD_UPSTREAM_TOKEN", "tok"), "ftok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cfg, err := parseGateway(tt.args, tt.env, io.Discard)
			if err != nil || cfg.UpstreamToken != tt.want {
				t.Fatalf("UpstreamToken = %q, %v; want %q", cfg.UpstreamToken, err, tt.want)
			}
		})
	}
}

func TestParseGatewayErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"positional argument", []string{"extra"}, noEnv, `unexpected argument "extra"`},
		{"unknown flag", []string{"-bogus"}, noEnv, "flag provided but not defined: -bogus"},
		{"empty upstream", []string{"-upstream", ""}, noEnv, "upstream is required"},
		{"bad duration flag", []string{"-peer-ttl", "abc"}, noEnv, "invalid value"},
		{"bad size flag", []string{"-xorb-cache-size", "8GB"}, noEnv, "suffix"},
		{"bad env duration", nil, env("XET_PD_PEER_TTL", "abc"), `XET_PD_PEER_TTL="abc": parse error`},
		{"bad env int", nil, env("XET_PD_REPLICAS", "x"), `XET_PD_REPLICAS="x": parse error`},
		{"bad env size", nil, env("XET_PD_XORB_CACHE_SIZE", "-1"), `XET_PD_XORB_CACHE_SIZE="-1": expected bytes`},
		{"empty env upstream is still the default", []string{"-upstream", ""}, env("XET_PD_UPSTREAM", "http://hub"), "upstream is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var help bytes.Buffer
			_, _, err := parseGateway(tt.args, tt.env, &help)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseGateway error = %v, want mention of %q", err, tt.want)
			}
			if help.Len() != 0 {
				t.Fatalf("help written on error: %q", help.String())
			}
		})
	}
}

func TestParseGatewayHelp(t *testing.T) {
	var help bytes.Buffer
	_, _, err := parseGateway([]string{"-h"}, noEnv, &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"Usage: xet-pd gateway", "-xorb-cache-size", "(default 8GiB)", "[env XET_PD_XORB_CACHE_SIZE]", "[env XET_PD_UPSTREAM_TOKEN, HF_TOKEN]", "[env XET_PD_SECRET_FILE]", "(default \":8080\")"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, help.String())
		}
	}
}

func TestSecretPolicy(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte(" s3cr3t \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	tests := []struct {
		name    string
		args    []string
		env     func(string) string
		want    string
		wantErr string
	}{
		{name: "flag", args: []string{"-secret", "a"}, env: noEnv, want: "a"},
		{name: "env", env: env("XET_PD_SECRET", "e"), want: "e"},
		{name: "flag file trimmed", args: []string{"-secret-file", file}, env: noEnv, want: "s3cr3t"},
		{name: "env file", env: env("XET_PD_SECRET_FILE", file), want: "s3cr3t"},
		{name: "flag secret beats env file", args: []string{"-secret", "a"}, env: env("XET_PD_SECRET_FILE", file), want: "a"},
		{name: "flag file beats env secret", args: []string{"-secret-file", file}, env: env("XET_PD_SECRET", "e"), want: "s3cr3t"},
		{name: "both flags", args: []string{"-secret", "hunter2", "-secret-file", file}, env: noEnv, wantErr: "mutually exclusive"},
		{name: "both env", env: env("XET_PD_SECRET", "hunter2", "XET_PD_SECRET_FILE", file), wantErr: "mutually exclusive"},
		{name: "missing file", args: []string{"-secret-file", missing}, env: noEnv, wantErr: missing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cfg, err := parseGateway(tt.args, tt.env, io.Discard)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want mention of %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "s3cr3t") {
					t.Fatalf("error echoes a secret: %v", err)
				}
				return
			}
			if err != nil || cfg.Secret != tt.want {
				t.Fatalf("Secret = %q, %v; want %q", cfg.Secret, err, tt.want)
			}
		})
	}
	_, pcfg, err := parsePeer([]string{"-advertise", "http://p:1", "-gateway", "http://g:2"}, env("XET_PD_SECRET_FILE", file), io.Discard)
	if err != nil || pcfg.Secret != "s3cr3t" {
		t.Fatalf("peer Secret = %q, %v; want s3cr3t", pcfg.Secret, err)
	}
}

func TestEmptySecretFlagOverridesEnvironment(t *testing.T) {
	getenv := env("XET_PD_SECRET", "environment-secret", "XET_PD_SECRET_FILE", "/missing-secret-file")
	for _, option := range []string{"-secret", "-secret-file"} {
		t.Run(option, func(t *testing.T) {
			_, gatewayConfig, err := parseGateway([]string{option, ""}, getenv, io.Discard)
			if err != nil || gatewayConfig.Secret != "" {
				t.Fatalf("gateway secret = %q, error = %v, want empty secret", gatewayConfig.Secret, err)
			}
			_, peerConfig, err := parsePeer([]string{"-advertise", "http://peer:8081", "-gateway", "http://gateway:8080", option, ""}, getenv, io.Discard)
			if err != nil || peerConfig.Secret != "" {
				t.Fatalf("peer secret = %q, error = %v, want empty secret", peerConfig.Secret, err)
			}
		})
	}
}

func TestParsePeerDefaults(t *testing.T) {
	// HF_TOKEN and the gateway token variable mean nothing to a peer.
	getenv := env("HF_TOKEN", "hf", "XET_PD_UPSTREAM_TOKEN", "tok")
	listen, cfg, err := parsePeer([]string{"-advertise", "http://p:1", "-gateway", "http://g:2"}, getenv, io.Discard)
	want := peer.Config{Advertise: "http://p:1", Gateway: "http://g:2", CacheDir: "./xet-pd-peer", MaxPulls: 8, PullTimeout: 10 * time.Minute, Heartbeat: 10 * time.Second}
	if err != nil || listen != ":8081" || cfg != want {
		t.Fatalf("parsePeer() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":8081", want)
	}
}

func TestParsePeerEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9001", "XET_PD_ADVERTISE", "http://p:1", "XET_PD_GATEWAY", "http://g:2", "XET_PD_CACHE_DIR", "/cache",
		"XET_PD_XORB_CACHE_SIZE", "2GiB", "XET_PD_MAX_PULLS", "3", "XET_PD_PULL_TIMEOUT", "1m", "XET_PD_HEARTBEAT", "2s", "XET_PD_SECRET", "sec")
	listen, cfg, err := parsePeer(nil, getenv, io.Discard)
	want := peer.Config{Advertise: "http://p:1", Gateway: "http://g:2", CacheDir: "/cache", XorbCacheSize: 2 << 30, MaxPulls: 3, PullTimeout: time.Minute, Heartbeat: 2 * time.Second, Secret: "sec"}
	if err != nil || listen != ":9001" || cfg != want {
		t.Fatalf("parsePeer() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":9001", want)
	}
}

func TestParsePeerFlagsOverrideEnv(t *testing.T) {
	getenv := env(
		"XET_PD_LISTEN", ":9001", "XET_PD_ADVERTISE", "http://p:1", "XET_PD_GATEWAY", "http://g:2", "XET_PD_CACHE_DIR", "/cache",
		"XET_PD_XORB_CACHE_SIZE", "bogus", "XET_PD_MAX_PULLS", "x", "XET_PD_PULL_TIMEOUT", "y", "XET_PD_HEARTBEAT", "z", "XET_PD_SECRET", "sec")
	args := []string{"-listen", ":7001", "-advertise", "http://fp:1", "-gateway", "http://fg:2", "-cache-dir", "/flag",
		"-xorb-cache-size", "512MiB", "-max-pulls", "2", "-pull-timeout", "30s", "-heartbeat", "1s", "-secret", "fsec"}
	listen, cfg, err := parsePeer(args, getenv, io.Discard)
	want := peer.Config{Advertise: "http://fp:1", Gateway: "http://fg:2", CacheDir: "/flag", XorbCacheSize: 512 << 20, MaxPulls: 2, PullTimeout: 30 * time.Second, Heartbeat: time.Second, Secret: "fsec"}
	if err != nil || listen != ":7001" || cfg != want {
		t.Fatalf("parsePeer() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":7001", want)
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
			_, _, err := parsePeer(tt.args, tt.env, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parsePeer error = %v, want mention of %q", err, tt.want)
			}
		})
	}
}

func TestParsePeerHelp(t *testing.T) {
	var help bytes.Buffer
	_, _, err := parsePeer([]string{"--help"}, noEnv, &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"Usage: xet-pd peer", "-advertise", "[env XET_PD_ADVERTISE]", "-pull-timeout", "(default 10m0s)", "[env XET_PD_SECRET]"} {
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

func TestRunDispatch(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		code       int
		stdout     string // substring, "" means empty
		stderr     string // substring, "" means empty
		wantStdout bool
		wantStderr bool
	}{
		{name: "no args", args: nil, code: 2, stderr: "Usage: xet-pd <command>", wantStderr: true},
		{name: "unknown command", args: []string{"bogus"}, code: 2, stderr: `unknown command "bogus"`, wantStderr: true},
		{name: "help", args: []string{"help"}, code: 0, stdout: "gateway", wantStdout: true},
		{name: "-h", args: []string{"-h"}, code: 0, stdout: "peer", wantStdout: true},
		{name: "gateway -h", args: []string{"gateway", "-h"}, code: 0, stdout: "Usage: xet-pd gateway", wantStdout: true},
		{name: "peer --help", args: []string{"peer", "--help"}, code: 0, stdout: "Usage: xet-pd peer", wantStdout: true},
		{name: "gateway unknown flag", args: []string{"gateway", "-bogus"}, code: 2, stderr: "xet-pd gateway: flag provided but not defined: -bogus\nRun \"xet-pd gateway -h\" for flags.\n", wantStderr: true},
		{name: "peer missing advertise", args: []string{"peer"}, code: 2, stderr: "xet-pd peer: advertise is required", wantStderr: true},
		{name: "gateway positional", args: []string{"gateway", "extra"}, code: 2, stderr: `unexpected argument "extra"`, wantStderr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), tt.args, noEnv, &stdout, &stderr); code != tt.code {
				t.Fatalf("run = %d, want %d; stderr %q", code, tt.code, stderr.String())
			}
			if (stdout.Len() > 0) != tt.wantStdout || !strings.Contains(stdout.String(), tt.stdout) {
				t.Fatalf("stdout %q, want %q (non-empty %v)", stdout.String(), tt.stdout, tt.wantStdout)
			}
			if (stderr.Len() > 0) != tt.wantStderr || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("stderr %q, want %q (non-empty %v)", stderr.String(), tt.stderr, tt.wantStderr)
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
	go func() { code <- run(ctx, args, getenv, stdout, stderr) }()
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

func TestRunInitFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"gateway", "-listen", "127.0.0.1:0", "-storage-dir", t.TempDir(), "-upstream", "ftp://hub"}
	if code := run(context.Background(), args, noEnv, &stdout, &stderr); code != 1 {
		t.Fatalf("run = %d, want 1; stderr %q", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "xet-pd gateway: gateway: upstream: scheme is not http(s)") {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}

func TestRunListenFailureClosesService(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()
	gw := newFakeGateway(t)
	args := []string{"peer", "-listen", taken.Addr().String(), "-advertise", "http://127.0.0.1:1", "-gateway", gw.URL, "-cache-dir", t.TempDir(), "-heartbeat", "20ms"}
	stdout, stderr, code := startRun(context.Background(), args, noEnv)
	if got := await(t, "exit", code); got != 1 {
		t.Fatalf("run = %d, want 1; stderr %q", got, stderr.String())
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
	args := []string{"peer", "-listen", "127.0.0.1:0", "-advertise", "http://127.0.0.1:1", "-gateway", gw.URL, "-cache-dir", t.TempDir(), "-secret", "hunter2"}
	stdout, stderr, code := startRun(ctx, args, noEnv)
	banner := await(t, "banner", stdout)
	addr, ok := strings.CutPrefix(strings.TrimSpace(banner), "xet-pd peer listening on ")
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
		t.Fatalf("run = %d, want 0; stderr %q", got, stderr.String())
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

func TestRunGatewayCancelledBeforeServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	args := []string{"gateway", "-listen", "127.0.0.1:0", "-storage-dir", t.TempDir(), "-upstream", "http://127.0.0.1:1"}
	if code := run(ctx, args, noEnv, &stdout, &stderr); code != 0 {
		t.Fatalf("run = %d, want 0; stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "xet-pd gateway listening on 127.0.0.1:") || stderr.Len() != 0 {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}

func TestServeHTTPCancelledContextClosesListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	if err := serveHTTP(ctx, "127.0.0.1:0", http.NotFoundHandler(), &stdout, "xet-pd test"); err != nil {
		t.Fatal(err)
	}
	addr, ok := strings.CutPrefix(strings.TrimSpace(stdout.String()), "xet-pd test listening on ")
	if !ok {
		t.Fatalf("banner %q", stdout.String())
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("listener still open after return")
	}
}

func TestServeHTTPForceClosesStalledResponses(t *testing.T) {
	defer func(d time.Duration) { shutdownTimeout = d }(shutdownTimeout)
	shutdownTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stalled := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	stdout, done := make(chanWriter, 8), make(chan error, 1)
	go func() { done <- serveHTTP(ctx, "127.0.0.1:0", stalled, stdout, "xet-pd test") }()
	addr, _ := strings.CutPrefix(strings.TrimSpace(await(t, "banner", stdout)), "xet-pd test listening on ")
	resp, err := http.Get("http://" + addr + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	start := time.Now()
	cancel()
	if err := await(t, "return", done); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("stalled response completed normally, want a closed connection")
	}
}
