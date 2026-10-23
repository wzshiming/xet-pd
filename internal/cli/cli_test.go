package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/cli/clitest"
)

func TestParseSize(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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
	t.Parallel()
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

// testFlags holds one flag of each kind; name falls back to two environment keys.
type testFlags struct {
	name    string
	count   int
	timeout time.Duration
	size    int64
}

func parseTestFlags(args []string, getenv func(string) string, help io.Writer) (testFlags, error) {
	var f testFlags
	fs := NewFlagSet("test")
	fs.StringVar(&f.name, "name", "anon", "who", "TEST_NAME", "TEST_ALIAS")
	fs.IntVar(&f.count, "count", 1, "how many", "TEST_COUNT")
	fs.DurationVar(&f.timeout, "timeout", time.Second, "how long", "TEST_TIMEOUT")
	fs.SizeVar(&f.size, "size", 1<<20, "how big", "TEST_SIZE")
	err := fs.Parse(args, getenv, help)
	return f, err
}

func TestFlagSetSources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want testFlags
	}{
		{"defaults", nil, clitest.Getenv(), testFlags{"anon", 1, time.Second, 1 << 20}},
		{"environment", nil, clitest.Getenv("TEST_NAME", "env", "TEST_COUNT", "2", "TEST_TIMEOUT", "2s", "TEST_SIZE", "2MiB"), testFlags{"env", 2, 2 * time.Second, 2 << 20}},
		{"later key fills in", nil, clitest.Getenv("TEST_ALIAS", "alias"), testFlags{"alias", 1, time.Second, 1 << 20}},
		{"earlier key wins", nil, clitest.Getenv("TEST_NAME", "env", "TEST_ALIAS", "alias"), testFlags{"env", 1, time.Second, 1 << 20}},
		// An unparsable environment value is ignored once its flag is given.
		{"flags win", []string{"-name", "flag", "-count", "3", "-timeout", "3s", "-size", "3KiB"}, clitest.Getenv("TEST_NAME", "env", "TEST_COUNT", "x", "TEST_TIMEOUT", "y", "TEST_SIZE", "z"), testFlags{"flag", 3, 3 * time.Second, 3 << 10}},
		{"empty flag wins", []string{"-name", ""}, clitest.Getenv("TEST_NAME", "env"), testFlags{"", 1, time.Second, 1 << 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTestFlags(tt.args, tt.env, io.Discard)
			if err != nil || got != tt.want {
				t.Fatalf("Parse = %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

func TestFlagSetErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"positional argument", []string{"-count", "2", "extra"}, clitest.Getenv(), `unexpected argument "extra"`},
		{"unknown flag", []string{"-bogus"}, clitest.Getenv(), "flag provided but not defined: -bogus"},
		{"bad env int", nil, clitest.Getenv("TEST_COUNT", "x"), `TEST_COUNT="x": parse error`},
		{"bad env duration", nil, clitest.Getenv("TEST_TIMEOUT", "soon"), `TEST_TIMEOUT="soon": parse error`},
		{"bad env size", nil, clitest.Getenv("TEST_SIZE", "-1"), `TEST_SIZE="-1": expected bytes`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var help bytes.Buffer
			_, err := parseTestFlags(tt.args, tt.env, &help)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse error = %v, want mention of %q", err, tt.want)
			}
			if help.Len() != 0 {
				t.Fatalf("help written on error: %q", help.String())
			}
		})
	}
}

func TestFlagSetHelp(t *testing.T) {
	t.Parallel()
	var help bytes.Buffer
	_, err := parseTestFlags([]string{"-h"}, clitest.Getenv(), &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	const head = "Usage: test [flags]\n\nFlags win over the environment variables in brackets.\n\n"
	if !strings.HasPrefix(help.String(), head) {
		t.Errorf("help does not start with %q:\n%s", head, help.String())
	}
	for _, want := range []string{"who [env TEST_NAME, TEST_ALIAS]", "how big [env TEST_SIZE] (default 1MiB)"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, help.String())
		}
	}
}

// secretOf parses args with only the secret flags registered and resolves the secret.
func secretOf(args []string, getenv func(string) string) (string, error) {
	fs := NewFlagSet("test")
	secret, file := fs.SecretFlags()
	if err := fs.Parse(args, getenv, io.Discard); err != nil {
		return "", err
	}
	return fs.ResolveSecret(*secret, *file, getenv)
}

func TestSecretPolicy(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte(" s3cr3t \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	blank := filepath.Join(t.TempDir(), "blank")
	if err := os.WriteFile(blank, []byte("  \n\n"), 0o600); err != nil {
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
		{name: "flag", args: []string{"-secret", "a"}, env: clitest.Getenv(), want: "a"},
		{name: "env", env: clitest.Getenv("XET_PD_SECRET", "e"), want: "e"},
		{name: "flag file trimmed", args: []string{"-secret-file", file}, env: clitest.Getenv(), want: "s3cr3t"},
		{name: "env file", env: clitest.Getenv("XET_PD_SECRET_FILE", file), want: "s3cr3t"},
		{name: "flag secret beats env file", args: []string{"-secret", "a"}, env: clitest.Getenv("XET_PD_SECRET_FILE", file), want: "a"},
		{name: "flag file beats env secret", args: []string{"-secret-file", file}, env: clitest.Getenv("XET_PD_SECRET", "e"), want: "s3cr3t"},
		{name: "both flags", args: []string{"-secret", "hunter2", "-secret-file", file}, env: clitest.Getenv(), wantErr: "mutually exclusive"},
		{name: "both env", env: clitest.Getenv("XET_PD_SECRET", "hunter2", "XET_PD_SECRET_FILE", file), wantErr: "mutually exclusive"},
		{name: "missing file", args: []string{"-secret-file", missing}, env: clitest.Getenv(), wantErr: missing},
		{name: "blank flag file", args: []string{"-secret-file", blank}, env: clitest.Getenv(), wantErr: "secret file is empty"},
		{name: "blank env file", env: clitest.Getenv("XET_PD_SECRET_FILE", blank), wantErr: "secret file is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			secret, err := secretOf(tt.args, tt.env)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want mention of %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "s3cr3t") {
					t.Fatalf("error echoes a secret: %v", err)
				}
				return
			}
			if err != nil || secret != tt.want {
				t.Fatalf("Secret = %q, %v; want %q", secret, err, tt.want)
			}
		})
	}
}

func TestEmptySecretFlagOverridesEnvironment(t *testing.T) {
	t.Parallel()
	getenv := clitest.Getenv("XET_PD_SECRET", "environment-secret", "XET_PD_SECRET_FILE", "/missing-secret-file")
	for _, option := range []string{"-secret", "-secret-file"} {
		t.Run(option, func(t *testing.T) {
			t.Parallel()
			secret, err := secretOf([]string{option, ""}, getenv)
			if err != nil || secret != "" {
				t.Fatalf("secret = %q, error = %v, want empty secret", secret, err)
			}
		})
	}
}

// fakeService counts the Close calls Run makes.
type fakeService struct {
	http.Handler
	closes atomic.Int32
}

func (s *fakeService) Close() error {
	s.closes.Add(1)
	return nil
}

// fakeCommand is named "fake": Parse yields listen and parseErr, writing a usage line on flag.ErrHelp; New yields svc or newErr.
func fakeCommand(listen string, parseErr error, svc *fakeService, newErr error) Command[struct{}] {
	return Command[struct{}]{
		Name: "fake",
		Parse: func(_ []string, _ func(string) string, help io.Writer) (string, struct{}, error) {
			if errors.Is(parseErr, flag.ErrHelp) {
				_, _ = io.WriteString(help, "Usage: fake [flags]\n")
			}
			return listen, struct{}{}, parseErr
		},
		New: func(struct{}) (Service, error) {
			if newErr != nil {
				return nil, newErr
			}
			return svc, nil
		},
	}
}

func TestRunExitsBeforeServing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		parseErr, newErr error
		code             int
		stdout, stderr   string
	}{
		{name: "help", parseErr: flag.ErrHelp, code: 0, stdout: "Usage: fake [flags]\n"},
		{name: "usage error", parseErr: errors.New("bad flag"), code: 2, stderr: "fake: bad flag\nRun \"fake -h\" for flags.\n"},
		{name: "init failure", newErr: errors.New("boom"), code: 1, stderr: "fake: boom\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := fakeCommand("127.0.0.1:0", tt.parseErr, nil, tt.newErr).Run(context.Background(), nil, clitest.Getenv(), &stdout, &stderr)
			if code != tt.code || stdout.String() != tt.stdout || stderr.String() != tt.stderr {
				t.Fatalf("Run = %d, stdout %q, stderr %q; want %d, %q, %q", code, stdout.String(), stderr.String(), tt.code, tt.stdout, tt.stderr)
			}
		})
	}
}

func TestRunListenFailureClosesService(t *testing.T) {
	t.Parallel()
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()
	svc := &fakeService{Handler: http.NotFoundHandler()}
	var stdout, stderr bytes.Buffer
	if code := fakeCommand(taken.Addr().String(), nil, svc, nil).Run(context.Background(), nil, clitest.Getenv(), &stdout, &stderr); code != 1 {
		t.Fatalf("Run = %d, want 1; stderr %q", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.HasPrefix(stderr.String(), "fake: ") || !strings.Contains(stderr.String(), "address already in use") {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
	if n := svc.closes.Load(); n != 1 {
		t.Fatalf("Close called %d times, want 1", n)
	}
}

func TestRunServesUntilCancelled(t *testing.T) {
	t.Parallel()
	svc := &fakeService{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout, stderr, code := make(clitest.ChanWriter, 8), &bytes.Buffer{}, make(chan int, 1)
	go func() {
		code <- fakeCommand("127.0.0.1:0", nil, svc, nil).Run(ctx, nil, clitest.Getenv(), stdout, stderr)
	}()
	banner := clitest.Await(t, "banner", stdout)
	addr, ok := strings.CutPrefix(strings.TrimSpace(banner), "fake listening on ")
	if !ok {
		t.Fatalf("banner %q", banner)
	}
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot || svc.closes.Load() != 0 {
		t.Fatalf("status %d, %d closes while serving", resp.StatusCode, svc.closes.Load())
	}
	cancel()
	if got := clitest.Await(t, "exit", code); got != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", got, stderr.String())
	}
	if n := svc.closes.Load(); n != 1 {
		t.Fatalf("Close called %d times, want 1", n)
	}
}

func TestRunCancelledBeforeServing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &fakeService{Handler: http.NotFoundHandler()}
	var stdout, stderr bytes.Buffer
	if code := fakeCommand("127.0.0.1:0", nil, svc, nil).Run(ctx, nil, clitest.Getenv(), &stdout, &stderr); code != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "fake listening on 127.0.0.1:") || stderr.Len() != 0 || svc.closes.Load() != 1 {
		t.Fatalf("stdout %q, stderr %q, %d closes", stdout.String(), stderr.String(), svc.closes.Load())
	}
}

func TestServeHTTPCancelledContextClosesListener(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	if err := ServeHTTP(ctx, "127.0.0.1:0", http.NotFoundHandler(), &stdout, "xet-pd test"); err != nil {
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
	// Not parallel: it overrides the package-level shutdown timeout that ServeHTTP reads in the other tests.
	defer func(d time.Duration) { shutdownTimeout = d }(shutdownTimeout)
	shutdownTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stalled := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	stdout, done := make(clitest.ChanWriter, 8), make(chan error, 1)
	go func() { done <- ServeHTTP(ctx, "127.0.0.1:0", stalled, stdout, "xet-pd test") }()
	addr, _ := strings.CutPrefix(strings.TrimSpace(clitest.Await(t, "banner", stdout)), "xet-pd test listening on ")
	resp, err := http.Get("http://" + addr + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	start := time.Now()
	cancel()
	if err := clitest.Await(t, "return", done); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("stalled response completed normally, want a closed connection")
	}
}
