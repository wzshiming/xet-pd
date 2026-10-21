package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/gateway"
	"github.com/wzshiming/xet-pd/internal/cli/clitest"
)

func TestParseGatewayDefaults(t *testing.T) {
	listen, cfg, err := parse(nil, clitest.Getenv(), io.Discard)
	want := gateway.Config{StorageDir: "./xet-pd-gateway", Upstream: "https://huggingface.co", PeerTTL: 30 * time.Second, RelayTimeout: 5 * time.Second, Replicas: 2, MinSeeds: 1, MaxPulls: 8, XorbCacheSize: 8 << 30}
	if err != nil || listen != ":8080" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":8080", want)
	}
}

func TestParseGatewayEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_STORAGE_DIR", "/data", "HF_ENDPOINT", "http://hub:8000",
		"HF_TOKEN", "tok", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "45s", "XET_PD_RELAY_TIMEOUT", "7s", "XET_PD_REPLICAS", "3", "XET_PD_MIN_SEEDS", "2",
		"XET_PD_MAX_PULLS", "3", "XET_PD_XORB_CACHE_SIZE", "64MiB")
	listen, cfg, err := parse(nil, getenv, io.Discard)
	want := gateway.Config{StorageDir: "/data", Upstream: "http://hub:8000", UpstreamToken: "tok", SigningKey: "sign", ExternalURL: "http://gw", Secret: "sec", PeerTTL: 45 * time.Second, RelayTimeout: 7 * time.Second, Replicas: 3, MinSeeds: 2, MaxPulls: 3, XorbCacheSize: 64 << 20}
	if err != nil || listen != ":9000" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":9000", want)
	}
}

// Flags win over the environment, and an unparsable environment value is ignored once its flag is given.
func TestParseGatewayFlagsOverrideEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_STORAGE_DIR", "/data", "HF_ENDPOINT", "http://hub:8000",
		"HF_TOKEN", "tok", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "bogus", "XET_PD_RELAY_TIMEOUT", "bogus", "XET_PD_REPLICAS", "x", "XET_PD_MIN_SEEDS", "1.5",
		"XET_PD_MAX_PULLS", "x", "XET_PD_XORB_CACHE_SIZE", "-1")
	args := []string{"-listen", ":7000", "-storage-dir", "/flag", "-upstream", "http://flag", "-upstream-token", "ftok",
		"-signing-key", "fsign", "-external-url", "http://fgw", "-secret", "fsec", "-peer-ttl", "5s", "-relay-timeout", "3s", "-replicas", "4",
		"-min-seeds", "3", "-max-pulls", "2", "-xorb-cache-size", "1GiB"}
	listen, cfg, err := parse(args, getenv, io.Discard)
	want := gateway.Config{StorageDir: "/flag", Upstream: "http://flag", UpstreamToken: "ftok", SigningKey: "fsign", ExternalURL: "http://fgw", Secret: "fsec", PeerTTL: 5 * time.Second, RelayTimeout: 3 * time.Second, Replicas: 4, MinSeeds: 3, MaxPulls: 2, XorbCacheSize: 1 << 30}
	if err != nil || listen != ":7000" || cfg != want {
		t.Fatalf("parse() = %q, %+v, %v; want %q, %+v", listen, cfg, err, ":7000", want)
	}
}

func TestParseGatewayUpstreamToken(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"xet-pd token", nil, clitest.Getenv("HF_TOKEN", "tok"), "tok"},
		{"flag wins", []string{"-upstream-token", "ftok"}, clitest.Getenv("HF_TOKEN", "tok"), "ftok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cfg, err := parse(tt.args, tt.env, io.Discard)
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
		{"positional argument", []string{"extra"}, clitest.Getenv(), `unexpected argument "extra"`},
		{"unknown flag", []string{"-bogus"}, clitest.Getenv(), "flag provided but not defined: -bogus"},
		{"empty upstream", []string{"-upstream", ""}, clitest.Getenv(), "upstream is required"},
		{"bad duration flag", []string{"-peer-ttl", "abc"}, clitest.Getenv(), "invalid value"},
		{"bad size flag", []string{"-xorb-cache-size", "8GB"}, clitest.Getenv(), "suffix"},
		{"bad env duration", nil, clitest.Getenv("XET_PD_PEER_TTL", "abc"), `XET_PD_PEER_TTL="abc": parse error`},
		{"bad env int", nil, clitest.Getenv("XET_PD_REPLICAS", "x"), `XET_PD_REPLICAS="x": parse error`},
		{"bad env size", nil, clitest.Getenv("XET_PD_XORB_CACHE_SIZE", "-1"), `XET_PD_XORB_CACHE_SIZE="-1": expected bytes`},
		{"empty env upstream is still the default", []string{"-upstream", ""}, clitest.Getenv("HF_ENDPOINT", "http://hub"), "upstream is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var help bytes.Buffer
			_, _, err := parse(tt.args, tt.env, &help)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parse error = %v, want mention of %q", err, tt.want)
			}
			if help.Len() != 0 {
				t.Fatalf("help written on error: %q", help.String())
			}
		})
	}
}

func TestParseGatewayHelp(t *testing.T) {
	var help bytes.Buffer
	_, _, err := parse([]string{"-h"}, clitest.Getenv(), &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"Usage: gateway [flags]", "-xorb-cache-size", "(default 8GiB)", "[env XET_PD_XORB_CACHE_SIZE]", "[env HF_TOKEN]", "[env XET_PD_SECRET_FILE]", "(default \":8080\")", "-relay-timeout", "[env XET_PD_RELAY_TIMEOUT]", "(default 5s)", "-max-pulls", "[env XET_PD_MAX_PULLS]", "(default 8)\n"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, help.String())
		}
	}
}

func TestParseGatewaySecretFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte(" s3cr3t \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, cfg, err := parse([]string{"-secret-file", file}, clitest.Getenv(), io.Discard)
	if err != nil || cfg.Secret != "s3cr3t" {
		t.Fatalf("Secret = %q, %v; want s3cr3t", cfg.Secret, err)
	}
}

func TestRunUsage(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		code           int
		stdout, stderr string // substrings, "" means empty
	}{
		{name: "-h", args: []string{"-h"}, code: 0, stdout: "Usage: gateway [flags]"},
		{name: "unknown flag", args: []string{"-bogus"}, code: 2, stderr: "gateway: flag provided but not defined: -bogus\nRun \"gateway -h\" for flags.\n"},
		{name: "positional", args: []string{"extra"}, code: 2, stderr: `unexpected argument "extra"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := command.Run(context.Background(), tt.args, clitest.Getenv(), &stdout, &stderr); code != tt.code {
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

func TestRunInitFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"-listen", "127.0.0.1:0", "-storage-dir", t.TempDir(), "-upstream", "ftp://hub"}
	if code := command.Run(context.Background(), args, clitest.Getenv(), &stdout, &stderr); code != 1 {
		t.Fatalf("Run = %d, want 1; stderr %q", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "gateway: gateway: upstream: scheme is not http(s)") {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}

func TestRunGatewayCancelledBeforeServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	args := []string{"-listen", "127.0.0.1:0", "-storage-dir", t.TempDir(), "-upstream", "http://127.0.0.1:1"}
	if code := command.Run(ctx, args, clitest.Getenv(), &stdout, &stderr); code != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "gateway listening on 127.0.0.1:") || stderr.Len() != 0 {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}
