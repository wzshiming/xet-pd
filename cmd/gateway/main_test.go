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
	"github.com/wzshiming/xet-pd/internal/cli"
	"github.com/wzshiming/xet-pd/internal/cli/clitest"
)

func TestParseGatewayDefaults(t *testing.T) {
	settings, cfg, err := parse(nil, clitest.Getenv(), io.Discard)
	wantSettings := cli.Settings{Listen: ":8080", LogFormat: cli.LogText}
	want := gateway.Config{DataDir: "./xet-pd-gateway", Upstream: "https://huggingface.co", PeerTTL: 30 * time.Second, Replicas: 2, MinSeeds: 1, XorbCacheSize: 8 << 30}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
	}
}

func TestParseGatewayEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_DATA_DIR", "/data", "HF_ENDPOINT", "http://hub:8000",
		"HF_TOKEN", "tok", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "45s", "XET_PD_REPLICAS", "3", "XET_PD_MIN_SEEDS", "2",
		"XET_PD_XORB_CACHE_SIZE", "64MiB", "XET_PD_LOG_FORMAT", "json")
	settings, cfg, err := parse(nil, getenv, io.Discard)
	wantSettings := cli.Settings{Listen: ":9000", LogFormat: cli.LogJSON}
	want := gateway.Config{DataDir: "/data", Upstream: "http://hub:8000", UpstreamToken: "tok", SigningKey: "sign", ExternalURL: "http://gw", Secret: "sec", PeerTTL: 45 * time.Second, Replicas: 3, MinSeeds: 2, XorbCacheSize: 64 << 20}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
	}
}

// Flags win over the environment, and an unparsable environment value is ignored once its flag is given.
func TestParseGatewayFlagsOverrideEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_DATA_DIR", "/data", "HF_ENDPOINT", "http://hub:8000",
		"HF_TOKEN", "tok", "XET_PD_SIGNING_KEY", "sign", "XET_PD_EXTERNAL_URL", "http://gw",
		"XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "bogus", "XET_PD_REPLICAS", "x", "XET_PD_MIN_SEEDS", "1.5",
		"XET_PD_XORB_CACHE_SIZE", "-1", "XET_PD_LOG_FORMAT", "bogus")
	args := []string{"-listen", ":7000", "-data-dir", "/flag", "-upstream", "http://flag", "-upstream-token", "ftok",
		"-signing-key", "fsign", "-external-url", "http://fgw", "-secret", "fsec", "-peer-ttl", "5s", "-replicas", "4",
		"-min-seeds", "3", "-xorb-cache-size", "1GiB", "-log-format", "json"}
	settings, cfg, err := parse(args, getenv, io.Discard)
	wantSettings := cli.Settings{Listen: ":7000", LogFormat: cli.LogJSON}
	want := gateway.Config{DataDir: "/flag", Upstream: "http://flag", UpstreamToken: "ftok", SigningKey: "fsign", ExternalURL: "http://fgw", Secret: "fsec", PeerTTL: 5 * time.Second, Replicas: 4, MinSeeds: 3, XorbCacheSize: 1 << 30}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
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
		{"bad log format flag", []string{"-log-format", "yaml"}, clitest.Getenv(), `invalid value "yaml" for flag -log-format: expected text or json`},
		{"bad env duration", nil, clitest.Getenv("XET_PD_PEER_TTL", "abc"), `XET_PD_PEER_TTL="abc": parse error`},
		{"bad env int", nil, clitest.Getenv("XET_PD_REPLICAS", "x"), `XET_PD_REPLICAS="x": parse error`},
		{"bad env size", nil, clitest.Getenv("XET_PD_XORB_CACHE_SIZE", "-1"), `XET_PD_XORB_CACHE_SIZE="-1": expected bytes`},
		{"bad env log format", nil, clitest.Getenv("XET_PD_LOG_FORMAT", "yaml"), `XET_PD_LOG_FORMAT="yaml": expected text or json`},
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
	for _, want := range []string{"Usage: gateway [flags]", "-data-dir", "[env XET_PD_DATA_DIR]", "-xorb-cache-size", "(default 8GiB)", "[env XET_PD_XORB_CACHE_SIZE]", "[env HF_TOKEN]", "[env XET_PD_SECRET_FILE]", "(default \":8080\")", "-log-format", "[env XET_PD_LOG_FORMAT]", "(default text)"} {
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
		{name: "bad log format", args: []string{"-log-format", "yaml"}, code: 2, stderr: "gateway: invalid value \"yaml\" for flag -log-format: expected text or json\nRun \"gateway -h\" for flags.\n"},
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
	args := []string{"-listen", "127.0.0.1:0", "-data-dir", t.TempDir(), "-upstream", "ftp://hub"}
	if code := command.Run(context.Background(), args, clitest.Getenv(), &stdout, &stderr); code != 1 {
		t.Fatalf("Run = %d, want 1; stderr %q", code, stderr.String())
	}
	const want = "gateway: upstream: scheme is not http(s)\n"
	if stdout.Len() != 0 || stderr.String() != want {
		t.Fatalf("stdout %q, stderr %q; want empty, %q", stdout.String(), stderr.String(), want)
	}
}

func TestRunGatewayCancelledBeforeServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	args := []string{"-listen", "127.0.0.1:0", "-data-dir", t.TempDir(), "-upstream", "http://127.0.0.1:1"}
	if code := command.Run(ctx, args, clitest.Getenv(), &stdout, &stderr); code != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "gateway listening on 127.0.0.1:") || stderr.Len() != 0 {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}
