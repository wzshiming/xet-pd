package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/cli"
	"github.com/wzshiming/xet-pd/internal/cli/clitest"
	"github.com/wzshiming/xet-pd/tracker"
)

func TestParseTrackerDefaults(t *testing.T) {
	settings, cfg, err := parse(nil, clitest.Getenv(), io.Discard)
	wantSettings := cli.Settings{Listen: ":8080", LogFormat: cli.LogText}
	want := tracker.Config{PeerTTL: 30 * time.Second, Replicas: 2, MinSeeds: 1}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
	}
}

func TestParseTrackerEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "45s",
		"XET_PD_REPLICAS", "3", "XET_PD_MIN_SEEDS", "2", "XET_PD_LOG_FORMAT", "json")
	settings, cfg, err := parse(nil, getenv, io.Discard)
	wantSettings := cli.Settings{Listen: ":9000", LogFormat: cli.LogJSON}
	want := tracker.Config{Secret: "sec", PeerTTL: 45 * time.Second, Replicas: 3, MinSeeds: 2}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
	}
}

// Flags win over the environment, and an unparsable environment value is ignored once its flag is given.
func TestParseTrackerFlagsOverrideEnv(t *testing.T) {
	getenv := clitest.Getenv(
		"XET_PD_LISTEN", ":9000", "XET_PD_SECRET", "sec", "XET_PD_PEER_TTL", "bogus",
		"XET_PD_REPLICAS", "x", "XET_PD_MIN_SEEDS", "1.5", "XET_PD_LOG_FORMAT", "bogus")
	args := []string{"-listen", ":7000", "-secret", "fsec", "-peer-ttl", "5s", "-replicas", "4", "-min-seeds", "3", "-log-format", "json"}
	settings, cfg, err := parse(args, getenv, io.Discard)
	wantSettings := cli.Settings{Listen: ":7000", LogFormat: cli.LogJSON}
	want := tracker.Config{Secret: "fsec", PeerTTL: 5 * time.Second, Replicas: 4, MinSeeds: 3}
	if err != nil || settings != wantSettings || cfg != want {
		t.Fatalf("parse() = %+v, %+v, %v; want %+v, %+v", settings, cfg, err, wantSettings, want)
	}
}

func TestParseTrackerErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{"positional argument", []string{"extra"}, clitest.Getenv(), `unexpected argument "extra"`},
		{"unknown flag", []string{"-bogus"}, clitest.Getenv(), "flag provided but not defined: -bogus"},
		{"bad duration flag", []string{"-peer-ttl", "abc"}, clitest.Getenv(), "invalid value"},
		{"bad log format flag", []string{"-log-format", "yaml"}, clitest.Getenv(), `invalid value "yaml" for flag -log-format: expected text or json`},
		{"bad env duration", nil, clitest.Getenv("XET_PD_PEER_TTL", "abc"), `XET_PD_PEER_TTL="abc": parse error`},
		{"bad env replicas", nil, clitest.Getenv("XET_PD_REPLICAS", "x"), `XET_PD_REPLICAS="x": parse error`},
		{"bad env min seeds", nil, clitest.Getenv("XET_PD_MIN_SEEDS", "x"), `XET_PD_MIN_SEEDS="x": parse error`},
		{"bad env log format", nil, clitest.Getenv("XET_PD_LOG_FORMAT", "yaml"), `XET_PD_LOG_FORMAT="yaml": expected text or json`},
		{"secret and secret file", []string{"-secret", "s", "-secret-file", "f"}, clitest.Getenv(), "secret and secret-file are mutually exclusive"},
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

func TestParseTrackerHelp(t *testing.T) {
	var help bytes.Buffer
	_, _, err := parse([]string{"-h"}, clitest.Getenv(), &help)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
	var names []string
	for _, m := range regexp.MustCompile(`(?m)^  -(\S+)`).FindAllStringSubmatch(help.String(), -1) {
		names = append(names, m[1])
	}
	if want := []string{"listen", "log-format", "min-seeds", "peer-ttl", "replicas", "secret", "secret-file"}; !slices.Equal(names, want) {
		t.Fatalf("help lists flags %v, want exactly %v:\n%s", names, want, help.String())
	}
	for _, want := range []string{
		"Usage: tracker [flags]",
		"[env XET_PD_LISTEN] (default \":8080\")",
		"[env XET_PD_LOG_FORMAT] (default text)",
		"seconds a peer stays registered after its last announce [env XET_PD_PEER_TTL] (default 30s)",
		"seed copies the tracker asks for per complete file [env XET_PD_REPLICAS] (default 2)",
		"other live holders a seed copy needs before a peer may evict it [env XET_PD_MIN_SEEDS] (default 1)",
		"[env XET_PD_SECRET]", "[env XET_PD_SECRET_FILE]",
	} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, help.String())
		}
	}
}

func TestParseTrackerSecretFile(t *testing.T) {
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
		{name: "-h", args: []string{"-h"}, code: 0, stdout: "Usage: tracker [flags]"},
		{name: "unknown flag", args: []string{"-bogus"}, code: 2, stderr: "tracker: flag provided but not defined: -bogus\nRun \"tracker -h\" for flags.\n"},
		{name: "positional", args: []string{"extra"}, code: 2, stderr: `unexpected argument "extra"`},
		{name: "bad log format", args: []string{"-log-format", "yaml"}, code: 2, stderr: "tracker: invalid value \"yaml\" for flag -log-format: expected text or json\nRun \"tracker -h\" for flags.\n"},
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
	args := []string{"-listen", "127.0.0.1:0", "-replicas", "1", "-min-seeds", "2"}
	if code := command.Run(context.Background(), args, clitest.Getenv(), &stdout, &stderr); code != 1 {
		t.Fatalf("Run = %d, want 1; stderr %q", code, stderr.String())
	}
	const want = "tracker: min seeds exceed replicas\n"
	if stdout.Len() != 0 || stderr.String() != want {
		t.Fatalf("stdout %q, stderr %q; want empty, %q", stdout.String(), stderr.String(), want)
	}
}

func TestRunTrackerCancelledBeforeServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := command.Run(ctx, []string{"-listen", "127.0.0.1:0"}, clitest.Getenv(), &stdout, &stderr); code != 0 {
		t.Fatalf("Run = %d, want 0; stderr %q", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "tracker listening on 127.0.0.1:") || stderr.Len() != 0 {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}
