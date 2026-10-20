// Package cli is the command-line plumbing the gateway and peer commands share.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Command is a binary serving the Service that New builds from the configuration Parse reads.
type Command[C any] struct {
	// Name is the binary name, the prefix of messages and of the banner.
	Name string
	// Parse returns the listen address and the configuration.
	Parse func(args []string, getenv func(string) string, help io.Writer) (string, C, error)
	New   func(C) (Service, error)
}

// Run executes the command line and returns the process exit code: 2 usage, 1 runtime failure, 0 otherwise.
func (c Command[C]) Run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	listen, cfg, err := c.Parse(args, getenv, stdout)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case err != nil:
		_, _ = fmt.Fprintf(stderr, "%s: %v\nRun \"%s -h\" for flags.\n", c.Name, err, c.Name)
		return 2
	}
	svc, err := c.New(cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Name, err)
		return 1
	}
	defer func() { _ = svc.Close() }()
	if err := ServeHTTP(ctx, listen, svc, stdout, c.Name); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Name, err)
		return 1
	}
	return 0
}

// Service is the handler a Command serves; Run closes it once serving stops.
type Service interface {
	http.Handler
	Close() error
}

var shutdownTimeout = 10 * time.Second

// ServeHTTP serves handler on listen until ctx is done, then drains connections for shutdownTimeout and force-closes the rest.
func ServeHTTP(ctx context.Context, listen string, handler http.Handler, stdout io.Writer, banner string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 15 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	_, _ = fmt.Fprintf(stdout, "%s listening on %s\n", banner, ln.Addr())
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	drain, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(drain); err != nil {
		_ = srv.Close()
	}
	return nil
}

// FlagSet is a flag.FlagSet whose flags fall back to environment variables when absent from the command line.
type FlagSet struct {
	*flag.FlagSet
	env [][2]string // flag name, environment key; earlier keys win
}

func NewFlagSet(name string) *FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return &FlagSet{FlagSet: fs}
}

func (s *FlagSet) bind(name, usage string, keys []string) string {
	for _, key := range keys {
		s.env = append(s.env, [2]string{name, key})
	}
	return usage + " [env " + strings.Join(keys, ", ") + "]"
}

func (s *FlagSet) StringVar(p *string, name, value, usage string, keys ...string) {
	s.FlagSet.StringVar(p, name, value, s.bind(name, usage, keys))
}

func (s *FlagSet) IntVar(p *int, name string, value int, usage string, keys ...string) {
	s.FlagSet.IntVar(p, name, value, s.bind(name, usage, keys))
}

func (s *FlagSet) DurationVar(p *time.Duration, name string, value time.Duration, usage string, keys ...string) {
	s.FlagSet.DurationVar(p, name, value, s.bind(name, usage, keys))
}

func (s *FlagSet) SizeVar(p *int64, name string, value int64, usage string, keys ...string) {
	*p = value
	s.Var((*sizeValue)(p), name, s.bind(name, usage, keys))
}

// Parse applies the command line, then the environment to flags left unset, and rejects positional arguments; -h prints the usage to help.
func (s *FlagSet) Parse(args []string, getenv func(string) string, help io.Writer) error {
	if err := s.FlagSet.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintf(help, "Usage: %s [flags]\n\nFlags win over the environment variables in brackets.\n\n", s.Name())
			s.SetOutput(help)
			s.PrintDefaults()
		}
		return err
	}
	if s.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", s.Arg(0))
	}
	set := map[string]bool{}
	s.Visit(func(f *flag.Flag) { set[f.Name] = true })
	for _, e := range s.env {
		name, key := e[0], e[1]
		if v := getenv(key); v != "" && !set[name] {
			if err := s.Set(name, v); err != nil {
				return fmt.Errorf("%s=%q: %w", key, v, err)
			}
			set[name] = true
		}
	}
	return nil
}

// SecretFlags registers -secret and -secret-file; their environment fallback is resolved by ResolveSecret.
func (s *FlagSet) SecretFlags() (secret, file *string) {
	secret = s.String("secret", "", "bearer token shared by the gateway and its peers [env XET_PD_SECRET]")
	file = s.String("secret-file", "", "file holding the shared secret, surrounding whitespace trimmed [env XET_PD_SECRET_FILE]")
	return secret, file
}

// ResolveSecret prefers the flags over the environment; a literal and a file from the same source, or an empty file, are errors.
func (s *FlagSet) ResolveSecret(secret, file string, getenv func(string) string) (string, error) {
	explicit := false
	s.Visit(func(option *flag.Flag) {
		if option.Name == "secret" || option.Name == "secret-file" {
			explicit = true
		}
	})
	if !explicit {
		secret, file = getenv("XET_PD_SECRET"), getenv("XET_PD_SECRET_FILE")
	}
	if secret != "" && file != "" {
		return "", errors.New("secret and secret-file are mutually exclusive")
	}
	if file == "" {
		return secret, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("secret file: %w", err)
	}
	secret = strings.TrimSpace(string(b))
	if secret == "" {
		return "", errors.New("secret file is empty")
	}
	return secret, nil
}

var sizeUnits = []string{"KiB", "MiB", "GiB"} // unit i scales by 1<<(10*(i+1))

// parseSize reads a byte count with an optional binary suffix, rejecting anything that does not fit an int64.
func parseSize(s string) (int64, error) {
	shift := 0
	for i, unit := range sizeUnits {
		if before, ok := strings.CutSuffix(s, unit); ok {
			s, shift = before, 10*(i+1)
			break
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	switch {
	case errors.Is(err, strconv.ErrRange) || err == nil && n > math.MaxInt64>>shift:
		return 0, errors.New("size overflows int64")
	case err != nil:
		return 0, errors.New("expected bytes with an optional KiB, MiB or GiB suffix")
	}
	return int64(n) << shift, nil
}

// sizeValue is a flag.Value for byte sizes that prints exact multiples in their unit.
type sizeValue int64

func (v *sizeValue) Set(s string) error {
	n, err := parseSize(s)
	if err == nil {
		*v = sizeValue(n)
	}
	return err
}

func (v *sizeValue) String() string {
	n := int64(*v)
	for i, sizeUnit := range slices.Backward(sizeUnits) {
		if shift := 10 * (i + 1); n != 0 && n%(1<<shift) == 0 {
			return strconv.FormatInt(n>>shift, 10) + sizeUnit
		}
	}
	return strconv.FormatInt(n, 10)
}
