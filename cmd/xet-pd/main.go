// Command xet-pd runs the gateway (a hub mirror whose xorbs live on peers) or a seed peer.
package main

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
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wzshiming/xet-pd/gateway"
	"github.com/wzshiming/xet-pd/peer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `Usage: xet-pd <command> [flags]

Commands:
  gateway  hub mirror that serves xorbs from registered peers
  peer     seed that caches and serves xorbs for a gateway

Run "xet-pd <command> -h" for the flags of a command.
`)
}

// run executes the command line and returns the process exit code: 2 usage, 1 runtime failure, 0 otherwise.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	name := args[0]
	var (
		listen string
		build  func() (service, error)
		err    error
	)
	switch name {
	case "gateway":
		var cfg gateway.Config
		listen, cfg, err = parseGateway(args[1:], getenv, stdout)
		build = func() (service, error) { return gateway.New(cfg) }
	case "peer":
		var cfg peer.Config
		listen, cfg, err = parsePeer(args[1:], getenv, stdout)
		build = func() (service, error) { return peer.New(cfg) }
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "xet-pd: unknown command %q\n", name)
		usage(stderr)
		return 2
	}
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case err != nil:
		_, _ = fmt.Fprintf(stderr, "xet-pd %s: %v\nRun \"xet-pd %s -h\" for flags.\n", name, err, name)
		return 2
	}
	svc, err := build()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "xet-pd %s: %v\n", name, err)
		return 1
	}
	defer func() { _ = svc.Close() }()
	if err := serveHTTP(ctx, listen, svc, stdout, "xet-pd "+name); err != nil {
		_, _ = fmt.Fprintf(stderr, "xet-pd %s: %v\n", name, err)
		return 1
	}
	return 0
}

type service interface {
	http.Handler
	Close() error
}

var shutdownTimeout = 10 * time.Second

// serveHTTP serves handler on listen until ctx is done, then drains connections for shutdownTimeout and force-closes the rest.
func serveHTTP(ctx context.Context, listen string, handler http.Handler, stdout io.Writer, banner string) error {
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

// envSet is a FlagSet whose flags fall back to environment variables when absent from the command line.
type envSet struct {
	*flag.FlagSet
	env [][2]string // flag name, environment key; earlier keys win
}

func newEnvSet(name string) *envSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return &envSet{FlagSet: fs}
}

func (s *envSet) bind(name, usage string, keys []string) string {
	for _, key := range keys {
		s.env = append(s.env, [2]string{name, key})
	}
	return usage + " [env " + strings.Join(keys, ", ") + "]"
}

func (s *envSet) StringVar(p *string, name, value, usage string, keys ...string) {
	s.FlagSet.StringVar(p, name, value, s.bind(name, usage, keys))
}

func (s *envSet) IntVar(p *int, name string, value int, usage string, keys ...string) {
	s.FlagSet.IntVar(p, name, value, s.bind(name, usage, keys))
}

func (s *envSet) DurationVar(p *time.Duration, name string, value time.Duration, usage string, keys ...string) {
	s.FlagSet.DurationVar(p, name, value, s.bind(name, usage, keys))
}

func (s *envSet) SizeVar(p *int64, name string, value int64, usage string, keys ...string) {
	*p = value
	s.Var((*sizeValue)(p), name, s.bind(name, usage, keys))
}

// parse applies the command line, then the environment to flags left unset, and rejects positional arguments; -h prints the usage to help.
func (s *envSet) parse(args []string, getenv func(string) string, help io.Writer) error {
	if err := s.Parse(args); err != nil {
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

// secretFlags registers -secret and -secret-file; their environment fallback is resolved by resolveSecret.
func secretFlags(fs *flag.FlagSet) (secret, file *string) {
	secret = fs.String("secret", "", "bearer token shared by the gateway and its peers [env XET_PD_SECRET]")
	file = fs.String("secret-file", "", "file holding the shared secret, surrounding whitespace trimmed [env XET_PD_SECRET_FILE]")
	return secret, file
}

// resolveSecret prefers the flags over the environment; a literal and a file from the same source are an error.
func resolveSecret(flags *flag.FlagSet, secret, file string, getenv func(string) string) (string, error) {
	explicit := false
	flags.Visit(func(option *flag.Flag) {
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
	return strings.TrimSpace(string(b)), nil
}

// parseGateway resolves the gateway command line into the listen address and configuration.
func parseGateway(args []string, getenv func(string) string, help io.Writer) (string, gateway.Config, error) {
	var (
		listen string
		cfg    gateway.Config
	)
	fs := newEnvSet("xet-pd gateway")
	fs.StringVar(&listen, "listen", ":8080", "listen address", "XET_PD_LISTEN")
	fs.StringVar(&cfg.StorageDir, "storage-dir", "./xet-pd-gateway", "data directory", "XET_PD_STORAGE_DIR")
	fs.StringVar(&cfg.Upstream, "upstream", "https://huggingface.co", "upstream hub origin, scheme://host[:port]", "XET_PD_UPSTREAM")
	fs.StringVar(&cfg.UpstreamToken, "upstream-token", "", "bearer token for the upstream hub", "XET_PD_UPSTREAM_TOKEN", "HF_TOKEN")
	fs.StringVar(&cfg.SigningKey, "signing-key", "", "HMAC key of CAS read tokens; empty leaves CAS routes open", "XET_PD_SIGNING_KEY")
	fs.StringVar(&cfg.ExternalURL, "external-url", "", "origin advertised in hub links; empty derives it from each request", "XET_PD_EXTERNAL_URL")
	fs.DurationVar(&cfg.PeerTTL, "peer-ttl", 30*time.Second, "validity of a peer announce", "XET_PD_PEER_TTL")
	fs.IntVar(&cfg.Replicas, "replicas", 2, "peers asked to hold each xorb", "XET_PD_REPLICAS")
	fs.IntVar(&cfg.MinSeeds, "min-seeds", 1, "live holders before the local xorb copy may go", "XET_PD_MIN_SEEDS")
	fs.SizeVar(&cfg.XorbCacheSize, "xorb-cache-size", 8<<30, "bytes of local xorbs kept, 0 unlimited", "XET_PD_XORB_CACHE_SIZE")
	secret, secretFile := secretFlags(fs.FlagSet)
	if err := fs.parse(args, getenv, help); err != nil {
		return "", gateway.Config{}, err
	}
	if cfg.Upstream == "" {
		return "", gateway.Config{}, errors.New("upstream is required")
	}
	var err error
	cfg.Secret, err = resolveSecret(fs.FlagSet, *secret, *secretFile, getenv)
	return listen, cfg, err
}

// parsePeer resolves the peer command line into the listen address and configuration.
func parsePeer(args []string, getenv func(string) string, help io.Writer) (string, peer.Config, error) {
	var (
		listen string
		cfg    peer.Config
	)
	fs := newEnvSet("xet-pd peer")
	fs.StringVar(&listen, "listen", ":8081", "listen address", "XET_PD_LISTEN")
	fs.StringVar(&cfg.Advertise, "advertise", "", "public origin of this peer, scheme://host[:port] (required)", "XET_PD_ADVERTISE")
	fs.StringVar(&cfg.Gateway, "gateway", "", "gateway origin, scheme://host[:port] (required)", "XET_PD_GATEWAY")
	fs.StringVar(&cfg.Dir, "cache-dir", "./xet-pd-peer", "xorb cache directory", "XET_PD_CACHE_DIR")
	fs.SizeVar(&cfg.CacheSize, "cache-size", 0, "bytes of xorbs kept, 0 unlimited", "XET_PD_CACHE_SIZE")
	fs.IntVar(&cfg.MaxFills, "max-fills", 8, "parallel fills from the gateway", "XET_PD_MAX_FILLS")
	fs.DurationVar(&cfg.FillTimeout, "fill-timeout", 10*time.Minute, "time limit of one fill, queueing included", "XET_PD_FILL_TIMEOUT")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", 10*time.Second, "announce interval, keep it below the gateway peer ttl", "XET_PD_HEARTBEAT")
	secret, secretFile := secretFlags(fs.FlagSet)
	if err := fs.parse(args, getenv, help); err != nil {
		return "", peer.Config{}, err
	}
	switch {
	case cfg.Advertise == "":
		return "", peer.Config{}, errors.New("advertise is required")
	case cfg.Gateway == "":
		return "", peer.Config{}, errors.New("gateway is required")
	}
	var err error
	cfg.Secret, err = resolveSecret(fs.FlagSet, *secret, *secretFile, getenv)
	return listen, cfg, err
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
