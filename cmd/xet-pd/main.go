// Command xet-pd delivers Hugging Face content over the HF_ENDPOINT protocol.
// The agent subcommand is a transparent cache composed from the xet mirror
// facade and CAS server (S0); with --peers it prefers files already held by
// sibling agents over the origin (S1). The tracker subcommand runs the
// cluster content index that replaces broadcast peer probing (S2).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wzshiming/xet/token"

	"github.com/wzshiming/xet-pd/agent"
	"github.com/wzshiming/xet-pd/tracker"
)

func usage(w io.Writer) {
	_, _ = fmt.Fprintf(w, `Usage: xet-pd <command> [flags]

Commands:
  agent    run the transparent-cache agent (HF_ENDPOINT facade + CAS)
  tracker  run the cluster content index (announce/lookup)

Run "xet-pd <command> -h" for command flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "agent":
		if err := runAgent(os.Args[2:], os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "xet-pd agent: %v\n", err)
			os.Exit(1)
		}
	case "tracker":
		if err := runTracker(os.Args[2:], os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "xet-pd tracker: %v\n", err)
			os.Exit(1)
		}
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
}

// envOr returns the first non-empty environment value among keys, or def.
func envOr(getenv func(string) string, def string, keys ...string) string {
	for _, key := range keys {
		if v := getenv(key); v != "" {
			return v
		}
	}
	return def
}

// splitPeers parses a comma-separated peer list, dropping empty items.
func splitPeers(s string) []string {
	var peers []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, p)
		}
	}
	return peers
}

// parseAgent resolves the agent flags with environment fallback (a set flag
// wins over its environment variable) into the listen address and agent
// configuration. A cluster secret file wins over the literal secret.
func parseAgent(args []string, getenv func(string) string) (string, agent.Config, error) {
	fs := flag.NewFlagSet("xet-pd agent", flag.ExitOnError)
	listen := fs.String("listen", envOr(getenv, ":8880", "XET_PD_LISTEN"),
		"Listen address (host:port) [env XET_PD_LISTEN]")
	upstream := fs.String("upstream", envOr(getenv, "https://huggingface.co", "XET_PD_UPSTREAM"),
		"Upstream hub base URL [env XET_PD_UPSTREAM]")
	upstreamToken := fs.String("upstream-token", envOr(getenv, "", "XET_PD_UPSTREAM_TOKEN", "HF_TOKEN"),
		"Bearer token used against the upstream hub [env XET_PD_UPSTREAM_TOKEN, HF_TOKEN]")
	storageDir := fs.String("storage-dir", envOr(getenv, "./xet-pd", "XET_PD_STORAGE_DIR"),
		"Root data directory [env XET_PD_STORAGE_DIR]")
	externalURL := fs.String("external-url", envOr(getenv, "", "XET_PD_EXTERNAL_URL"),
		"Externally visible base URL; empty derives it per request [env XET_PD_EXTERNAL_URL]")
	clusterSecret := fs.String("cluster-secret", envOr(getenv, "", "XET_PD_CLUSTER_SECRET"),
		"Token issuer secret shared across the cluster; empty generates a per-process one [env XET_PD_CLUSTER_SECRET]")
	clusterSecretFile := fs.String("cluster-secret-file", envOr(getenv, "", "XET_PD_CLUSTER_SECRET_FILE"),
		"File holding the cluster secret; wins over -cluster-secret [env XET_PD_CLUSTER_SECRET_FILE]")
	peers := fs.String("peers", envOr(getenv, "", "XET_PD_PEERS"),
		"Comma-separated sibling agent base URLs probed before the origin [env XET_PD_PEERS]")
	trackerURL := fs.String("tracker", envOr(getenv, "", "XET_PD_TRACKER"),
		"Tracker base URL for peer discovery and announces; empty disables it [env XET_PD_TRACKER]")
	peerTimeout := fs.Duration("peer-timeout", 2*time.Second,
		"Timeout for each peer readiness probe [env XET_PD_PEER_TIMEOUT]")
	maxPeerConns := fs.Int("max-peer-conns", 64,
		"Connection cap per peer host [env XET_PD_MAX_PEER_CONNS]")
	if v := getenv("XET_PD_PEER_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return "", agent.Config{}, fmt.Errorf("parse XET_PD_PEER_TIMEOUT: %w", err)
		}
		*peerTimeout = d
	}
	if v := getenv("XET_PD_MAX_PEER_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return "", agent.Config{}, fmt.Errorf("parse XET_PD_MAX_PEER_CONNS: %w", err)
		}
		*maxPeerConns = n
	}
	if err := fs.Parse(args); err != nil {
		return "", agent.Config{}, err
	}
	if *peerTimeout <= 0 {
		return "", agent.Config{}, fmt.Errorf("peer-timeout must be positive, got %v", *peerTimeout)
	}
	if *maxPeerConns <= 0 {
		return "", agent.Config{}, fmt.Errorf("max-peer-conns must be positive, got %d", *maxPeerConns)
	}

	secret := *clusterSecret
	if *clusterSecretFile != "" {
		b, err := os.ReadFile(*clusterSecretFile)
		if err != nil {
			return "", agent.Config{}, fmt.Errorf("read cluster secret file: %w", err)
		}
		secret = strings.TrimSpace(string(b))
	}

	return *listen, agent.Config{
		Upstream:      *upstream,
		UpstreamToken: *upstreamToken,
		StorageDir:    *storageDir,
		ExternalURL:   *externalURL,
		ClusterSecret: secret,
		Peers:         splitPeers(*peers),
		Tracker:       *trackerURL,
		PeerTimeout:   *peerTimeout,
		MaxPeerConns:  *maxPeerConns,
	}, nil
}

// runAgent starts the agent HTTP server and shuts it down gracefully on
// SIGINT/SIGTERM.
func runAgent(args []string, getenv func(string) string) error {
	listen, cfg, err := parseAgent(args, getenv)
	if err != nil {
		return err
	}
	a, err := agent.New(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = a.Close()
	}()
	return serveHTTP(listen, a,
		fmt.Sprintf("xet-pd agent listening on %s, upstream %s", listen, cfg.Upstream))
}

// trackerFlags holds the parsed tracker command line, one field per flag.
type trackerFlags struct {
	listen        string
	storageDir    string
	clusterSecret string
}

// parseTracker resolves the tracker flags with environment fallback (a set
// flag wins over its environment variable). A cluster secret file wins over
// the literal secret.
func parseTracker(args []string, getenv func(string) string) (trackerFlags, error) {
	fs := flag.NewFlagSet("xet-pd tracker", flag.ExitOnError)
	listen := fs.String("listen", envOr(getenv, ":8881", "XET_PD_TRACKER_LISTEN"),
		"Listen address (host:port) [env XET_PD_TRACKER_LISTEN]")
	storageDir := fs.String("storage-dir", envOr(getenv, "./xet-pd", "XET_PD_STORAGE_DIR"),
		"Root data directory; the index lives under tracker/ [env XET_PD_STORAGE_DIR]")
	clusterSecret := fs.String("cluster-secret", envOr(getenv, "", "XET_PD_CLUSTER_SECRET"),
		"Cluster secret validating announce tokens; empty leaves announces open [env XET_PD_CLUSTER_SECRET]")
	clusterSecretFile := fs.String("cluster-secret-file", envOr(getenv, "", "XET_PD_CLUSTER_SECRET_FILE"),
		"File holding the cluster secret; wins over -cluster-secret [env XET_PD_CLUSTER_SECRET_FILE]")
	if err := fs.Parse(args); err != nil {
		return trackerFlags{}, err
	}

	secret := *clusterSecret
	if *clusterSecretFile != "" {
		b, err := os.ReadFile(*clusterSecretFile)
		if err != nil {
			return trackerFlags{}, fmt.Errorf("read cluster secret file: %w", err)
		}
		secret = strings.TrimSpace(string(b))
	}

	return trackerFlags{
		listen:        *listen,
		storageDir:    *storageDir,
		clusterSecret: secret,
	}, nil
}

// runTracker starts the tracker HTTP server and shuts it down gracefully on
// SIGINT/SIGTERM.
func runTracker(args []string, getenv func(string) string) error {
	f, err := parseTracker(args, getenv)
	if err != nil {
		return err
	}
	opts := []tracker.Option{tracker.WithStorageDir(filepath.Join(f.storageDir, "tracker"))}
	if f.clusterSecret != "" {
		issuer, err := token.NewIssuer([]byte(f.clusterSecret), 15*time.Minute)
		if err != nil {
			return fmt.Errorf("create token issuer: %w", err)
		}
		opts = append(opts, tracker.WithAuth(func(tok string) bool {
			return issuer.Validate(tok, time.Now())
		}))
	}
	h, err := tracker.NewHandler(opts...)
	if err != nil {
		return err
	}
	return serveHTTP(f.listen, h,
		fmt.Sprintf("xet-pd tracker listening on %s", f.listen))
}

// serveHTTP runs the handler until SIGINT/SIGTERM, then shuts down
// gracefully.
func serveHTTP(listen string, handler http.Handler, banner string) error {
	srv := &http.Server{Addr: listen, Handler: handler}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	fmt.Println(banner)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
