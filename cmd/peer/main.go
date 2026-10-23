// Command peer runs an xet-pd seed: it caches xorbs pulled through the gateway and serves them.
package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/wzshiming/xet-pd/internal/cli"
	"github.com/wzshiming/xet-pd/peer"
)

var command = cli.Command[peer.Config]{
	Name:  "peer",
	Parse: parse,
	New:   func(cfg peer.Config) (cli.Service, error) { return peer.New(cfg) },
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first signal restores default handling, so a second one kills the process mid-drain.
	context.AfterFunc(ctx, stop)
	code := command.Run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// parse resolves the peer command line into the listen address and configuration.
func parse(args []string, getenv func(string) string, help io.Writer) (string, peer.Config, error) {
	var (
		listen string
		cfg    peer.Config
	)
	fs := cli.NewFlagSet("peer")
	fs.StringVar(&listen, "listen", ":8081", "listen address", "XET_PD_LISTEN")
	fs.StringVar(&cfg.Advertise, "advertise", "", "public origin of this peer, scheme://host[:port] (required)", "XET_PD_ADVERTISE")
	fs.StringVar(&cfg.Gateway, "gateway", "", "gateway origin, scheme://host[:port] (required)", "XET_PD_GATEWAY")
	fs.StringVar(&cfg.DataDir, "data-dir", "./xet-pd-peer", "data directory", "XET_PD_DATA_DIR")
	fs.SizeVar(&cfg.XorbCacheSize, "xorb-cache-size", 8<<30, "bytes of local xorbs kept, 0 unlimited", "XET_PD_XORB_CACHE_SIZE")
	fs.IntVar(&cfg.MaxPulls, "max-pulls", peer.DefaultMaxPulls, "parallel pulls from the gateway", "XET_PD_MAX_PULLS")
	fs.DurationVar(&cfg.PullTimeout, "pull-timeout", peer.DefaultPullTimeout, "time limit of one pull, queueing included", "XET_PD_PULL_TIMEOUT")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", peer.DefaultHeartbeat, "announce interval, keep it below the gateway peer ttl", "XET_PD_HEARTBEAT")
	secret, secretFile := fs.SecretFlags()
	if err := fs.Parse(args, getenv, help); err != nil {
		return "", peer.Config{}, err
	}
	switch {
	case cfg.Advertise == "":
		return "", peer.Config{}, errors.New("advertise is required")
	case cfg.Gateway == "":
		return "", peer.Config{}, errors.New("gateway is required")
	}
	var err error
	cfg.Secret, err = fs.ResolveSecret(*secret, *secretFile, getenv)
	return listen, cfg, err
}
