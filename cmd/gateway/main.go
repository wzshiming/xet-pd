// Command gateway runs the xet-pd gateway: a hub mirror whose xorbs live on peers.
package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/wzshiming/xet-pd/gateway"
	"github.com/wzshiming/xet-pd/internal/cli"
)

var command = cli.Command[gateway.Config]{
	Name:  "gateway",
	Parse: parse,
	New:   func(cfg gateway.Config) (cli.Service, error) { return gateway.New(cfg) },
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first signal restores default handling, so a second one kills the process mid-drain.
	context.AfterFunc(ctx, stop)
	code := command.Run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// parse resolves the gateway command line into the settings and configuration.
func parse(args []string, getenv func(string) string, help io.Writer) (cli.Settings, gateway.Config, error) {
	var (
		s   cli.Settings
		cfg gateway.Config
	)
	fs := cli.NewFlagSet("gateway")
	fs.StringVar(&s.Listen, "listen", ":8080", "listen address", "XET_PD_LISTEN")
	fs.LogFormatVar(&s.LogFormat)
	fs.StringVar(&cfg.DataDir, "data-dir", "./xet-pd-gateway", "data directory", "XET_PD_DATA_DIR")
	fs.StringVar(&cfg.Upstream, "upstream", "https://huggingface.co", "upstream hub origin, scheme://host[:port]", "HF_ENDPOINT")
	fs.StringVar(&cfg.UpstreamToken, "upstream-token", "", "bearer token for the upstream hub", "HF_TOKEN")
	fs.StringVar(&cfg.SigningKey, "signing-key", "", "HMAC key of CAS read tokens; empty leaves CAS routes open", "XET_PD_SIGNING_KEY")
	fs.StringVar(&cfg.ExternalURL, "external-url", "", "origin advertised in hub links; empty derives it from each request", "XET_PD_EXTERNAL_URL")
	fs.DurationVar(&cfg.PeerTTL, "peer-ttl", gateway.DefaultPeerTTL, "validity of a peer announce", "XET_PD_PEER_TTL")
	fs.IntVar(&cfg.Replicas, "replicas", gateway.DefaultReplicas, "peers asked to hold each xorb", "XET_PD_REPLICAS")
	fs.IntVar(&cfg.MinSeeds, "min-seeds", gateway.DefaultMinSeeds, "live holders before the local xorb copy may go", "XET_PD_MIN_SEEDS")
	fs.SizeVar(&cfg.XorbCacheSize, "xorb-cache-size", 8<<30, "bytes of local xorbs kept, 0 unlimited", "XET_PD_XORB_CACHE_SIZE")
	secret, secretFile := fs.SecretFlags()
	if err := fs.Parse(args, getenv, help); err != nil {
		return cli.Settings{}, gateway.Config{}, err
	}
	if cfg.Upstream == "" {
		return cli.Settings{}, gateway.Config{}, errors.New("upstream is required")
	}
	var err error
	cfg.Secret, err = fs.ResolveSecret(*secret, *secretFile, getenv)
	return s, cfg, err
}
