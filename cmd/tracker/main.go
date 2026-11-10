// Command tracker runs the xet-pd tracker: it records which live peers hold which xorbs and complete files and asks peers to seed.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/wzshiming/xet-pd/internal/cli"
	"github.com/wzshiming/xet-pd/tracker"
)

var command = cli.Command[tracker.Config]{
	Name:  "tracker",
	Parse: parse,
	New:   newService,
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first signal restores default handling, so a second one kills the process mid-drain.
	context.AfterFunc(ctx, stop)
	code := command.Run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// parse resolves the tracker command line into the settings and configuration.
func parse(args []string, getenv func(string) string, help io.Writer) (cli.Settings, tracker.Config, error) {
	var (
		s   cli.Settings
		cfg tracker.Config
	)
	fs := cli.NewFlagSet("tracker")
	fs.StringVar(&s.Listen, "listen", ":8080", "listen address", "XET_PD_LISTEN")
	fs.LogFormatVar(&s.LogFormat)
	fs.DurationVar(&cfg.PeerTTL, "peer-ttl", tracker.DefaultPeerTTL, "seconds a peer stays registered after its last announce", "XET_PD_PEER_TTL")
	fs.IntVar(&cfg.Replicas, "replicas", tracker.DefaultReplicas, "seed copies the tracker asks for per complete file", "XET_PD_REPLICAS")
	fs.IntVar(&cfg.MinSeeds, "min-seeds", tracker.DefaultMinSeeds, "other live holders a seed copy needs before a peer may evict it", "XET_PD_MIN_SEEDS")
	secret, secretFile := fs.SecretFlags()
	if err := fs.Parse(args, getenv, help); err != nil {
		return cli.Settings{}, tracker.Config{}, err
	}
	var err error
	cfg.Secret, err = fs.ResolveSecret(*secret, *secretFile, getenv)
	return s, cfg, err
}

func newService(cfg tracker.Config) (cli.Service, error) {
	tr, err := tracker.New(cfg)
	if err != nil {
		return nil, err
	}
	return tr, nil
}
