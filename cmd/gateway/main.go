// Command gateway runs the xet-pd gateway: a hub mirror whose xorbs live on peers.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/rest"

	"github.com/wzshiming/xet-pd/gateway"
	"github.com/wzshiming/xet-pd/internal/cli"
	"github.com/wzshiming/xet-pd/internal/podnode"
)

var command = cli.Command[config]{
	Name:  "gateway",
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

// parse resolves the gateway command line into the settings and configuration.
func parse(args []string, getenv func(string) string, help io.Writer) (cli.Settings, config, error) {
	var (
		s   cli.Settings
		cfg config
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
	cfg.locality = localityNone
	fs.Var(&cfg.locality, "locality", "how client nodes are learned: none, or k8s to watch pods through the API server and prefer the peer on the client's node", "XET_PD_LOCALITY")
	secret, secretFile := fs.SecretFlags()
	if err := fs.Parse(args, getenv, help); err != nil {
		return cli.Settings{}, config{}, err
	}
	if cfg.Upstream == "" {
		return cli.Settings{}, config{}, errors.New("upstream is required")
	}
	var err error
	cfg.Secret, err = fs.ResolveSecret(*secret, *secretFile, getenv)
	return s, cfg, err
}

type config struct {
	gateway.Config
	locality localityMode
}

// localityMode is a flag.Value naming how the gateway learns the node of a client.
type localityMode string

const (
	localityNone localityMode = "none"
	localityK8s  localityMode = "k8s"
)

func (m *localityMode) Set(s string) error {
	switch v := localityMode(s); v {
	case localityNone, localityK8s:
		*m = v
		return nil
	}
	return errors.New("expected none or k8s")
}

func (m *localityMode) String() string {
	return string(*m)
}

type service struct {
	*gateway.Gateway
	locality *podnode.Index
}

func (s service) Close() error {
	err := s.Gateway.Close()
	if s.locality != nil {
		s.locality.Close()
	}
	return err
}

func newService(c config) (cli.Service, error) {
	var idx *podnode.Index
	if c.locality == localityK8s {
		rc, err := rest.InClusterConfig()
		if err == nil {
			idx, err = podnode.New(context.Background(), rc)
		}
		if err != nil {
			return nil, fmt.Errorf("locality k8s: %w", err)
		}
		c.Locality = idx
	}
	g, err := gateway.New(c.Config)
	if err != nil {
		if idx != nil {
			idx.Close()
		}
		return nil, err
	}
	return service{Gateway: g, locality: idx}, nil
}
