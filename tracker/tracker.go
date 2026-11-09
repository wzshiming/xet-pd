// Package tracker implements the xet-pd tracker: an in-memory record of which live peers hold which xorbs and complete files, answering lookups and asking peers to seed files.
package tracker

import (
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

// Config configures New; zero values take the noted defaults.
type Config struct {
	Secret   string        // bearer token of announces, lookups and pull requests
	PeerTTL  time.Duration // announce validity, default 30s, at least 1s
	Replicas int           // complete holders asked for per file, default 2
	MinSeeds int           // other live holders a seed copy needs before a peer may evict it, default 1
}

// Values New applies to zero fields; cmd/tracker shows them as flag defaults.
const (
	DefaultPeerTTL  = 30 * time.Second
	DefaultReplicas = 2
	DefaultMinSeeds = 1
)

// normalize validates cfg and fills in the defaults.
func (cfg *Config) normalize() error {
	if cfg.PeerTTL < 0 || cfg.Replicas < 0 || cfg.MinSeeds < 0 {
		return errors.New("negative config value")
	}
	cfg.PeerTTL = cmp.Or(cfg.PeerTTL, DefaultPeerTTL)
	if cfg.PeerTTL < time.Second {
		return errors.New("peer ttl below 1s")
	}
	cfg.Replicas = cmp.Or(cfg.Replicas, DefaultReplicas)
	cfg.MinSeeds = cmp.Or(cfg.MinSeeds, DefaultMinSeeds)
	if cfg.MinSeeds > cfg.Replicas {
		return errors.New("min seeds exceed replicas")
	}
	return nil
}

// Tracker is the xet-pd coordinator; it holds no xorb bytes.
type Tracker struct {
	mux     *http.ServeMux
	reg     *registry
	seeder  *seeder
	secret  string
	metrics *trackerMetrics
}

// New composes the registry and the seeder behind the control routes; it touches no disk and opens no network.
func New(cfg Config) (*Tracker, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	m := newTrackerMetrics()
	reg := newRegistry(cfg.PeerTTL, cfg.MinSeeds)
	tr := &Tracker{
		mux:     http.NewServeMux(),
		reg:     reg,
		seeder:  newSeeder(reg, seedConfig{replicas: cfg.Replicas, secret: cfg.Secret, metrics: m}),
		secret:  cfg.Secret,
		metrics: m,
	}
	m.reg.MustRegister(trackerState{tr})
	tr.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	tr.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	tr.mux.Handle("GET /metrics", m.handler())
	tr.mux.HandleFunc(apis.AnnouncePath, route(http.MethodPost, tr.serveAnnounce))
	tr.mux.HandleFunc(apis.LookupPath, route(http.MethodPost, tr.serveLookup))
	tr.mux.HandleFunc(apis.PeersPath, route(http.MethodGet, tr.servePeers))
	return tr, nil
}

func (tr *Tracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tr.mux.ServeHTTP(w, r)
}

// Close stops seeding and waits for the pull requests in flight; later calls do nothing.
func (tr *Tracker) Close() error {
	return tr.seeder.Close()
}

// route answers a control route for one method and 405 otherwise.
func route(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

func (tr *Tracker) servePeers(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(apis.PeersResponse{Peers: tr.reg.snapshot()})
}
