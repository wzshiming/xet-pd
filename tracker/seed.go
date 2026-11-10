package tracker

import (
	"cmp"
	"context"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet-pd/apis"
)

// pullTimeout bounds one pull request to a peer.
const pullTimeout = 2 * time.Second

// seedConfig tunes seeding; zero values take the noted defaults.
type seedConfig struct {
	replicas int           // complete holders asked for per file
	secret   string        // bearer token of pull requests
	retry    time.Duration // pause per (file, peer) after a pull request, default 1m
	tick     time.Duration // pass period besides wakes, default 1s
	metrics  *trackerMetrics
}

// maxPulls bounds parallel pull requests.
const maxPulls = 8

// ask is a file and the peer asked to pull it.
type ask struct{ file, peer string }

// seeder asks live peers to pull complete files until each has replicas complete holders.
type seeder struct {
	cfg    seedConfig
	reg    *registry
	client *http.Client // carries the secret, never follows redirects
	wake   chan struct{}
	sem    chan struct{} // bounds in-flight pull requests
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu    sync.Mutex          // guards want and asked; ordered before reg.mu, never held across network calls
	want  map[string]struct{} // files that may lack complete holders
	asked map[ask]time.Time   // when each ask may be repeated
}

func newSeeder(reg *registry, cfg seedConfig) *seeder {
	cfg.retry = cmp.Or(cfg.retry, time.Minute)
	cfg.tick = cmp.Or(cfg.tick, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	s := &seeder{
		cfg:    cfg,
		reg:    reg,
		client: apis.NewClient(apis.NewTransport()),
		wake:   make(chan struct{}, 1),
		sem:    make(chan struct{}, maxPulls),
		ctx:    ctx,
		cancel: cancel,
		want:   map[string]struct{}{},
		asked:  map[ask]time.Time{},
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// run is the seeding goroutine: one pass per tick or wake until Close.
func (s *seeder) run() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.tick)
	defer tick.Stop()
	for {
		s.seed()
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		case <-s.wake:
		}
	}
}

// note adds files to the wanted ones and wakes a pass.
func (s *seeder) note(files []string) {
	s.mu.Lock()
	for _, f := range files {
		s.want[f] = struct{}{}
	}
	s.mu.Unlock()
	s.kick()
}

// seed asks the top-ranked live non-holders of each wanted file to pull it until every pull slot is busy; a file leaves want once it has no complete holder or replicas of them.
func (s *seeder) seed() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for a, next := range s.asked {
		if !now.Before(next) {
			delete(s.asked, a)
		}
	}
seeding:
	for f := range s.want {
		holders := s.reg.fileHolders(f)
		if len(holders) == 0 || len(holders) >= s.cfg.replicas {
			delete(s.want, f)
			continue
		}
		need := s.cfg.replicas - len(holders)
		for _, p := range s.reg.rank(f) {
			if need == 0 {
				break
			}
			if slices.ContainsFunc(holders, func(h apis.Peer) bool { return h.ID == p.ID }) {
				continue
			}
			need--
			a := ask{file: f, peer: p.ID}
			if _, ok := s.asked[a]; ok {
				continue
			}
			select {
			case s.sem <- struct{}{}:
			default:
				break seeding
			}
			s.asked[a] = now.Add(s.cfg.retry)
			s.wg.Add(1)
			go s.pull(p.Peer, f)
		}
	}
}

// pull asks p to pull file as a seed copy; only 202 counts, failures are logged unless the seeder is closing.
func (s *seeder) pull(p apis.Peer, file string) {
	defer s.wg.Done()
	defer s.kick()
	defer func() { <-s.sem }()
	ctx, cancel := context.WithTimeout(s.ctx, pullTimeout)
	err := apis.Pull(ctx, s.client, p.URL, s.cfg.secret, apis.PullRequest{File: file, Seed: true})
	cancel()
	s.cfg.metrics.seedRequest(err == nil)
	if err != nil && s.ctx.Err() == nil {
		slog.Warn("tracker: seed request", "peer", p.ID, "file", file, "err", err)
	}
}

// kick schedules a pass without blocking; safe after Close.
func (s *seeder) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Close stops seeding, cancels the pull requests in flight and waits for them.
func (s *seeder) Close() error {
	s.once.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.client.CloseIdleConnections()
	})
	return nil
}
