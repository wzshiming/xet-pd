package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// backlogged returns a peer whose only pull slot is taken by a cold GET of a; the gateway holds a before its headers until release and serves b at once.
func backlogged(t *testing.T) (*Peer, *httptest.Server, *clustertest.FakeGateway, xet.XorbHash, func() result) {
	t.Helper()
	a, bodyA := clustertest.MakeXorb(t, 80, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 81, 2, 8<<10)
	gate := make(chan struct{})
	serve := clustertest.ServeByHash(map[xet.XorbHash][]byte{a: bodyA, b: bodyB})
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if path.Base(r.URL.Path) == a.String() {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		serve(w, r)
	})
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.MaxPulls, c.Heartbeat = 1, time.Hour })
	lateA := doAsync(t, xorbURL(srv, a))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a holding the only pull slot", func() bool { return pullOf(p, a) != nil && len(g.Requests()) == 1 })
	return p, srv, g, b, func() result {
		close(gate)
		return <-lateA
	}
}

// clientGet starts a GET of hash in p's handler; leave cancels the client and returns once the handler did.
func clientGet(t *testing.T, p *Peer, hash xet.XorbHash) (leave func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/xorbs/default/"+hash.String(), nil).WithContext(ctx))
	}()
	leave = func() {
		cancel()
		<-done
	}
	t.Cleanup(leave)
	return leave
}

func postPull(t *testing.T, srv *httptest.Server, hash xet.XorbHash) int {
	t.Helper()
	resp, err := http.Post(srv.URL+apis.PullPath, "application/json", strings.NewReader(`{"hash":"`+hash.String()+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func pullOf(p *Peer, hash xet.XorbHash) *pull {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pulls[hash]
}

func admitted(p *Peer, hash xet.XorbHash) func() bool {
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.xorbs.Get(hash) != nil
	}
}

func TestQueuedPullDroppedWhenClientLeaves(t *testing.T) {
	p, srv, g, b, release := backlogged(t)
	leave := clientGet(t, p, b)
	var fb *pull
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool {
		fb = pullOf(p, b)
		return fb != nil
	})
	leave()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b to leave p.pulls after its only client left", func() bool { return pullOf(p, b) == nil })
	select {
	case <-fb.done:
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("dropped pull of b never finished")
	}
	p.mu.Lock()
	_, cooling := p.cooldown[b]
	p.mu.Unlock()
	if cooling || len(g.Requests()) != 1 {
		t.Fatalf("after dropping b: cooling %v, gateway GETs %d, want false and 1", cooling, len(g.Requests()))
	}
	if r := release(); r.err != nil || r.resp.StatusCode != http.StatusOK {
		t.Fatalf("GET a = %d, %v, want 200", r.resp.StatusCode, r.err)
	}
	if resp, _, err := do(t, http.MethodGet, xorbURL(srv, b)); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET b after the drop = %d, %v, want 200", resp.StatusCode, err)
	}
	if n := len(g.Requests()); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2", n)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
}

// The gateway counts on an accepted pull, so it waits out the queue with no client attached.
func TestQueuedGatewayPullKeptWithoutClients(t *testing.T) {
	p, srv, g, b, release := backlogged(t)
	if code := postPull(t, srv, b); code != http.StatusAccepted {
		t.Fatalf("pull b = %d, want 202", code)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool { return pullOf(p, b) != nil })
	time.Sleep(300 * time.Millisecond)
	p.mu.Lock()
	queued := p.pulls[b] != nil
	_, cooling := p.cooldown[b]
	p.mu.Unlock()
	if !queued || cooling {
		t.Fatalf("gateway pull of b 300ms into the queue: queued %v, cooling %v, want true and false", queued, cooling)
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
	if n := len(g.Requests()); n != 2 {
		t.Fatalf("gateway GETs = %d, want 2", n)
	}
}

func TestQueuedGatewayPullKeptWhenJoinedClientLeaves(t *testing.T) {
	p, srv, _, b, release := backlogged(t)
	if code := postPull(t, srv, b); code != http.StatusAccepted {
		t.Fatalf("pull b = %d, want 202", code)
	}
	leave := clientGet(t, p, b)
	// The pull goroutine holds one reference, the joined client the other.
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a client joining the pull of b", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.pulls[b] != nil && p.pulls[b].refs == 2
	})
	leave()
	time.Sleep(300 * time.Millisecond)
	if pullOf(p, b) == nil {
		t.Fatal("gateway pull of b dropped when a joined client left")
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
}

func TestQueuedPullKeptOnceGatewayJoins(t *testing.T) {
	p, srv, _, b, release := backlogged(t)
	leave := clientGet(t, p, b)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool { return pullOf(p, b) != nil })
	if code := postPull(t, srv, b); code != http.StatusAccepted {
		t.Fatalf("pull b = %d, want 202", code)
	}
	leave()
	time.Sleep(300 * time.Millisecond)
	if pullOf(p, b) == nil {
		t.Fatal("pull of b dropped when its client left after the gateway joined it")
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
}

func TestQueuedPullUnlistedBeforeHandlerReturns(t *testing.T) {
	p, _, _, b, release := backlogged(t)
	// Repeated since a late unlisting by the pull's goroutine can still beat a single check.
	for i := range 50 {
		leave := clientGet(t, p, b)
		var fb *pull
		clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool {
			fb = pullOf(p, b)
			return fb != nil
		})
		leave()
		if got := pullOf(p, b); got != nil {
			t.Fatalf("iteration %d: p.pulls[b] = %p once its only client's handler returned, want nil", i, got)
		}
		select {
		case <-fb.done:
		case <-time.After(clustertest.WaitTimeout):
			t.Fatalf("iteration %d: dropped pull of b never finished", i)
		}
		p.mu.Lock()
		_, cooling := p.cooldown[b]
		p.mu.Unlock()
		if cooling {
			t.Fatalf("iteration %d: b cooling after its dropped pull ended", i)
		}
	}
	release()
}

func TestQueuedPullDropKeepsNewerPull(t *testing.T) {
	p, _, _, b, release := backlogged(t)
	leave := clientGet(t, p, b)
	var fb *pull
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool {
		fb = pullOf(p, b)
		return fb != nil
	})
	newer := &pull{} // stands in for a pull of b listed after fb was dropped
	p.mu.Lock()
	p.pulls[b] = newer
	p.mu.Unlock()
	t.Cleanup(func() {
		p.mu.Lock()
		delete(p.pulls, b)
		p.mu.Unlock()
	})
	leave()
	select {
	case <-fb.done:
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("dropped pull of b never finished")
	}
	if got := pullOf(p, b); got != newer {
		t.Fatalf("p.pulls[b] = %p once the dropped pull of b ended, want the newer pull %p", got, newer)
	}
	release()
}
