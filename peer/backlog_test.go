package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// backlogged returns a peer whose only pull slot is taken by a cold GET of a; the holder holds a's body until release and serves b at once.
func backlogged(t *testing.T) (*Peer, *httptest.Server, *clustertest.FakePeer, xet.XorbHash, func() result) {
	t.Helper()
	a, bodyA := clustertest.MakeXorb(t, 80, 2, 8<<10)
	b, bodyB := clustertest.MakeXorb(t, 81, 2, 8<<10)
	gate := make(chan struct{})
	tracker, holder := cluster(t, map[xet.XorbHash][]byte{a: bodyA, b: bodyB})
	holder.GateXorb(a, bodyA, 0, gate)
	p, srv := newTestPeer(t, tracker.URL, func(c *Config) { c.MaxPulls, c.Heartbeat = 1, time.Hour })
	lateA := doAsync(t, xorbURL(srv, a))
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a holding the only pull slot", func() bool { return pullOf(p, a) != nil && len(holder.Requests()) == 1 })
	return p, srv, holder, b, func() result {
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

func TestQueuedPullDroppedWhenClientLeaves(t *testing.T) {
	p, srv, holder, b, release := backlogged(t)
	leave := clientGet(t, p, b)
	var fb *pull
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool {
		fb = pullOf(p, b)
		return fb != nil
	})
	leave()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b to leave p.pulls after its only client left", func() bool { return pullOf(p, b) == nil })
	select {
	case <-pullDone(fb):
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("dropped pull of b never finished")
	}
	_, cooling := cooldownOf(p, b)
	if cooling || len(holder.Requests()) != 1 {
		t.Fatalf("after dropping b: cooling %v, holder GETs %d, want false and 1", cooling, len(holder.Requests()))
	}
	if r := release(); r.err != nil || r.resp.StatusCode != http.StatusOK {
		t.Fatalf("GET a = %d, %v, want 200", r.resp.StatusCode, r.err)
	}
	if resp, _, err := do(t, http.MethodGet, xorbURL(srv, b)); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET b after the drop = %d, %v, want 200", resp.StatusCode, err)
	}
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
}

// A file pull counts on its xorbs' pulls, so a forced one waits out the queue with no client attached.
func TestQueuedForcedPullKeptWithoutClients(t *testing.T) {
	p, _, holder, b, release := backlogged(t)
	forcePull(t, p, b)
	// forcePull's release settled any drop under the store's lock.
	fb, _, queued, keep := pullStatus(p, b)
	kept := fb != nil && queued && keep
	_, cooling := cooldownOf(p, b)
	if !kept || cooling {
		t.Fatalf("forced pull of b: queued and kept %v, cooling %v, want true and false", kept, cooling)
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
}

func TestQueuedForcedPullKeptWhenJoinedClientLeaves(t *testing.T) {
	p, _, holder, b, release := backlogged(t)
	forcePull(t, p, b)
	leave := clientGet(t, p, b)
	// The pull goroutine holds one reference, the joined client the other.
	var fb *pull
	clustertest.WaitFor(t, clustertest.WaitTimeout, "a client joining the pull of b", func() bool {
		var refs int
		fb, refs, _, _ = pullStatus(p, b)
		return fb != nil && refs == 2
	})
	leave()
	// leave returns after the handler's release, which settles any drop under p.mu.
	f, _, queued, keep := pullStatus(p, b)
	kept := f == fb && queued && keep
	if !kept {
		t.Fatal("forced pull of b dropped when a joined client left")
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
}

func TestQueuedPullKeptOnceForced(t *testing.T) {
	p, _, holder, b, release := backlogged(t)
	leave := clientGet(t, p, b)
	var fb *pull
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b queued behind a", func() bool {
		fb = pullOf(p, b)
		return fb != nil
	})
	forcePull(t, p, b)
	leave()
	// leave returns after the handler's release, which settles any drop under p.mu.
	f, _, queued, keep := pullStatus(p, b)
	kept := f == fb && queued && keep
	if !kept {
		t.Fatal("pull of b dropped when its client left after it was forced")
	}
	release()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "b committed", admitted(p, b))
	if n := len(holder.Requests()); n != 2 {
		t.Fatalf("holder GETs = %d, want 2", n)
	}
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
		case <-pullDone(fb):
		case <-time.After(clustertest.WaitTimeout):
			t.Fatalf("iteration %d: dropped pull of b never finished", i)
		}
		_, cooling := cooldownOf(p, b)
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
	newer := plantPull(t, p, b) // stands in for a pull of b listed after fb was dropped
	leave()
	select {
	case <-pullDone(fb):
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("dropped pull of b never finished")
	}
	if got := pullOf(p, b); got != newer {
		t.Fatalf("p.pulls[b] = %p once the dropped pull of b ended, want the newer pull %p", got, newer)
	}
	release()
}
