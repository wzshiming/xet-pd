package peer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// beat is the heartbeat of the backoff tests; timers never fire early, so delays are bounded below exactly and above with slack.
const beat = 50 * time.Millisecond

func refuseAnnounce(int) (int, apis.AnnounceResponse) {
	return http.StatusInternalServerError, apis.AnnounceResponse{}
}

// stampAnnounces answers the i-th announce with answer(i) and returns a wait for the arrival times of the first n, which FakeGateway does not keep.
func stampAnnounces(g *clustertest.FakeGateway, answer func(i int) (int, apis.AnnounceResponse)) func(t *testing.T, n int) []time.Time {
	var mu sync.Mutex
	var at []time.Time
	g.SetReply(func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		mu.Lock()
		i := len(at)
		at = append(at, time.Now())
		mu.Unlock()
		return answer(i)
	})
	return func(t *testing.T, n int) []time.Time {
		t.Helper()
		var got []time.Time
		clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("%d announces", n), func() bool {
			mu.Lock()
			defer mu.Unlock()
			got = slices.Clone(at)
			return len(got) >= n
		})
		return got[:n]
	}
}

// logTap keeps what the default logger records while a test runs.
type logTap struct {
	mu      sync.Mutex
	records []slog.Record
}

// tapLogs points the default logger, and the log package behind it, at a logTap until the test ends.
func tapLogs(t *testing.T) *logTap {
	tap := &logTap{}
	prev, out, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(tap))
	t.Cleanup(func() {
		log.SetOutput(out)
		log.SetFlags(flags)
		slog.SetDefault(prev)
	})
	return tap
}

func (l *logTap) Enabled(context.Context, slog.Level) bool { return true }

func (l *logTap) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Clone())
	return nil
}

func (l *logTap) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logTap) WithGroup(string) slog.Handler { return l }

// find returns the records logged at level with msg.
func (l *logTap) find(level slog.Level, msg string) []slog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []slog.Record
	for _, r := range l.records {
		if r.Level == level && r.Message == msg {
			out = append(out, r)
		}
	}
	return out
}

// Never acked: every retry is a Full and the delay doubles from the heartbeat up to its 4-beat cap.
func TestAnnounceBackoffDoublesUntilCap(t *testing.T) {
	g := clustertest.NewFakeGateway(t, nil)
	arrivals := stampAnnounces(g, refuseAnnounce)
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = beat })
	at := arrivals(t, 5)
	for i, a := range g.Announces()[:5] {
		if !a.Req.Full {
			t.Errorf("announce %d = %+v, want a Full", i, a.Req)
		}
	}
	for i, want := range []time.Duration{beat, 2 * beat, 4 * beat, 4 * beat} {
		if d := at[i+1].Sub(at[i]); d < want {
			t.Errorf("delay after failure %d = %v, want at least %v", i+1, d, want)
		}
	}
	// The first failure keeps the heartbeat: doubling from it would take 2+4+4 beats.
	if d := at[3].Sub(at[0]); d >= 8*beat {
		t.Errorf("delays after failures 1 to 3 = %v, want 1+2+4 beats, below %v", d, 8*beat)
	}
	if d := at[4].Sub(at[3]); d >= 8*beat {
		t.Errorf("delay after failure 4 = %v, want it capped below %v", d, 8*beat)
	}
}

// After an ack with a 1s TTL the cap is half of it: the fifth failure in a row waits 500ms instead of 16 beats.
func TestAnnounceBackoffCapFollowsTTL(t *testing.T) {
	g := clustertest.NewFakeGateway(t, nil)
	arrivals := stampAnnounces(g, func(i int) (int, apis.AnnounceResponse) {
		if i == 0 {
			return http.StatusOK, apis.AnnounceResponse{TTL: 1}
		}
		return refuseAnnounce(i)
	})
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = beat })
	// Announces 1 to 5 fail and wait 1, 2, 4 and 8 beats, then the cap.
	at := arrivals(t, 7)
	if d := at[5].Sub(at[4]); d < 8*beat {
		t.Errorf("delay after failure 4 = %v, want at least %v", d, 8*beat)
	}
	if d := at[6].Sub(at[5]); d < 500*time.Millisecond || d >= 16*beat {
		t.Errorf("delay after failure 5 = %v, want the 500ms cap, below %v", d, 16*beat)
	}
}

// An ack without a TTL keeps the cap of the last TTL: after a 1s TTL and then none, the fourth failure in a row still waits 8 beats.
func TestAnnounceBackoffCapKeepsLastTTL(t *testing.T) {
	g := clustertest.NewFakeGateway(t, nil)
	arrivals := stampAnnounces(g, func(i int) (int, apis.AnnounceResponse) {
		switch i {
		case 0:
			return http.StatusOK, apis.AnnounceResponse{TTL: 1}
		case 1:
			return http.StatusOK, apis.AnnounceResponse{}
		}
		return refuseAnnounce(i)
	})
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = beat })
	// Announces 2 to 5 fail and wait 1, 2, 4 and 8 beats; the 4-beat cap would cut the last.
	at := arrivals(t, 7)
	if d := at[6].Sub(at[5]); d < 8*beat {
		t.Errorf("delay after failure 4 = %v, want at least %v under the 500ms cap of the last TTL", d, 8*beat)
	}
}

// The first ack after failures resets the delay to the heartbeat; the failures warn once and the ack logs the recovery once.
func TestAnnounceRecoveryResetsBackoffAndLogs(t *testing.T) {
	logs := tapLogs(t)
	g := clustertest.NewFakeGateway(t, nil)
	arrivals := stampAnnounces(g, func(i int) (int, apis.AnnounceResponse) {
		if i < 3 {
			return refuseAnnounce(i)
		}
		return http.StatusOK, apis.AnnounceResponse{TTL: 30}
	})
	newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = beat })
	// Three failures leave a 4-beat delay; announce 3 is the first ack and announce 5 shows announce 4 was handled.
	at := arrivals(t, 6)
	if a := g.Announces()[3]; !a.Req.Full {
		t.Errorf("first acked announce = %+v, want a Full", a.Req)
	}
	if d := at[4].Sub(at[3]); d >= 3*beat {
		t.Errorf("delay after the ack = %v, want the %v heartbeat again", d, beat)
	}
	if n := len(logs.find(slog.LevelWarn, "peer: announce")); n != 1 {
		t.Errorf("announce warnings = %d, want 1 for three failures in a row", n)
	}
	recovered := logs.find(slog.LevelInfo, "peer: announce recovered")
	if len(recovered) != 1 {
		t.Fatalf("recovery infos = %d, want 1", len(recovered))
	}
	if !recovered[0].Time.After(at[3]) {
		t.Errorf("recovery logged at %v, before the ack of announce 3", recovered[0].Time)
	}
}

// A commit wakes the announcer out of the backoff at once; the woken announce fails like any other, so the backoff does not restart.
func TestAnnounceWakeBypassesBackoff(t *testing.T) {
	hash, body := clustertest.MakeXorb(t, 100, 2, 8<<10)
	g := clustertest.NewFakeGateway(t, nil)
	arrivals := stampAnnounces(g, refuseAnnounce)
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = beat })
	g.Replied(t, 2) // three failures: the next heartbeat is 4 beats away
	kicked := time.Now()
	commitDirect(t, p, hash, body)
	at := arrivals(t, 5)
	if d := at[3].Sub(kicked); d >= 2*beat {
		t.Errorf("announce after the commit came %v later, want it at once", d)
	}
	if woken := g.Announces()[3]; !woken.Req.Full || !hasAdd(woken, hash) {
		t.Errorf("announce after the commit = %+v, want a Full adding %s", woken.Req, hash)
	}
	if d := at[4].Sub(at[3]); d < 4*beat {
		t.Errorf("delay after the woken failure = %v, want at least %v", d, 4*beat)
	}
}

func TestAnnouncedTracksLastAttempt(t *testing.T) {
	refuse := func(http.ResponseWriter, *http.Request, apis.AnnounceRequest) (int, apis.AnnounceResponse) {
		return http.StatusInternalServerError, apis.AnnounceResponse{}
	}
	g := clustertest.NewFakeGateway(t, nil)
	g.SetReply(refuse)
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.Heartbeat = time.Hour })
	kick := func() {
		p.mu.Lock()
		p.kick()
		p.mu.Unlock()
	}
	g.Replied(t, 0)
	if p.Announced() {
		t.Fatal("Announced after a refused first announce")
	}
	g.SetReply(nil)
	kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "Announced after an ack", p.Announced)
	g.SetReply(refuse)
	kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "Announced cleared by a refused announce", func() bool { return !p.Announced() })
	g.SetReply(nil)
	kick()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "Announced after the next ack", p.Announced)
	// Close ends the wait, not an announce, so the last outcome stands.
	_ = p.Close()
	if !p.Announced() {
		t.Fatal("Close while waiting cleared Announced")
	}
}
