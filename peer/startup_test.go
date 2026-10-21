package peer

import (
	"bytes"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// validationTimeout bounds a wait for the whole backlog to validate, which reads 64 MiB through the race detector.
const validationTimeout = time.Minute

// decoys is the number of invalid copies seeded ahead of the valid object; together they make 64 MiB to validate.
const decoys = 7

// heavy is a valid ~8 MiB xorb packed once for the package.
var heavy struct {
	once sync.Once
	hash xet.XorbHash
	body []byte
}

// seedBacklog writes the heavy xorb under decoys foreign hash names, oldest first, then under its own; Validate reads each decoy to its footer before rejecting it. Every mtime is a minute or more before now.
func seedBacklog(t *testing.T, dir string) (valid xet.XorbHash, invalid []xet.XorbHash) {
	t.Helper()
	heavy.once.Do(func() { heavy.hash, heavy.body = clustertest.MakeXorb(t, 90, 128, 64<<10) })
	if heavy.body == nil {
		t.Fatal("heavy xorb fixture unavailable")
	}
	now := time.Now()
	for i := range decoys + 1 {
		hash := heavy.hash
		if i < decoys {
			hash, _ = xet.ParseXorbHash(clustertest.SHA256([]byte{byte(i)}))
			invalid = append(invalid, hash)
		}
		writeXorbFile(t, dir, hash, heavy.body, now.Add(time.Duration(i-decoys-1)*time.Minute))
	}
	return heavy.hash, invalid
}

// writeXorbFile places body under hash in dir without validating it and sets its mtime.
func writeXorbFile(t *testing.T, dir string, hash xet.XorbHash, body []byte, mtime time.Time) {
	t.Helper()
	path := xorbPath(dir, hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// gatewayView replays the announces in arrival order the way the gateway applies them and returns what it holds for the peer.
func gatewayView(g *clustertest.FakeGateway) []string {
	held := map[string]struct{}{}
	for _, a := range g.Announces() {
		if a.Req.Full {
			clear(held)
		}
		for _, h := range a.Req.Add {
			held[h] = struct{}{}
		}
		for _, h := range a.Req.Remove {
			delete(held, h)
		}
	}
	return slices.Sorted(maps.Keys(held))
}

// waitView waits until the replayed gateway view holds exactly want.
func waitView(t *testing.T, g *clustertest.FakeGateway, want ...xet.XorbHash) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("gateway view %v", hashes(want...)), func() bool {
		return slices.Equal(gatewayView(g), hashes(want...))
	})
}

func TestStartupValidatesInBackground(t *testing.T) {
	dir := t.TempDir()
	valid, invalid := seedBacklog(t, dir)
	g := clustertest.NewFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("seeded object reached the gateway")
		w.WriteHeader(http.StatusInternalServerError)
	})
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	p.mu.Lock()
	n := p.xorbs.Len()
	p.mu.Unlock()
	if done := p.Validated(); n != 0 || done {
		t.Fatalf("New returned with %d xorbs admitted and validation done %v, want neither before validation", n, done)
	}
	clustertest.WaitFor(t, validationTimeout, "startup validation", p.Validated)
	p.mu.Lock()
	var kept []string
	for e := range p.xorbs.Oldest() {
		kept = append(kept, e.Key.String())
	}
	p.mu.Unlock()
	slices.Sort(kept)
	if !slices.Equal(kept, hashes(valid)) || !clustertest.HasXorb(t, p.store, valid) {
		t.Fatalf("inventory after validation = %v, valid stored %v, want only %s", kept, clustertest.HasXorb(t, p.store, valid), valid)
	}
	for _, h := range invalid {
		if _, err := os.Stat(xorbPath(dir, h)); !os.IsNotExist(err) {
			t.Fatalf("invalid %s kept after validation: %v", h, err)
		}
	}
	waitView(t, g, valid)
	for i, a := range g.Announces() {
		for _, h := range invalid {
			if hasAdd(a, h) {
				t.Fatalf("announce %d = %+v adds invalid %s", i, a.Req, h)
			}
		}
	}
}

func TestUnvalidatedXorbServedWhileValidating(t *testing.T) {
	dir := t.TempDir()
	seedBacklog(t, dir)
	probe, body := clustertest.MakeXorb(t, 91, 2, 8<<10)
	writeXorbFile(t, dir, probe, body, time.Now())
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{probe: body}))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	// The probe is newest, so validation reaches it last.
	pullAndSettle(t, p, srv, probe, body)
	if !admitted(p, probe)() || !clustertest.HasXorb(t, p.store, probe) {
		t.Fatalf("probe after the GET: admitted %v, stored %v", admitted(p, probe)(), clustertest.HasXorb(t, p.store, probe))
	}
}

func TestCorruptUnvalidatedXorbReplacedByPull(t *testing.T) {
	dir := t.TempDir()
	seedBacklog(t, dir)
	probe, body := clustertest.MakeXorb(t, 92, 2, 8<<10)
	corrupt := bytes.Clone(body)
	corrupt[len(corrupt)/2] ^= 0x01
	writeXorbFile(t, dir, probe, corrupt, time.Now())
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(map[xet.XorbHash][]byte{probe: body}))
	p, srv := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	if _, err := os.Stat(xorbPath(dir, probe)); err != nil {
		t.Fatalf("corrupt probe gone before validation reached it: %v", err)
	}
	pullAndSettle(t, p, srv, probe, body)
	replaced := func(when string) {
		t.Helper()
		disk, err := os.ReadFile(xorbPath(dir, probe))
		if err != nil || !bytes.Equal(disk, body) || !admitted(p, probe)() {
			t.Fatalf("probe %s: stored copy is the pulled body %v (%v), admitted %v", when, bytes.Equal(disk, body), err, admitted(p, probe)())
		}
	}
	replaced("after the pull")
	if n := len(g.Requests()); n != 1 {
		t.Fatalf("gateway GETs = %d, want 1", n)
	}
	// Whether the pull or validation reached the corrupt copy first, validation ends with the pulled copy in place.
	clustertest.WaitFor(t, validationTimeout, "startup validation", p.Validated)
	replaced("after validation")
}

func TestCloseInterruptsValidation(t *testing.T) {
	dir := t.TempDir()
	_, invalid := seedBacklog(t, dir)
	g := clustertest.NewFakeGateway(t, clustertest.ServeByHash(nil))
	p, _ := newTestPeer(t, g.URL, func(c *Config) { c.CacheDir, c.Heartbeat = dir, time.Hour })
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(clustertest.WaitTimeout):
		t.Fatal("Close did not interrupt startup validation")
	}
	// Once canceled the validator changes nothing, so only its goroutine shows whether Close waited for it.
	validator := runtime.FuncForPC(reflect.ValueOf((*Peer).validateUnchecked).Pointer()).Name()
	stacks := make([]byte, 1<<20)
	if stacks = stacks[:runtime.Stack(stacks, true)]; bytes.Contains(stacks, []byte(validator+"(")) {
		t.Errorf("%s still running after Close returned", validator)
	}
	onDisk := func() (left []xet.XorbHash) {
		for _, h := range invalid {
			if _, err := os.Stat(xorbPath(dir, h)); err == nil {
				left = append(left, h)
			}
		}
		return left
	}
	left := onDisk()
	if len(left) == 0 {
		t.Errorf("all %d invalid objects were validated and dropped, want Close to stop validation before the last", len(invalid))
	}
	if again := onDisk(); !slices.Equal(again, left) {
		t.Errorf("invalid objects on disk went from %d to %d after Close returned", len(left), len(again))
	}
	if p.Validated() {
		t.Error("validation reported done after Close interrupted it")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}
