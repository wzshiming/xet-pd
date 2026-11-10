package kind

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const hfUpstream = "https://huggingface.co"

// fixture is a pinned public xet-backed file on huggingface.co.
type fixture struct {
	repo, file, revision, sha string
	size                      int
}

var (
	// model is the fixture of e2e/hf_cli_test.go; test files of another package cannot be imported.
	model = fixture{"hf-internal-testing/tiny-random-LlamaForCausalLM", "model.safetensors", "9fb191250dd56d0ba7ec9785a025ed29c03d5998", "49c20f32c6c597480fcaec5df2f86c645eabea765cbea1e67886dbae45e5c992", 4131280}
	// inner is another file of that commit, first asked for through the internal peer.
	inner = fixture{"hf-internal-testing/tiny-random-LlamaForCausalLM", "tokenizer.model", "9fb191250dd56d0ba7ec9785a025ed29c03d5998", "9e556afd44213b6bd1be2b850ebbbd98f5481437a8021afaf58ee7fb1818d347", 499723}
)

var (
	trackerTypes = []string{"xetpd_tracker_peers", "xetpd_tracker_upstream_peers", "xetpd_tracker_xorbs", "xetpd_tracker_shards", "xetpd_tracker_announces_total", "xetpd_tracker_lookups_total", "xetpd_tracker_seed_requests_total"}
	peerTypes    = []string{"xetpd_peer_xorb_gets_total", "xetpd_peer_pulls_total", "xetpd_peer_announces_total", "xetpd_peer_shard_gets_total", "xetpd_peer_shards", "xetpd_peer_xorb_cache_bytes"}
)

func TestKind(t *testing.T) {
	if os.Getenv("XET_PD_E2E_KIND") != "1" {
		t.Skip("set XET_PD_E2E_KIND=1 to run against a kind cluster")
	}
	c := newCluster(t)
	for _, tool := range [][]string{{"docker", "info"}, {"kind", "version"}, {"kubectl", "version", "--client"}} {
		c.must(t, toolTimeout, tool[0], tool[1:]...)
	}
	preflight(t)
	keep := os.Getenv("XET_PD_E2E_KIND_KEEP") == "1"
	if !keep {
		t.Cleanup(func() {
			if _, err := c.runBy(context.Background(), c.end.Add(-deleteReserve), createTimeout, "", "kind", "delete", "cluster", "--name", clusterName); err != nil {
				t.Errorf("delete the cluster: %v", err)
				return
			}
			t.Logf("deleted cluster %s", clusterName)
		})
	}
	up := false
	t.Cleanup(func() {
		if up && t.Failed() {
			c.dump(t)
		}
	})

	var (
		workers       []string
		first, second string // the node labelled for the access peer and the internal peer's node
		tracker       string
		peers         map[string]pod // by node
		summary       []string
	)
	passed := true
	// need stays below the step's duration on a warm run.
	step := func(name string, need time.Duration, f func(t *testing.T)) {
		passed = t.Run(name, func(t *testing.T) {
			if !passed {
				t.Skip("an earlier step failed")
			}
			if left := time.Until(c.deadline); left < need {
				t.Fatalf("budget exhausted before %s: %v left of the run, %v needed", name, left.Round(time.Second), need)
			}
			f(t)
		}) && passed
	}
	// listed waits until the tracker lists the peer of pod p with at least xorbs xorbs and shards shards, under an epoch other than skip.
	listed := func(t *testing.T, timeout time.Duration, p pod, xorbs, shards int, skip string) {
		t.Helper()
		c.eventually(t, timeout, fmt.Sprintf("the tracker to list %s with %d xorbs and %d shards", p.name, xorbs, shards), func() error {
			d, err := c.peers(t.Context(), tracker)
			if err != nil {
				return err
			}
			if s, ok := peerStatus(d, p.url()); !ok || s.Holdings < xorbs || s.Shards < shards || s.Epoch == skip {
				return fmt.Errorf("tracker lists %v", holdings(d))
			}
			return nil
		})
	}

	step("build", 15*time.Second, func(t *testing.T) {
		c.must(t, buildTimeout, "docker", "build", "--target", "tracker", "-t", trackerImage, c.root)
		c.must(t, buildTimeout, "docker", "build", "--target", "peer", "-t", peerImage, c.root)
		c.must(t, buildTimeout, "docker", "build", "-t", hfImage, "-f", filepath.Join(c.testdata, "hfcli.Dockerfile"), c.testdata)
	})
	step("cluster", 15*time.Second, func(t *testing.T) {
		how := "created"
		if slices.Contains(strings.Fields(c.must(t, callTimeout, "kind", "get", "clusters")), clusterName) {
			how = "reused"
			c.must(t, callTimeout, "kind", "export", "kubeconfig", "--name", clusterName, "--kubeconfig", c.kubeconfig)
			up = true
			c.reset(t)
		} else {
			args := []string{"create", "cluster", "--name", clusterName, "--config", filepath.Join(c.testdata, "kind-config.yaml"), "--kubeconfig", c.kubeconfig}
			if image := os.Getenv("XET_PD_E2E_KIND_NODE_IMAGE"); image != "" {
				args = append(args, "--image", image)
			}
			c.must(t, createTimeout, "kind", args...)
			up = true
		}
		c.must(t, nodesTimeout+callTimeout, "kubectl", "wait", "--for=condition=Ready", "nodes", "--all", "--timeout="+nodesTimeout.String())
		if workers = c.workers(t); len(workers) != 2 {
			t.Fatalf("workers %v, want 2", workers)
		}
		first, second = workers[0], workers[1]
		summary = append(summary, fmt.Sprintf("cluster %s %s, workers %v", clusterName, how, workers))
	})
	step("load", 5*time.Second, func(t *testing.T) {
		c.must(t, loadTimeout, "kind", "load", "docker-image", trackerImage, peerImage, hfImage, "--name", clusterName)
	})
	step("deploy", 5*time.Second, func(t *testing.T) {
		c.kubectl(t, "label", "node", first, upstreamLabel+"=true", "--overwrite")
		c.kubectl(t, "apply", "-k", c.overlay)
		for _, workload := range []string{"deploy/xet-pd-tracker", "ds/xet-pd-peer-upstream", "ds/xet-pd-peer"} {
			c.must(t, rolloutTimeout+callTimeout, "kubectl", "-n", namespace, "rollout", "status", workload, "--timeout="+rolloutTimeout.String())
		}
	})
	step("roles", time.Second, func(t *testing.T) {
		trackers := c.pods(t, trackerSelector)
		if len(trackers) != 1 || !slices.Contains(workers, trackers[0].node) {
			t.Fatalf("tracker pods %v, want one on a worker of %v", trackers, workers)
		}
		tracker = trackers[0].name
		access, internal := c.pods(t, accessSelector), c.pods(t, peerSelector)
		if len(access) != 1 || access[0].node != first || len(internal) != 1 || internal[0].node != second {
			t.Fatalf("access peer pods %v and internal peer pods %v, want one on %s and one on %s", access, internal, first, second)
		}
		peers = map[string]pod{first: access[0], second: internal[0]}
		c.eventually(t, peersTimeout, "both peers registered, the one on "+first+" with an upstream", func() error {
			d, err := c.peers(t.Context(), tracker)
			if err != nil {
				return err
			}
			a, okA := peerStatus(d, peers[first].url())
			i, okI := peerStatus(d, peers[second].url())
			if len(d.Peers) != 2 || !okA || !okI || !a.Upstream || i.Upstream {
				return fmt.Errorf("tracker lists %v", holdings(d))
			}
			return nil
		})
		summary = append(summary, fmt.Sprintf("tracker %s on %s; access peer %s on %s, internal peer %s on %s", tracker, trackers[0].node, peers[first].name, first, peers[second].name, second))
	})
	step("smoke", time.Second, func(t *testing.T) {
		c.raw(t, tracker, trackerPort, "/readyz")
		for _, node := range workers {
			c.raw(t, peers[node].name, peerPort, "/readyz")
		}
		text := c.raw(t, tracker, trackerPort, "/metrics")
		declared := checkTypes(t, "tracker", text, trackerTypes)
		m := clustertest.ParseMetrics(t, text)
		live, upstreams := sample(t, m, "xetpd_tracker_peers"), sample(t, m, "xetpd_tracker_upstream_peers")
		if live != 2 || upstreams != 1 {
			t.Errorf("tracker gauges: %v peers, %v upstream peers; want 2 and 1", live, upstreams)
		}
		announced := map[string]float64{}
		for i, node := range workers {
			text := c.raw(t, peers[node].name, peerPort, "/metrics")
			if found := checkTypes(t, "peer on "+node, text, peerTypes); i == 0 {
				declared = append(declared, found...)
			}
			announced[node] = sample(t, clustertest.ParseMetrics(t, text), `xetpd_peer_announces_total{result="ok"}`)
			if announced[node] <= 0 {
				t.Errorf("peer on %s announced ok %v times, want some", node, announced[node])
			}
		}
		summary = append(summary, fmt.Sprintf("smoke: /readyz 200 on the tracker and both peers; TYPE %v; tracker gauges %v peers, %v upstream peers; peer announces ok by node %v", declared, live, upstreams, announced))
	})
	// A cold resolve carries no xet headers while the peer ingests, so hf streams plain bytes and the access peer may count no client xorb GET: its holdings show the Job reached it.
	step("cold", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-cold", first, model)
		listed(t, filesTimeout, peers[first], 1, 1, "")
		s := c.count(t, tracker, peers)
		if s.pulled[first] != 0 || s.client[second] != 0 {
			t.Errorf("pulls ok on %s %v, client xorb GETs on %s %v; want the access peer's own ingest and nothing asked of %s", first, s.pulled[first], second, s.client[second], second)
		}
		if t.Failed() {
			t.Logf("hf-cold log:\n%s", log)
		}
		summary = append(summary, "cold: hf-cold on "+first+": "+s.String())
	})
	step("seed", time.Second, func(t *testing.T) {
		listed(t, seedTimeout, peers[second], 1, 1, "")
		s := c.count(t, tracker, peers)
		if s.seeds < 1 || s.pulled[second] < 1 || s.relayed[first] < 1 {
			t.Errorf("seed requests ok %v, pulls ok on %s %v, peer xorb GETs answered on %s %v; want the tracker's seed of %s pulled from %s",
				s.seeds, second, s.pulled[second], first, s.relayed[first], second, first)
		}
		summary = append(summary, "seed: "+s.String())
	})
	step("warm", 3*time.Second, func(t *testing.T) {
		before := c.count(t, tracker, peers)
		log := c.job(t, "hf-warm", second, model)
		s := c.count(t, tracker, peers)
		if s.client[second] < 1 || s.client[first] != before.client[first] {
			t.Errorf("client xorb GETs on %s %v, on %s %v -> %v; want the Job's on its own node only", second, s.client[second], first, before.client[first], s.client[first])
		}
		if t.Failed() {
			t.Logf("hf-warm log:\n%s", log)
		}
		summary = append(summary, "warm: hf-warm on "+second+": "+s.String())
	})
	// The internal peer has no upstream, so the Job succeeds only through the access peer, which ingests the file while the internal peer chunks it again.
	step("cold-inner", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-cold-inner", second, inner)
		for _, node := range workers {
			listed(t, filesTimeout, peers[node], 2, 2, "")
		}
		s := c.count(t, tracker, peers)
		if t.Failed() {
			t.Logf("hf-cold-inner log:\n%s", log)
		}
		summary = append(summary, fmt.Sprintf("cold-inner: hf-cold-inner of %s on %s: %s", inner.file, second, s))
	})
	step("restart", 15*time.Second, func(t *testing.T) {
		old := peers[second]
		d, err := c.peers(t.Context(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		before, _ := peerStatus(d, old.url())
		c.kubectl(t, "-n", namespace, "delete", "pod", old.name, "--wait=true")
		c.eventually(t, restartTimeout, "a new peer pod running on "+second, func() error {
			for _, p := range c.pods(t, peerSelector) {
				if p.node == second && p.name != old.name {
					peers[second] = p
					return nil
				}
			}
			return fmt.Errorf("no peer pod replaced %s", old.name)
		})
		c.must(t, rolloutTimeout+callTimeout, "kubectl", "-n", namespace, "wait", "--for=condition=Ready", "pod/"+peers[second].name, "--timeout="+rolloutTimeout.String())
		listed(t, filesTimeout, peers[second], 2, 2, before.Epoch)
		log := c.job(t, "hf-restart", second, model)
		s := c.count(t, tracker, peers)
		if s.client[second] < 1 {
			t.Errorf("restarted peer on %s answered %v client xorb GETs, want the Job's", second, s.client[second])
		}
		if t.Failed() {
			t.Logf("hf-restart log:\n%s", log)
		}
		summary = append(summary, fmt.Sprintf("restart: %s replaced %s and restored its xorbs and shards; hf-restart on %s: %s", peers[second].name, old.name, second, s))
	})
	if keep {
		summary = append(summary, "cluster kept; kind export kubeconfig --name "+clusterName+" reaches it")
	}
	t.Logf("summary:\n%s", strings.Join(summary, "\n"))
}

func TestCleanupReserves(t *testing.T) {
	if cleanupReserve <= dumpReserve || dumpReserve <= deleteReserve || deleteReserve <= 0 {
		t.Errorf("want cleanupReserve %v > dumpReserve %v > deleteReserve %v > 0", cleanupReserve, dumpReserve, deleteReserve)
	}
}
