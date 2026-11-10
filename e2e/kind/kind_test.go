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

// The fixture of e2e/hf_cli_test.go, a pinned public xet-backed file on huggingface.co; test files of another package cannot be imported.
const (
	hfUpstream = "https://huggingface.co"
	hfRepo     = "hf-internal-testing/tiny-random-LlamaForCausalLM"
	hfFile     = "model.safetensors"
	hfRevision = "9fb191250dd56d0ba7ec9785a025ed29c03d5998"
	hfFileSHA  = "49c20f32c6c597480fcaec5df2f86c645eabea765cbea1e67886dbae45e5c992"
	hfFileSize = 4131280
)

var (
	trackerTypes = []string{"xetpd_tracker_peers", "xetpd_tracker_xorbs", "xetpd_tracker_shards", "xetpd_tracker_announces_total", "xetpd_tracker_lookups_total", "xetpd_tracker_seed_requests_total"}
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
		first, second string // the workers the Jobs run on: cold on first, warm and restart on second
		tracker       string
		peers         map[string]pod // by node
		cold          counts
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
	// listed waits until the tracker lists the peer of pod p with a complete file, under an epoch other than skip.
	listed := func(t *testing.T, timeout time.Duration, p pod, skip string) {
		t.Helper()
		c.eventually(t, timeout, "the tracker to list "+p.name+" with a complete file", func() error {
			d, err := c.peers(t.Context(), tracker)
			if err != nil {
				return err
			}
			if s, ok := peerStatus(d, p.url()); !ok || s.Shards < 1 || s.Epoch == skip {
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
		c.kubectl(t, "apply", "-k", c.overlay)
		for _, workload := range []string{"deploy/xet-pd-tracker", "ds/xet-pd-peer"} {
			c.must(t, rolloutTimeout+callTimeout, "kubectl", "-n", namespace, "rollout", "status", workload, "--timeout="+rolloutTimeout.String())
		}
	})
	step("roles", time.Second, func(t *testing.T) {
		trackers := c.pods(t, trackerSelector)
		if len(trackers) != 1 || !slices.Contains(workers, trackers[0].node) {
			t.Fatalf("tracker pods %v, want one on a worker of %v", trackers, workers)
		}
		tracker = trackers[0].name
		peers = map[string]pod{}
		for _, p := range c.pods(t, peerSelector) {
			peers[p.node] = p
		}
		if len(peers) != 2 || peers[first].name == "" || peers[second].name == "" {
			t.Fatalf("peer pods by node %v, want one on each of %v", peers, workers)
		}
		want := []string{peers[first].url(), peers[second].url()}
		slices.Sort(want)
		c.eventually(t, peersTimeout, "both peers registered", func() error {
			d, err := c.peers(t.Context(), tracker)
			if err != nil {
				return err
			}
			var urls []string
			for _, p := range d.Peers {
				urls = append(urls, p.URL)
			}
			if slices.Sort(urls); !slices.Equal(urls, want) {
				return fmt.Errorf("tracker lists peers %v, want %v", urls, want)
			}
			return nil
		})
		summary = append(summary, fmt.Sprintf("tracker %s on %s; peer pods by node %v", tracker, trackers[0].node, peers))
	})
	step("smoke", time.Second, func(t *testing.T) {
		c.raw(t, tracker, trackerPort, "/readyz")
		for _, node := range workers {
			c.raw(t, peers[node].name, peerPort, "/readyz")
		}
		text := c.raw(t, tracker, trackerPort, "/metrics")
		declared := checkTypes(t, "tracker", text, trackerTypes)
		if live := sample(t, clustertest.ParseMetrics(t, text), "xetpd_tracker_peers"); live != 2 {
			t.Errorf("tracker peers gauge %v, want 2", live)
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
		summary = append(summary, fmt.Sprintf("smoke: /readyz 200 on the tracker and both peers; TYPE %v; peer announces ok by node %v", declared, announced))
	})
	// A cold resolve carries no xet headers while the peer ingests, so hf downloads plain bytes from its node's peer: no pull there shows the Job reached it.
	step("cold", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-cold", first)
		listed(t, filesTimeout, peers[first], "")
		cold = c.count(t, tracker, peers)
		if cold.pulled[first] != 0 {
			t.Errorf("peer on %s pulled %v times, want the file from its own ingest", first, cold.pulled[first])
		}
		if cold.client[second] != 0 {
			t.Errorf("peer on %s answered %v client xorb GETs, want none", second, cold.client[second])
		}
		if t.Failed() {
			t.Logf("hf-cold log:\n%s", log)
		}
		summary = append(summary, "cold: hf-cold on "+first+": "+cold.String())
	})
	step("seed", time.Second, func(t *testing.T) {
		listed(t, seedTimeout, peers[second], "")
		s := c.count(t, tracker, peers)
		if s.seeds < 1 || s.pulled[second] < 1 || s.relayed[first] < 1 {
			t.Errorf("seed requests ok %v, pulls ok on %s %v, peer xorb GETs answered on %s %v; want the tracker's seed of %s pulled from %s",
				s.seeds, second, s.pulled[second], first, s.relayed[first], second, first)
		}
		summary = append(summary, "seed: "+s.String())
	})
	// kind counts no upstream bytes, so the peer counters show the copy on the second worker came from the first.
	step("warm", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-warm", second)
		s := c.count(t, tracker, peers)
		if s.client[second] <= 0 || s.client[first] != cold.client[first] {
			t.Errorf("client xorb GETs on %s %v, on %s %v -> %v; want the Job's on its own node only", second, s.client[second], first, cold.client[first], s.client[first])
		}
		if s.lookups < 1 || s.shards[first] < 1 || s.relayed[first] < 1 || s.pulled[second] < 1 {
			t.Errorf("tracker lookups %v, shard gets on %s %v, peer xorb GETs answered on %s %v, pulls ok on %s %v; want the copy on %s pulled from %s",
				s.lookups, first, s.shards[first], first, s.relayed[first], second, s.pulled[second], second, first)
		}
		if t.Failed() {
			t.Logf("hf-warm log:\n%s", log)
		}
		summary = append(summary, "warm: hf-warm on "+second+": "+s.String())
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
		listed(t, filesTimeout, peers[second], before.Epoch)
		log := c.job(t, "hf-restart", second)
		s := c.count(t, tracker, peers)
		if s.client[second] <= 0 {
			t.Errorf("restarted peer on %s answered %v client xorb GETs, want the Job's", second, s.client[second])
		}
		if t.Failed() {
			t.Logf("hf-restart log:\n%s", log)
		}
		summary = append(summary, fmt.Sprintf("restart: %s replaced %s and restored its files; hf-restart on %s: %s", peers[second].name, old.name, second, s))
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
