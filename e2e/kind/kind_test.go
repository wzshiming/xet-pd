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
	gatewayTypes = []string{"xetpd_gateway_xorb_gets_total", "xetpd_gateway_locality_picks_total", "xetpd_gateway_locality_synced", "xetpd_gateway_peers", "xetpd_gateway_announces_total", "xetpd_gateway_pull_requests_total"}
	peerTypes    = []string{"xetpd_peer_xorb_gets_total", "xetpd_peer_pulls_total", "xetpd_peer_announces_total", "xetpd_peer_xorb_cache_bytes"}
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
		workers            []string
		gateway, near, far string
		peers              map[string]string // pod by node
		a, b               counts
		summary            []string
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

	step("build", 15*time.Second, func(t *testing.T) {
		c.must(t, buildTimeout, "docker", "build", "--target", "gateway", "-t", gatewayImage, c.root)
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
		summary = append(summary, fmt.Sprintf("cluster %s %s, workers %v", clusterName, how, workers))
	})
	step("load", 5*time.Second, func(t *testing.T) {
		c.must(t, loadTimeout, "kind", "load", "docker-image", gatewayImage, peerImage, hfImage, "--name", clusterName)
	})
	step("deploy", 5*time.Second, func(t *testing.T) {
		c.kubectl(t, "apply", "-k", c.overlay)
		for _, workload := range []string{"deploy/xet-pd-gateway", "ds/xet-pd-peer"} {
			c.must(t, rolloutTimeout+callTimeout, "kubectl", "-n", namespace, "rollout", "status", workload, "--timeout="+rolloutTimeout.String())
		}
	})
	step("roles", time.Second, func(t *testing.T) {
		gws := c.pods(t, gatewaySelector)
		if len(gws) != 1 || !slices.Contains(workers, gws[0].node) {
			t.Fatalf("gateway pods %v, want one on a worker of %v", gws, workers)
		}
		gateway, near = gws[0].name, gws[0].node
		// The far node is the one (c) taints: evicting the gateway would strand its volume, which is bound to its node.
		far = workers[0]
		if far == near {
			far = workers[1]
		}
		peers = map[string]string{}
		for _, p := range c.pods(t, peerSelector) {
			peers[p.node] = p.name
		}
		if len(peers) != 2 || peers[near] == "" || peers[far] == "" {
			t.Fatalf("peer pods by node %v, want one on each of %v", peers, workers)
		}
		c.eventually(t, peersTimeout, "both peers registered with their nodes", func() error {
			d, err := c.peers(t.Context(), gateway)
			if err == nil && !slices.Equal(nodesOf(d), workers) {
				err = fmt.Errorf("gateway lists peers on %v", nodesOf(d))
			}
			return err
		})
		summary = append(summary, fmt.Sprintf("gateway %s on near %s, far %s, peer pods by node %v", gateway, near, far, peers))
	})
	step("smoke", time.Second, func(t *testing.T) {
		c.raw(t, gateway, gatewayPort, "/readyz")
		for _, node := range workers {
			c.raw(t, peers[node], peerPort, "/readyz")
		}
		text := c.raw(t, gateway, gatewayPort, "/metrics")
		declared := checkTypes(t, "gateway", text, gatewayTypes)
		m := clustertest.ParseMetrics(t, text)
		synced, live := sample(t, m, "xetpd_gateway_locality_synced"), sample(t, m, "xetpd_gateway_peers")
		if synced != 1 || live != 2 {
			t.Errorf("gateway locality_synced %v, peers %v; want 1 and 2", synced, live)
		}
		announced := map[string]float64{}
		for i, node := range workers {
			text := c.raw(t, peers[node], peerPort, "/metrics")
			if found := checkTypes(t, "peer on "+node, text, peerTypes); i == 0 {
				declared = append(declared, found...)
			}
			announced[node] = sample(t, clustertest.ParseMetrics(t, text), `xetpd_peer_announces_total{result="ok"}`)
			if announced[node] <= 0 {
				t.Errorf("peer on %s announced ok %v times, want some", node, announced[node])
			}
		}
		summary = append(summary, fmt.Sprintf("(d) /readyz 200 on the gateway and both peers; TYPE %v; locality_synced %v, peers %v; peer announces ok by node %v", declared, synced, live, announced))
	})
	// Ingest first: a cold resolve carries no xet headers, so hf would bypass the peers.
	step("ingest", 3*time.Second, func(t *testing.T) {
		start := time.Now()
		body := c.must(t, ingestTimeout, "kubectl", "get", "--raw", proxyPath(gateway, gatewayPort, "/"+hfRepo+"/resolve/"+hfRevision+"/"+hfFile))
		if sum := clustertest.SHA256([]byte(body)); sum != hfFileSHA || len(body) != hfFileSize {
			t.Fatalf("fixture through the gateway: sha256 %s, %d bytes; want %s, %d", sum, len(body), hfFileSHA, hfFileSize)
		}
		// The bridge serves a digest once the ingest stored the file, right before resolve starts advertising xet.
		c.eventually(t, storedTimeout, "the fixture in gateway storage", func() error {
			_, err := c.run(t.Context(), callTimeout, "", "kubectl", "get", "--raw", proxyPath(gateway, gatewayPort, "/xet-bridge/"+hfFileSHA))
			return err
		})
		summary = append(summary, fmt.Sprintf("fixture fetched through the gateway and stored after %v", time.Since(start).Round(time.Second)))
	})
	step("near", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-a", near)
		a = c.count(t, gateway, peers)
		if got := nodesOf(a.peers); !slices.Equal(got, workers) {
			t.Errorf("gateway lists peers on %v, want %v", got, workers)
		}
		if g := a.peers.XorbGets; g.Direct != 0 || g.Peer == 0 {
			t.Errorf("gateway xorb gets %+v, want peer > 0 and direct 0", g)
		}
		if a.client[near] <= 0 || a.client[far] != 0 {
			t.Errorf("client 2xx near %v, far %v; want near > 0 and far 0", a.client[near], a.client[far])
		}
		if a.local <= 0 || a.fallback != 0 || a.none != 0 {
			t.Errorf("picks local %v, fallback %v, none %v; want local only", a.local, a.fallback, a.none)
		}
		if a.direct != 0 {
			t.Errorf("direct xorb gets metric %v, want 0", a.direct)
		}
		if t.Failed() {
			t.Logf("hf-a log:\n%s", log)
		}
		summary = append(summary, "(a) hf-a on near: "+a.String())
	})
	step("far", 3*time.Second, func(t *testing.T) {
		log := c.job(t, "hf-b", far)
		b = c.count(t, gateway, peers)
		if b.client[far] <= 0 || b.client[near] != a.client[near] {
			t.Errorf("client 2xx near %v, far %v; want near still %v and far > 0", b.client[near], b.client[far], a.client[near])
		}
		if b.local <= a.local || b.fallback != 0 {
			t.Errorf("picks local %v, fallback %v; want local above %v and no fallback", b.local, b.fallback, a.local)
		}
		if b.peers.XorbGets.Direct != 0 || b.direct != 0 {
			t.Errorf("direct xorb gets %d (registry), %v (metric); want 0", b.peers.XorbGets.Direct, b.direct)
		}
		if t.Failed() {
			t.Logf("hf-b log:\n%s", log)
		}
		summary = append(summary, "(b) hf-b on far: "+b.String())
	})
	step("fallback", 15*time.Second, func(t *testing.T) {
		c.kubectl(t, "taint", "nodes", far, taintKey+"=down:NoExecute")
		if keep {
			t.Cleanup(func() { c.untaint(t, far) })
		}
		c.eventually(t, expireTimeout, "the peer on "+far+" evicted and expired", func() error {
			out, err := c.run(t.Context(), callTimeout, "", "kubectl", "-n", namespace, "get", "pods", "-l", peerSelector, "--field-selector", "spec.nodeName="+far, "-o", "name")
			if err != nil {
				return err
			}
			if out = strings.TrimSpace(out); out != "" {
				return fmt.Errorf("%s still on %s", out, far)
			}
			d, err := c.peers(t.Context(), gateway)
			if err == nil && !slices.Equal(nodesOf(d), []string{near}) {
				err = fmt.Errorf("gateway lists peers on %v", nodesOf(d))
			}
			return err
		})
		log := c.job(t, "hf-c", far)
		s := c.count(t, gateway, map[string]string{near: peers[near]})
		if s.client[near] <= b.client[near] {
			t.Errorf("client 2xx near %v, want above %v", s.client[near], b.client[near])
		}
		if s.fallback <= 0 || s.none != 0 {
			t.Errorf("picks fallback %v, none %v; want fallback > 0 and none 0", s.fallback, s.none)
		}
		if s.peers.XorbGets.Direct != 0 || s.direct != 0 {
			t.Errorf("direct xorb gets %d (registry), %v (metric); want 0", s.peers.XorbGets.Direct, s.direct)
		}
		if s.live != 1 {
			t.Errorf("gateway peers gauge %v, want 1", s.live)
		}
		if t.Failed() {
			t.Logf("hf-c log:\n%s", log)
		}
		summary = append(summary, "(c) hf-c on far without its peer: "+s.String())
	})
	if keep {
		summary = append(summary, "cluster kept; kind export kubeconfig --name "+clusterName+" reaches it")
	}
	t.Logf("summary:\n%s", strings.Join(summary, "\n"))
}

func TestCleanupReserves(t *testing.T) {
	if cleanupReserve <= untaintReserve || untaintReserve <= dumpReserve || dumpReserve <= deleteReserve || deleteReserve <= 0 {
		t.Errorf("want cleanupReserve %v > untaintReserve %v > dumpReserve %v > deleteReserve %v > 0", cleanupReserve, untaintReserve, dumpReserve, deleteReserve)
	}
}
