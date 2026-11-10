// Package kind smoke-tests the deploy/k8s manifests in a kind cluster; it runs only with XET_PD_E2E_KIND=1.
package kind

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"text/template"
	"time"

	"github.com/wzshiming/xet-pd/apis"
	"github.com/wzshiming/xet-pd/internal/clustertest"
)

const (
	clusterName = "xet-pd-e2e"
	namespace   = "xet-pd"
	trackerPort = 8080
	peerPort    = 8081
	taintKey    = "xetpd-e2e"

	trackerSelector = "app.kubernetes.io/name=xet-pd-tracker"
	accessSelector  = "app.kubernetes.io/name=xet-pd-peer-upstream"
	peerSelector    = "app.kubernetes.io/name=xet-pd-peer"
	jobSelector     = "app.kubernetes.io/part-of=xet-pd-e2e"

	// upstreamLabel set to true puts an access peer on a node and keeps the internal peer off it.
	upstreamLabel = "xet-pd/upstream"

	trackerImage = "xet-pd-tracker:kind"
	peerImage    = "xet-pd-peer:kind"
	hfImage      = "xet-pd-hfcli:kind"
)

const (
	testTimeout    = 25 * time.Minute // ci.yml's -timeout, assumed when the test has no deadline
	toolTimeout    = 10 * time.Second // docker info, kind version, kubectl version
	callTimeout    = time.Minute      // one kubectl or docker call
	buildTimeout   = 8 * time.Minute  // per image, mostly a cold go mod download on a slow network
	createTimeout  = 5 * time.Minute  // kind create or delete cluster
	resetTimeout   = 3 * time.Minute  // deleting the deployment of a kept cluster
	nodesTimeout   = 3 * time.Minute
	loadTimeout    = 5 * time.Minute
	rolloutTimeout = 5 * time.Minute
	peersTimeout   = 60 * time.Second // both peers registered with the tracker
	filesTimeout   = 60 * time.Second // a file's xorbs and shard listed by the tracker once a holder announced them
	seedTimeout    = 60 * time.Second // the tracker's seed of the other peer
	restartTimeout = 3 * time.Minute  // a deleted peer pod replaced and running
	jobTimeout     = 4 * time.Minute
	waitDelay      = 10 * time.Second // pipes the children of a killed command hold open
)

// How long before go test's timeout panic the run and then each cleanup phase must end.
const (
	cleanupReserve = 3 * time.Minute
	dumpReserve    = time.Minute
	deleteReserve  = 10 * time.Second
)

// cluster runs kind, kubectl and docker against the test cluster through a kubeconfig of its own, never ~/.kube/config.
type cluster struct {
	root, testdata, overlay string
	kubeconfig              string
	end, deadline           time.Time // go test's timeout panic and the run's deadline
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	end, ok := t.Deadline()
	if !ok {
		end = time.Now().Add(testTimeout)
	}
	return &cluster{
		root:       root,
		testdata:   filepath.Join(root, "e2e", "kind", "testdata"),
		overlay:    filepath.Join(root, "deploy", "k8s", "overlays", "kind"),
		kubeconfig: filepath.Join(t.TempDir(), "kubeconfig"),
		end:        end,
		deadline:   end.Add(-cleanupReserve),
	}
}

// run executes name with args and stdin under timeout, cut short at the run deadline, and returns its stdout; the error carries the command line and both streams.
func (c *cluster) run(ctx context.Context, timeout time.Duration, stdin, name string, args ...string) (string, error) {
	return c.runBy(ctx, c.deadline, timeout, stdin, name, args...)
}

// runBy is run cut short at deadline instead, for the cleanups that spend the reserve after the run deadline.
func (c *cluster) runBy(ctx context.Context, deadline time.Time, timeout time.Duration, stdin, name string, args ...string) (string, error) {
	ctx, cancel := context.WithDeadlineCause(ctx, deadline, errors.New("budget exhausted"))
	defer cancel()
	ctx, cancel = context.WithTimeoutCause(ctx, timeout, fmt.Errorf("timed out after %v", timeout))
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+c.kubeconfig)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	cmd.WaitDelay = waitDelay
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = fmt.Errorf("%w: %w", cause, err)
		}
		return stdout.String(), fmt.Errorf("%s %s: %w\nstdout:\n%s\nstderr:\n%s", name, strings.Join(args, " "), err, &stdout, &stderr)
	}
	return stdout.String(), nil
}

func (c *cluster) must(t testing.TB, timeout time.Duration, name string, args ...string) string {
	t.Helper()
	out, err := c.run(t.Context(), timeout, "", name, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func (c *cluster) kubectl(t testing.TB, args ...string) string {
	t.Helper()
	return c.must(t, callTimeout, "kubectl", args...)
}

// proxyPath reaches path on port of a xet-pd pod through the API server: peers listen on their node IP only, out of reach of exec and port-forward.
func proxyPath(pod string, port int, path string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy%s", namespace, pod, port, path)
}

// raw GETs path on port of a xet-pd pod; kubectl fails on any answer but 2xx.
func (c *cluster) raw(t testing.TB, pod string, port int, path string) string {
	t.Helper()
	return c.kubectl(t, "get", "--raw", proxyPath(pod, port, path))
}

// peers reads the live registry of the tracker pod.
func (c *cluster) peers(ctx context.Context, tracker string) (apis.PeersResponse, error) {
	var d apis.PeersResponse
	out, err := c.run(ctx, callTimeout, "", "kubectl", "get", "--raw", proxyPath(tracker, trackerPort, apis.PeersPath))
	if err == nil {
		err = json.Unmarshal([]byte(out), &d)
	}
	return d, err
}

// peerStatus returns the tracker's entry for the peer advertising url.
func peerStatus(d apis.PeersResponse, url string) (apis.PeerStatus, bool) {
	i := slices.IndexFunc(d.Peers, func(p apis.PeerStatus) bool { return p.URL == url })
	if i < 0 {
		return apis.PeerStatus{}, false
	}
	return d.Peers[i], true
}

// holdings renders each live peer as its URL with whether it has an upstream and the xorbs and shards it announced, sorted.
func holdings(d apis.PeersResponse) []string {
	var out []string
	for _, p := range d.Peers {
		out = append(out, fmt.Sprintf("%s upstream=%t xorbs=%d shards=%d", p.URL, p.Upstream, p.Holdings, p.Shards))
	}
	slices.Sort(out)
	return out
}

type pod struct{ name, node, hostIP string }

// url is the origin a peer pod advertises: its node's IP, on hostNetwork.
func (p pod) url() string {
	return "http://" + net.JoinHostPort(p.hostIP, strconv.Itoa(peerPort))
}

// pods lists the running pods of the xet-pd namespace that match selector and are not being deleted.
func (c *cluster) pods(t testing.TB, selector string) []pod {
	t.Helper()
	var list struct {
		Items []struct {
			Metadata struct{ Name, DeletionTimestamp string }
			Spec     struct{ NodeName string }
			Status   struct{ Phase, HostIP string }
		}
	}
	if err := json.Unmarshal([]byte(c.kubectl(t, "-n", namespace, "get", "pods", "-l", selector, "-o", "json")), &list); err != nil {
		t.Fatal(err)
	}
	var pods []pod
	for _, p := range list.Items {
		if p.Status.Phase == "Running" && p.Metadata.DeletionTimestamp == "" {
			pods = append(pods, pod{p.Metadata.Name, p.Spec.NodeName, p.Status.HostIP})
		}
	}
	return pods
}

// workers lists the nodes without the control-plane role, sorted; kind names their containers the same.
func (c *cluster) workers(t testing.TB) []string {
	t.Helper()
	var nodes []string
	for _, name := range strings.Fields(c.kubectl(t, "get", "nodes", "-l", "!node-role.kubernetes.io/control-plane", "-o", "name")) {
		nodes = append(nodes, strings.TrimPrefix(name, "node/"))
	}
	slices.Sort(nodes)
	return nodes
}

// reset returns a kept cluster to a cold start: no taint an older run left, no xet-pd objects or test Jobs, empty peer caches.
func (c *cluster) reset(t testing.TB) {
	t.Helper()
	_, _ = c.run(t.Context(), callTimeout, "", "kubectl", "taint", "nodes", "--all", taintKey+"-") // fails when no node carries it
	c.must(t, resetTimeout+callTimeout, "kubectl", "delete", "-k", c.overlay, "--ignore-not-found", "--wait", "--timeout="+resetTimeout.String())
	c.kubectl(t, "-n", "default", "delete", "jobs", "-l", jobSelector)
	for _, node := range c.workers(t) {
		c.must(t, callTimeout, "docker", "exec", node, "rm", "-rf", "/var/lib/xet-pd/peer")
	}
}

// dump logs the state of a failed run; t's context is already canceled in cleanups.
func (c *cluster) dump(t testing.TB) {
	by := c.end.Add(-dumpReserve)
	if time.Now().After(by) {
		t.Log("dump skipped: past its deadline")
		return
	}
	for _, args := range [][]string{
		{"get", "pods", "-A", "-o", "wide"},
		{"get", "events", "-A", "--sort-by=.lastTimestamp"},
		{"-n", namespace, "logs", "deploy/xet-pd-tracker", "--tail=50"},
		{"-n", namespace, "logs", "-l", accessSelector, "--prefix", "--tail=50"},
		{"-n", namespace, "logs", "-l", peerSelector, "--prefix", "--tail=50"},
		{"get", "nodes", "-o", "wide"},
	} {
		out, err := c.runBy(context.Background(), by, callTimeout, "", "kubectl", args...)
		if err != nil {
			t.Log(err)
			continue
		}
		t.Logf("kubectl %s\n%s", strings.Join(args, " "), out)
	}
}

// jobYAML points hf at the peer on the Job's own node through the Downward API, as deploy/k8s/base documents for clients.
const jobYAML = `apiVersion: batch/v1
kind: Job
metadata:
  name: {{.Name}}
  namespace: default
  labels:
    app.kubernetes.io/part-of: xet-pd-e2e
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 180
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: xet-pd-e2e
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: {{.Node}}
      tolerations:
        - key: xetpd-e2e
          operator: Exists
          effect: NoExecute
      containers:
        - name: hf
          image: {{.Image}}
          imagePullPolicy: Never
          env:
            - name: HOST_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP
            - name: HF_ENDPOINT
              value: http://$(HOST_IP):{{.Port}}
            - name: HOME
              value: /work
            - name: HF_HOME
              value: /work/hf
            - name: HF_XET_CACHE
              value: /work/xet
            - name: HF_HUB_DISABLE_TELEMETRY
              value: "1"
            - name: HF_HUB_DISABLE_UPDATE_CHECK
              value: "1"
            - name: HF_HUB_DISABLE_PROGRESS_BARS
              value: "1"
            - name: HF_DEBUG
              value: "1"
            - name: HF_HUB_ETAG_TIMEOUT
              value: "60"
            - name: HF_HUB_DOWNLOAD_TIMEOUT
              value: "60"
          command:
            - sh
            - -ec
            - >-
              sleep 2; hf download {{.Repo}} {{.File}} --revision {{.Revision}} --local-dir /work/out;
              f=/work/out/{{.File}}; sha256sum $f; stat -c %s $f;
              test "$(sha256sum $f|cut -d" " -f1)" = {{.SHA}}; test "$(stat -c %s $f)" = {{.Size}}
          volumeMounts:
            - name: work
              mountPath: /work
      volumes:
        - name: work
          emptyDir: {}
`

var jobTemplate = template.Must(template.New("job").Parse(jobYAML))

// job runs the hf Job name on node to download f and returns its log, which must show the digest of f.
func (c *cluster) job(t testing.TB, name, node string, f fixture) string {
	t.Helper()
	var spec strings.Builder
	data := map[string]any{"Name": name, "Node": node, "Image": hfImage, "Port": peerPort, "Repo": f.repo, "File": f.file, "Revision": f.revision, "SHA": f.sha, "Size": f.size}
	if err := jobTemplate.Execute(&spec, data); err != nil {
		t.Fatal(err)
	}
	if _, err := c.run(t.Context(), callTimeout, spec.String(), "kubectl", "apply", "-f", "-"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(jobTimeout)
	for {
		var job struct {
			Status struct {
				Conditions []struct{ Type, Status string }
			}
		}
		if err := json.Unmarshal([]byte(c.kubectl(t, "-n", "default", "get", "job", name, "-o", "json")), &job); err != nil {
			t.Fatal(err)
		}
		for _, cond := range job.Status.Conditions {
			if cond.Status != "True" {
				continue
			}
			switch cond.Type {
			case "Complete":
				log := c.kubectl(t, "-n", "default", "logs", "job/"+name)
				if !strings.Contains(log, f.sha+"  /work/out/"+f.file) {
					t.Fatalf("job %s log lacks the digest of %s:\n%s", name, f.file, log)
				}
				return log
			case "Failed":
				t.Fatalf("job %s failed:\n%s", name, c.jobReport(t, name))
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s unfinished after %v:\n%s", name, jobTimeout, c.jobReport(t, name))
		}
		time.Sleep(2 * time.Second)
	}
}

// jobReport collects the log and descriptions of a Job that did not complete.
func (c *cluster) jobReport(t testing.TB, name string) string {
	var b strings.Builder
	for _, args := range [][]string{
		{"-n", "default", "logs", "job/" + name},
		{"-n", "default", "describe", "job", name},
		{"-n", "default", "describe", "pods", "-l", "batch.kubernetes.io/job-name=" + name},
	} {
		out, err := c.run(t.Context(), callTimeout, "", "kubectl", args...)
		if err != nil {
			fmt.Fprintf(&b, "%v\n", err)
			continue
		}
		fmt.Fprintf(&b, "kubectl %s\n%s\n", strings.Join(args, " "), out)
	}
	return b.String()
}

// counts is what the assertions read after a step.
type counts struct {
	peers     apis.PeersResponse
	live      float64            // xetpd_tracker_peers
	upstreams float64            // xetpd_tracker_upstream_peers
	lookups   float64            // xetpd_tracker_lookups_total
	seeds     float64            // xetpd_tracker_seed_requests_total{result="ok"}
	client    map[string]float64 // xorb GETs a peer answered clients with 2xx, by node
	relayed   map[string]float64 // xorb GETs a peer answered other peers with 2xx, by node
	pulled    map[string]float64 // xetpd_peer_pulls_total{result="ok"}, by node
	shards    map[string]float64 // xetpd_peer_shard_gets_total{status="200"}, by node
}

// count reads the tracker and the peer pods given by node.
func (c *cluster) count(t testing.TB, tracker string, peers map[string]pod) counts {
	t.Helper()
	d, err := c.peers(t.Context(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	m := clustertest.ParseMetrics(t, c.raw(t, tracker, trackerPort, "/metrics"))
	s := counts{
		peers:     d,
		live:      sample(t, m, "xetpd_tracker_peers"),
		upstreams: sample(t, m, "xetpd_tracker_upstream_peers"),
		lookups:   sample(t, m, "xetpd_tracker_lookups_total"),
		seeds:     sample(t, m, `xetpd_tracker_seed_requests_total{result="ok"}`),
		client:    map[string]float64{},
		relayed:   map[string]float64{},
		pulled:    map[string]float64{},
		shards:    map[string]float64{},
	}
	for node, p := range peers {
		pm := clustertest.ParseMetrics(t, c.raw(t, p.name, peerPort, "/metrics"))
		for key, v := range pm {
			switch {
			case strings.HasPrefix(key, `xetpd_peer_xorb_gets_total{requester="client",status="2`):
				s.client[node] += v
			case strings.HasPrefix(key, `xetpd_peer_xorb_gets_total{requester="peer",status="2`):
				s.relayed[node] += v
			}
		}
		s.pulled[node] = sample(t, pm, `xetpd_peer_pulls_total{result="ok"}`)
		s.shards[node] = sample(t, pm, `xetpd_peer_shard_gets_total{status="200"}`)
	}
	return s
}

func (s counts) String() string {
	return fmt.Sprintf("peers %v; tracker peers gauge %v, upstream peers %v, lookups %v, seed requests ok %v; by node: client xorb 2xx %v, peer xorb 2xx %v, pulls ok %v, shard gets 200 %v",
		holdings(s.peers), s.live, s.upstreams, s.lookups, s.seeds, s.client, s.relayed, s.pulled, s.shards)
}

// sample returns the value of key and fails t when the exposition lacks it.
func sample(t testing.TB, samples map[string]float64, key string) float64 {
	t.Helper()
	v, ok := samples[key]
	if !ok {
		t.Errorf("no sample %s", key)
	}
	return v
}

// checkTypes fails t for each family of want that the exposition of who declares no TYPE for, and returns name=type for the others.
func checkTypes(t testing.TB, who, text string, want []string) []string {
	t.Helper()
	types := map[string]string{}
	for line := range strings.Lines(text) {
		if f := strings.Fields(line); len(f) == 4 && f[0] == "#" && f[1] == "TYPE" {
			types[f[2]] = f[3]
		}
	}
	var found []string
	for _, name := range want {
		if types[name] == "" {
			t.Errorf("%s declares no TYPE for %s", who, name)
			continue
		}
		found = append(found, name+"="+types[name])
	}
	return found
}

// eventually retries check every second until it returns nil and fails t with its last error once timeout or the run deadline has passed.
func (c *cluster) eventually(t testing.TB, timeout time.Duration, what string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := check()
		if err == nil {
			return
		}
		switch now := time.Now(); {
		case now.After(c.deadline):
			t.Fatalf("budget exhausted waiting for %s: %v", what, err)
		case now.After(deadline):
			t.Fatalf("timed out after %v waiting for %s: %v", timeout, what, err)
		}
		time.Sleep(time.Second)
	}
}

// preflight skips, as e2e's TestHFCLI does, when huggingface.co is unreachable or unhealthy; any other failure fails.
func preflight(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, hfUpstream+"/"+model.repo+"/resolve/"+model.revision+"/"+model.file, nil)
	if err != nil {
		t.Fatal(err)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Do(req)
	if err != nil {
		if networkUnavailable(err) {
			t.Skipf("huggingface.co unreachable: %v", err)
		}
		t.Fatalf("upstream HEAD of the fixture: %v", err)
	}
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 500:
		t.Skipf("huggingface.co unhealthy: %s", resp.Status)
	case resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusFound:
		t.Fatalf("upstream HEAD of the fixture = %s", resp.Status)
	}
}

// networkUnavailable reports whether the network never carried the request; anything the far end answered is a failure instead.
func networkUnavailable(err error) bool {
	var dnsErr *net.DNSError
	var opErr *net.OpError
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &dnsErr):
		return true
	case errors.As(err, &opErr):
		return opErr.Timeout() || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH)
	}
	return false
}
