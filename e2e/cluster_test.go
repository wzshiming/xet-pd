//go:build e2e_cluster

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/download"

	"github.com/wzshiming/xet-pd/apis"
)

// The fixture is a pinned public xet-backed file on huggingface.co.
const (
	clusterRepo     = "hf-internal-testing/tiny-random-LlamaForCausalLM"
	clusterFile     = "model.safetensors"
	clusterRevision = "9fb191250dd56d0ba7ec9785a025ed29c03d5998"
	clusterFileSHA  = "49c20f32c6c597480fcaec5df2f86c645eabea765cbea1e67886dbae45e5c992"
	clusterFileSize = 4131280
	clusterTimeout  = 3 * time.Minute  // the whole test
	clusterRequest  = 20 * time.Second // one control request
	clusterXetLog   = "Downloading file from Xet Storage"
)

// cluster reaches the gateway from inside the Compose network.
type cluster struct {
	t       *testing.T
	ctx     context.Context
	gateway string
	control *http.Client // never follows redirects
}

func TestComposeCluster(t *testing.T) {
	endpoint := os.Getenv("HF_ENDPOINT")
	if endpoint == "" {
		t.Skip("HF_ENDPOINT unset: this test runs inside compose.cluster.yaml")
	}
	hfPath, err := exec.LookPath("hf")
	if err != nil {
		t.Fatalf("the verifier needs the hf CLI: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), clusterTimeout)
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = clusterRequest
	defer transport.CloseIdleConnections()
	c := &cluster{t: t, ctx: ctx, gateway: endpoint, control: &http.Client{
		Transport:     transport,
		Timeout:       clusterRequest,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	resolveURL := endpoint + "/" + clusterRepo + "/resolve/" + clusterRevision + "/" + clusterFile
	clusterHFCheck(t, ctx, hfPath, endpoint)

	digest, size, coldLog := clusterHFDownload(t, ctx, hfPath, endpoint)
	if digest != clusterFileSHA || size != clusterFileSize {
		t.Fatalf("cold hf download: sha256 %s, %d bytes; want %s, %d", digest, size, clusterFileSHA, clusterFileSize)
	}

	var fileHash string
	c.wait("the gateway to advertise the xet hash", func() error {
		resp, body, err := c.get(http.MethodHead, resolveURL)
		if err != nil {
			return err
		}
		if fileHash = resp.Header.Get("X-Xet-Hash"); resp.StatusCode != http.StatusOK || fileHash == "" {
			return fmt.Errorf("HEAD resolve = %d %q without X-Xet-Hash", resp.StatusCode, body)
		}
		return nil
	})
	c.wait("the gateway to finish seeding and evict its cached xorbs", func() error {
		d, err := c.diag()
		if err != nil {
			return err
		}
		return clusterSeeded(d)
	})
	resp, body, err := c.get(http.MethodGet, endpoint+"/v2/reconstructions/"+fileHash)
	if err != nil {
		t.Fatalf("reconstruction v2 of %s: %v", fileHash, err)
	}
	var v2 download.ReconstructionResponseV2
	if err := json.Unmarshal(body, &v2); resp.StatusCode != http.StatusOK || err != nil {
		t.Fatalf("reconstruction v2 of %s = %d %q: %v", fileHash, resp.StatusCode, body, err)
	}
	var fetchURLs []string
	for _, xorb := range v2.Xorbs {
		for _, f := range xorb {
			address, err := url.Parse(f.URL)
			if err != nil || address.Hostname() == "" || (address.Scheme != "http" && address.Scheme != "https") {
				t.Fatalf("invalid reconstruction xorb URL %q", f.URL)
			}
			fetchURLs = append(fetchURLs, f.URL)
		}
	}
	if len(fetchURLs) == 0 {
		t.Fatalf("reconstruction of %s lists no xorb fetch URL", fileHash)
	}

	before, err := c.diag()
	if err != nil {
		t.Fatal(err)
	}
	digest, size, warmLog := clusterHFDownload(t, ctx, hfPath, endpoint)
	if digest != clusterFileSHA || size != clusterFileSize {
		t.Fatalf("warm hf download: sha256 %s, %d bytes; want %s, %d", digest, size, clusterFileSHA, clusterFileSize)
	}
	if !strings.Contains(warmLog, clusterXetLog) {
		t.Errorf("warm hf log does not announce Xet Storage:\n%s", warmLog)
	}
	after, err := c.diag()
	if err != nil {
		t.Fatal(err)
	}
	if after.XorbGets != before.XorbGets {
		t.Errorf("gateway xorb gets %+v -> %+v during the warm hf download, want none", before.XorbGets, after.XorbGets)
	}
	if err := clusterSeeded(after); err != nil {
		t.Errorf("after the warm hf download: %v", err)
	}
	t.Logf("file %s (%d bytes): first hf download (xet %t), warm hf download via reconstruction URLs %v; gateway stages %d bytes, xorb gets %+v",
		fileHash, size, strings.Contains(coldLog, clusterXetLog), fetchURLs, after.XorbBytes, after.XorbGets)
}

// clusterHF runs one hf command against the gateway with a throwaway HOME and caches, so nothing warms across calls.
func clusterHF(t *testing.T, ctx context.Context, hfPath, endpoint string, args ...string) (string, error) {
	t.Helper()
	home := t.TempDir()
	cmd := exec.CommandContext(ctx, hfPath, args...)
	for _, kv := range os.Environ() {
		if key, _, _ := strings.Cut(kv, "="); !strings.HasPrefix(key, "HF_") && key != "HUGGING_FACE_HUB_TOKEN" && key != "HOME" {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env,
		"HOME="+home, "HF_HOME="+filepath.Join(home, "hf"), "HF_XET_CACHE="+filepath.Join(home, "xet"),
		"HF_ENDPOINT="+endpoint, "HF_HUB_DISABLE_TELEMETRY=1", "HF_HUB_DISABLE_UPDATE_CHECK=1", "HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_DEBUG=1", // the CLI forces info verbosity otherwise
		"HF_HUB_ETAG_TIMEOUT=20", "HF_HUB_DOWNLOAD_TIMEOUT=60",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// clusterHFCheck fails unless hf env reports hf_xet installed and logs the huggingface_hub and hf_xet versions.
func clusterHFCheck(t *testing.T, ctx context.Context, hfPath, endpoint string) {
	t.Helper()
	out, err := clusterHF(t, ctx, hfPath, endpoint, "env")
	if err != nil {
		t.Fatalf("hf env: %v\n%s", err, out)
	}
	var versions []string
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "- huggingface_hub version: ") || strings.HasPrefix(line, "- hf_xet: ") {
			versions = append(versions, strings.TrimPrefix(line, "- "))
		}
	}
	if len(versions) != 2 || strings.HasSuffix(versions[1], "N/A") {
		t.Fatalf("hf env must report huggingface_hub with hf_xet installed, got %q:\n%s", versions, out)
	}
	t.Logf("%s: %s", hfPath, strings.Join(versions, ", "))
}

// clusterHFDownload fetches the fixture through the gateway with a cold hf client and returns the file's sha256, size and the CLI output.
func clusterHFDownload(t *testing.T, ctx context.Context, hfPath, endpoint string) (string, int64, string) {
	t.Helper()
	dir := t.TempDir()
	out, err := clusterHF(t, ctx, hfPath, endpoint, "download", clusterRepo, clusterFile, "--revision", clusterRevision, "--local-dir", dir)
	if err != nil {
		t.Fatalf("hf download: %v\n%s", err, out)
	}
	f, err := os.Open(filepath.Join(dir, clusterFile))
	if err != nil {
		t.Fatalf("hf download left no file: %v\n%s", err, out)
	}
	defer func() { _ = f.Close() }()
	digest, size, err := clusterDigest(f)
	if err != nil {
		t.Fatal(err)
	}
	return digest, size, out
}

// wait polls cond every 100ms until it returns nil; the test deadline fails with the last error.
func (c *cluster) wait(what string, cond func() error) {
	c.t.Helper()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		err := cond()
		if err == nil {
			return
		}
		select {
		case <-tick.C:
		case <-c.ctx.Done():
			c.t.Fatalf("waiting for %s: %v; last: %v", what, c.ctx.Err(), err)
		}
	}
}

// get performs one control request without following redirects and returns the response with its body.
func (c *cluster) get(method, url string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(c.ctx, method, url, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.control.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp, body, err
}

func (c *cluster) diag() (apis.PeersResponse, error) {
	var d apis.PeersResponse
	resp, body, err := c.get(http.MethodGet, c.gateway+apis.PeersPath)
	if err != nil {
		return d, err
	}
	if resp.StatusCode != http.StatusOK {
		return d, fmt.Errorf("GET %s = %d %q", apis.PeersPath, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return d, fmt.Errorf("GET %s: %w", apis.PeersPath, err)
	}
	return d, nil
}

func clusterSeeded(d apis.PeersResponse) error {
	held := false
	for _, p := range d.Peers {
		held = held || p.Holdings > 0
	}
	if !held {
		return fmt.Errorf("gateway reports no cached xorb on any peer")
	}
	if d.XorbBytes != 0 {
		return fmt.Errorf("gateway still stages %d bytes; seeding needs -xorb-cache-size 1 (XET_PD_XORB_CACHE_SIZE)", d.XorbBytes)
	}
	return nil
}

func clusterDigest(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
