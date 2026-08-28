package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func TestParseAgentTracker(t *testing.T) {
	_, cfg, err := parseAgent(nil, mapEnv(map[string]string{"XET_PD_TRACKER": "http://env-tracker:8881"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tracker != "http://env-tracker:8881" {
		t.Errorf("tracker = %q, want env value", cfg.Tracker)
	}
	_, cfg, err = parseAgent([]string{"--tracker", "http://flag-tracker:8881"}, mapEnv(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tracker != "http://flag-tracker:8881" {
		t.Errorf("tracker = %q, want flag value", cfg.Tracker)
	}
}

func TestParseTracker(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		f, err := parseTracker(nil, mapEnv(nil))
		if err != nil {
			t.Fatal(err)
		}
		if f.listen != ":8881" {
			t.Errorf("listen = %q, want :8881", f.listen)
		}
		if f.storageDir != "./xet-pd" {
			t.Errorf("storage dir = %q, want ./xet-pd", f.storageDir)
		}
		if f.clusterSecret != "" {
			t.Errorf("cluster secret = %q, want empty", f.clusterSecret)
		}
	})
	t.Run("env fallback and secret file", func(t *testing.T) {
		secretFile := filepath.Join(t.TempDir(), "secret")
		if err := os.WriteFile(secretFile, []byte(" tracker-secret\n"), 0600); err != nil {
			t.Fatal(err)
		}
		f, err := parseTracker(
			[]string{"--cluster-secret-file", secretFile},
			mapEnv(map[string]string{"XET_PD_TRACKER_LISTEN": ":9001"}),
		)
		if err != nil {
			t.Fatal(err)
		}
		if f.listen != ":9001" {
			t.Errorf("listen = %q, want :9001", f.listen)
		}
		if f.clusterSecret != "tracker-secret" {
			t.Errorf("cluster secret = %q, want tracker-secret (trimmed)", f.clusterSecret)
		}
	})
}

func TestParseAgentPeers(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		_, cfg, err := parseAgent(nil, mapEnv(nil))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Peers != nil {
			t.Errorf("peers = %v, want none", cfg.Peers)
		}
		if cfg.PeerTimeout != 2*time.Second {
			t.Errorf("peer timeout = %v, want 2s", cfg.PeerTimeout)
		}
		if cfg.MaxPeerConns != 64 {
			t.Errorf("max peer conns = %d, want 64", cfg.MaxPeerConns)
		}
	})
	t.Run("flags split and trim the list", func(t *testing.T) {
		_, cfg, err := parseAgent(
			[]string{"--peers", "http://a:8880, http://b:8880,,", "--peer-timeout", "500ms", "--max-peer-conns", "8"},
			mapEnv(nil),
		)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"http://a:8880", "http://b:8880"}; !reflect.DeepEqual(cfg.Peers, want) {
			t.Errorf("peers = %v, want %v", cfg.Peers, want)
		}
		if cfg.PeerTimeout != 500*time.Millisecond {
			t.Errorf("peer timeout = %v, want 500ms", cfg.PeerTimeout)
		}
		if cfg.MaxPeerConns != 8 {
			t.Errorf("max peer conns = %d, want 8", cfg.MaxPeerConns)
		}
	})
	t.Run("env fallback and flag override", func(t *testing.T) {
		env := map[string]string{
			"XET_PD_PEERS":          "http://env:8880",
			"XET_PD_PEER_TIMEOUT":   "3s",
			"XET_PD_MAX_PEER_CONNS": "16",
		}
		_, cfg, err := parseAgent(nil, mapEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"http://env:8880"}; !reflect.DeepEqual(cfg.Peers, want) {
			t.Errorf("peers = %v, want %v", cfg.Peers, want)
		}
		if cfg.PeerTimeout != 3*time.Second {
			t.Errorf("peer timeout = %v, want 3s", cfg.PeerTimeout)
		}
		if cfg.MaxPeerConns != 16 {
			t.Errorf("max peer conns = %d, want 16", cfg.MaxPeerConns)
		}

		_, cfg, err = parseAgent([]string{"--peer-timeout", "1s"}, mapEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.PeerTimeout != time.Second {
			t.Errorf("peer timeout = %v, want 1s (flag over env)", cfg.PeerTimeout)
		}
	})
	t.Run("bad env values error", func(t *testing.T) {
		if _, _, err := parseAgent(nil, mapEnv(map[string]string{"XET_PD_PEER_TIMEOUT": "soon"})); err == nil {
			t.Error("bad XET_PD_PEER_TIMEOUT accepted")
		}
		if _, _, err := parseAgent(nil, mapEnv(map[string]string{"XET_PD_MAX_PEER_CONNS": "many"})); err == nil {
			t.Error("bad XET_PD_MAX_PEER_CONNS accepted")
		}
	})
}

func TestParseAgentDefaults(t *testing.T) {
	listen, cfg, err := parseAgent(nil, mapEnv(nil))
	if err != nil {
		t.Fatal(err)
	}
	if listen != ":8880" {
		t.Errorf("listen = %q, want :8880", listen)
	}
	if cfg.Upstream != "https://huggingface.co" {
		t.Errorf("upstream = %q, want https://huggingface.co", cfg.Upstream)
	}
	if cfg.StorageDir != "./xet-pd" {
		t.Errorf("storage dir = %q, want ./xet-pd", cfg.StorageDir)
	}
	if cfg.UpstreamToken != "" || cfg.ExternalURL != "" || cfg.ClusterSecret != "" {
		t.Errorf("token/external/secret = %q/%q/%q, want empty",
			cfg.UpstreamToken, cfg.ExternalURL, cfg.ClusterSecret)
	}
}

func TestParseAgentEnvFallback(t *testing.T) {
	env := map[string]string{
		"XET_PD_LISTEN":         ":9999",
		"XET_PD_UPSTREAM":       "https://env.example",
		"XET_PD_STORAGE_DIR":    "/env/data",
		"XET_PD_EXTERNAL_URL":   "https://ext.example",
		"XET_PD_CLUSTER_SECRET": "env-secret",
	}
	listen, cfg, err := parseAgent(nil, mapEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	if listen != ":9999" {
		t.Errorf("listen = %q, want :9999", listen)
	}
	if cfg.Upstream != "https://env.example" {
		t.Errorf("upstream = %q, want https://env.example", cfg.Upstream)
	}
	if cfg.StorageDir != "/env/data" {
		t.Errorf("storage dir = %q, want /env/data", cfg.StorageDir)
	}
	if cfg.ExternalURL != "https://ext.example" {
		t.Errorf("external URL = %q, want https://ext.example", cfg.ExternalURL)
	}
	if cfg.ClusterSecret != "env-secret" {
		t.Errorf("cluster secret = %q, want env-secret", cfg.ClusterSecret)
	}
}

func TestParseAgentFlagWinsOverEnv(t *testing.T) {
	env := map[string]string{
		"XET_PD_LISTEN":   ":9999",
		"XET_PD_UPSTREAM": "https://env.example",
	}
	listen, cfg, err := parseAgent(
		[]string{"--listen", ":7777", "--upstream", "https://flag.example"},
		mapEnv(env),
	)
	if err != nil {
		t.Fatal(err)
	}
	if listen != ":7777" {
		t.Errorf("listen = %q, want :7777 (flag over env)", listen)
	}
	if cfg.Upstream != "https://flag.example" {
		t.Errorf("upstream = %q, want https://flag.example (flag over env)", cfg.Upstream)
	}
}

func TestParseAgentUpstreamTokenChain(t *testing.T) {
	t.Run("HF_TOKEN is the secondary fallback", func(t *testing.T) {
		_, cfg, err := parseAgent(nil, mapEnv(map[string]string{"HF_TOKEN": "hf-tok"}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.UpstreamToken != "hf-tok" {
			t.Errorf("token = %q, want hf-tok", cfg.UpstreamToken)
		}
	})
	t.Run("XET_PD_UPSTREAM_TOKEN wins over HF_TOKEN", func(t *testing.T) {
		env := map[string]string{"XET_PD_UPSTREAM_TOKEN": "xet-tok", "HF_TOKEN": "hf-tok"}
		_, cfg, err := parseAgent(nil, mapEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.UpstreamToken != "xet-tok" {
			t.Errorf("token = %q, want xet-tok", cfg.UpstreamToken)
		}
	})
	t.Run("flag wins over both", func(t *testing.T) {
		env := map[string]string{"XET_PD_UPSTREAM_TOKEN": "xet-tok", "HF_TOKEN": "hf-tok"}
		_, cfg, err := parseAgent([]string{"--upstream-token", "flag-tok"}, mapEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.UpstreamToken != "flag-tok" {
			t.Errorf("token = %q, want flag-tok", cfg.UpstreamToken)
		}
	})
}

func TestParseAgentClusterSecretFileWinsOverLiteral(t *testing.T) {
	secretFile := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secretFile, []byte("  file-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}

	_, cfg, err := parseAgent(
		[]string{"--cluster-secret", "literal", "--cluster-secret-file", secretFile},
		mapEnv(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterSecret != "file-secret" {
		t.Errorf("cluster secret = %q, want file-secret (file over literal, trimmed)", cfg.ClusterSecret)
	}

	t.Run("literal used when no file", func(t *testing.T) {
		_, cfg, err := parseAgent([]string{"--cluster-secret", "literal"}, mapEnv(nil))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ClusterSecret != "literal" {
			t.Errorf("cluster secret = %q, want literal", cfg.ClusterSecret)
		}
	})

	t.Run("unreadable file errors", func(t *testing.T) {
		_, _, err := parseAgent(
			[]string{"--cluster-secret-file", filepath.Join(t.TempDir(), "missing")},
			mapEnv(nil),
		)
		if err == nil {
			t.Fatal("expected error for missing secret file")
		}
	})
}
