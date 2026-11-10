package apis

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWireConstants(t *testing.T) {
	t.Parallel()
	if AnnouncePath != "/xet-pd/v1/announce" {
		t.Fatalf("AnnouncePath = %q", AnnouncePath)
	}
	if PeerHeader != "X-Xet-PD-Peer" {
		t.Fatalf("PeerHeader = %q", PeerHeader)
	}
	if PullPath != "/xet-pd/v1/pull" {
		t.Fatalf("PullPath = %q", PullPath)
	}
	if PeersPath != "/xet-pd/v1/peers" {
		t.Fatalf("PeersPath = %q", PeersPath)
	}
	if LookupPath != "/xet-pd/v1/lookup" {
		t.Fatalf("LookupPath = %q", LookupPath)
	}
	if ShardPathPrefix != "/xet-pd/v1/shards/" {
		t.Fatalf("ShardPathPrefix = %q", ShardPathPrefix)
	}
	if MaxAnnounceBytes != 32<<20 || MaxLookupBytes != 32<<20 || MaxShardBytes != 64<<20 || MaxReplyBytes != 4096 {
		t.Fatalf("MaxAnnounceBytes, MaxLookupBytes, MaxShardBytes, MaxReplyBytes = %d, %d, %d, %d", MaxAnnounceBytes, MaxLookupBytes, MaxShardBytes, MaxReplyBytes)
	}
	if MaxAnnounceReplyBytes != 256<<10 {
		t.Fatalf("MaxAnnounceReplyBytes = %d", MaxAnnounceReplyBytes)
	}
	if MaxPullBytes != 1<<20 || MaxPullBatch != 256 {
		t.Fatalf("MaxPullBytes, MaxPullBatch = %d, %d", MaxPullBytes, MaxPullBatch)
	}
	if XorbPathPrefix != "/v1/xorbs/default/" {
		t.Fatalf("XorbPathPrefix = %q", XorbPathPrefix)
	}
	if MaxXorbBytes != 128<<20 {
		t.Fatalf("MaxXorbBytes = %d", MaxXorbBytes)
	}
	if MaxIDLen != 128 {
		t.Fatalf("MaxIDLen = %d", MaxIDLen)
	}
}

func TestPullRequestWire(t *testing.T) {
	t.Parallel()
	// Unkeyed, so a field added or dropped fails to compile here.
	in := PullRequest{[]string{"aa"}, []string{"f1"}, true}
	want := `{"xorbs":["aa"],"shards":["f1"],"seed":true}`
	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != want {
		t.Fatalf("encoded\n got %s\nwant %s", encoded, want)
	}
	var out PullRequest
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestPullRequestBatchWire(t *testing.T) {
	t.Parallel()
	in := PullRequest{Xorbs: []string{"aa", "bb"}, Shards: []string{"f1"}, Seed: true}
	want := `{"xorbs":["aa","bb"],"shards":["f1"],"seed":true}`
	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != want {
		t.Fatalf("encoded\n got %s\nwant %s", encoded, want)
	}
	var out PullRequest
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestPullRequestOmitsEmptyFields(t *testing.T) {
	t.Parallel()
	for _, in := range []PullRequest{{}, {Xorbs: []string{}, Shards: []string{}}} {
		encoded, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `{}` {
			t.Fatalf("encoded %+v\n got %s\nwant {}", in, encoded)
		}
	}
}

func TestPeerWire(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   Peer
		want string
	}{
		{Peer{ID: "peer-1", URL: "http://peer-1:8080"}, `{"id":"peer-1","url":"http://peer-1:8080"}`},
		{Peer{ID: "peer-1", URL: "http://peer-1:8080", Upstream: true}, `{"id":"peer-1","url":"http://peer-1:8080","upstream":true}`},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Fatalf("encoded\n got %s\nwant %s", got, c.want)
		}
		var out Peer
		if err := json.Unmarshal(got, &out); err != nil {
			t.Fatal(err)
		}
		if out != c.in {
			t.Fatalf("round trip\n got %+v\nwant %+v", out, c.in)
		}
	}
}

func TestFileWire(t *testing.T) {
	t.Parallel()
	in := File{Hash: "file-hash", SHA256: "file-sha256"}
	want := `{"hash":"file-hash","sha256":"file-sha256"}`
	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != want {
		t.Fatalf("encoded\n got %s\nwant %s", encoded, want)
	}
	var out File
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestAnnounceRequestWire(t *testing.T) {
	t.Parallel()
	in := AnnounceRequest{
		Peer:   Peer{ID: "peer-1", URL: "http://peer-1:8080"},
		Epoch:  "epoch-1",
		Full:   true,
		Add:    []string{"aa", "bb"},
		Remove: []string{"cc"},
	}
	want := `{"peer":{"id":"peer-1","url":"http://peer-1:8080"},"epoch":"epoch-1","full":true,"add":["aa","bb"],"remove":["cc"]}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out AnnounceRequest
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestAnnounceRequestEveryFieldWire(t *testing.T) {
	t.Parallel()
	// Unkeyed, so a field added or dropped fails to compile here.
	in := AnnounceRequest{
		Peer{ID: "peer-1", URL: "http://peer-1:8080", Upstream: true},
		"epoch-1",
		true,
		[]string{"aa"},
		[]string{"cc"},
		[]File{{Hash: "f1", SHA256: "s1"}},
		[]string{"f2"},
	}
	want := `{"peer":{"id":"peer-1","url":"http://peer-1:8080","upstream":true},"epoch":"epoch-1","full":true,"add":["aa"],"remove":["cc"],"add_shards":[{"hash":"f1","sha256":"s1"}],"remove_shards":["f2"]}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out AnnounceRequest
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestAnnounceRequestShardsWire(t *testing.T) {
	t.Parallel()
	in := AnnounceRequest{
		Peer:         Peer{ID: "peer-1", URL: "http://peer-1:8080"},
		Epoch:        "epoch-1",
		AddShards:    []File{{Hash: "f1", SHA256: "s1"}},
		RemoveShards: []string{"f2"},
	}
	want := `{"peer":{"id":"peer-1","url":"http://peer-1:8080"},"epoch":"epoch-1","add_shards":[{"hash":"f1","sha256":"s1"}],"remove_shards":["f2"]}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out AnnounceRequest
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestAnnounceRequestOmitsEmptyShards(t *testing.T) {
	t.Parallel()
	in := AnnounceRequest{Peer: Peer{ID: "peer-1", URL: "http://peer-1:8080"}, Epoch: "epoch-1", AddShards: []File{}, RemoveShards: []string{}}
	want := `{"peer":{"id":"peer-1","url":"http://peer-1:8080"},"epoch":"epoch-1"}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
}

func TestAnnounceRequestOmitsEmptyDelta(t *testing.T) {
	t.Parallel()
	in := AnnounceRequest{Peer: Peer{ID: "peer-1", URL: "http://peer-1:8080"}, Epoch: "epoch-1"}
	want := `{"peer":{"id":"peer-1","url":"http://peer-1:8080"},"epoch":"epoch-1"}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
}

func TestValidLabel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s      string
		maxLen int
		want   bool
	}{
		{"", MaxIDLen, false},
		{"a", MaxIDLen, true},
		{strings.Repeat("a", MaxIDLen), MaxIDLen, true},
		{strings.Repeat("a", MaxIDLen+1), MaxIDLen, false},
		{"a b", MaxIDLen, false},
		{"a/b", MaxIDLen, false},
		{"A-Z_0.9", MaxIDLen, true},
		{"é", MaxIDLen, false},
	}
	for _, c := range cases {
		if got := ValidLabel(c.s, c.maxLen); got != c.want {
			t.Errorf("ValidLabel(%q, %d) = %v, want %v", c.s, c.maxLen, got, c.want)
		}
	}
}

func TestAnnounceResponseWire(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   AnnounceResponse
		want string
	}{
		{AnnounceResponse{TTL: 30}, `{"ttl":30}`},
		{AnnounceResponse{Resync: true, TTL: 30}, `{"resync":true,"ttl":30}`},
		{AnnounceResponse{TTL: 30, MinSeeds: 2}, `{"ttl":30,"min_seeds":2}`},
		// Upstreams keep the order given: sorting them is the tracker's job.
		{
			AnnounceResponse{TTL: 30, Upstreams: []Peer{{ID: "p2", URL: "http://p2", Upstream: true}, {ID: "p1", URL: "http://p1", Upstream: true}}},
			`{"ttl":30,"upstreams":[{"id":"p2","url":"http://p2","upstream":true},{"id":"p1","url":"http://p1","upstream":true}]}`,
		},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Fatalf("encoded\n got %s\nwant %s", got, c.want)
		}
		var out AnnounceResponse
		if err := json.Unmarshal(got, &out); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(out, c.in) {
			t.Fatalf("round trip\n got %+v\nwant %+v", out, c.in)
		}
	}
}

func TestPeersResponseWire(t *testing.T) {
	t.Parallel()
	expiry := time.Date(2026, 9, 23, 14, 0, 0, 0, time.UTC)
	in := PeersResponse{Peers: []PeerStatus{
		{ID: "peer-1", URL: "http://peer-1:8080", Upstream: true, Epoch: "epoch-1", Expiry: expiry, Holdings: 3, Shards: 4},
		{ID: "peer-2", URL: "http://peer-2:8080", Epoch: "epoch-2", Expiry: expiry},
	}}
	want := `{"peers":[` +
		`{"id":"peer-1","url":"http://peer-1:8080","upstream":true,"epoch":"epoch-1","expiry":"2026-09-23T14:00:00Z","holdings":3,"shards":4},` +
		`{"id":"peer-2","url":"http://peer-2:8080","epoch":"epoch-2","expiry":"2026-09-23T14:00:00Z","holdings":0,"shards":0}]}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out PeersResponse
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestPeersResponseEmptyPeers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   PeersResponse
		want string
	}{
		{PeersResponse{Peers: []PeerStatus{}}, `{"peers":[]}`},
		{PeersResponse{}, `{"peers":null}`},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Fatalf("encoded\n got %s\nwant %s", got, c.want)
		}
	}
}

func TestLookupRequestWire(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   LookupRequest
		want string
	}{
		{LookupRequest{Peer: "peer-1", Xorbs: []string{"aa"}, SHA256s: []string{"s1"}, Files: []string{"f1"}}, `{"peer":"peer-1","xorbs":["aa"],"sha256s":["s1"],"files":["f1"]}`},
		{LookupRequest{}, `{}`},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != c.want {
			t.Fatalf("encoded\n got %s\nwant %s", got, c.want)
		}
		var out LookupRequest
		if err := json.Unmarshal(got, &out); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(out, c.in) {
			t.Fatalf("round trip\n got %+v\nwant %+v", out, c.in)
		}
	}
}

func TestFileLocationWire(t *testing.T) {
	t.Parallel()
	in := FileLocation{File: File{Hash: "f1", SHA256: "s1"}, Peers: []Peer{{ID: "peer-1", URL: "http://peer-1:8080"}}}
	want := `{"hash":"f1","sha256":"s1","peers":[{"id":"peer-1","url":"http://peer-1:8080"}]}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out FileLocation
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestLookupResponseWire(t *testing.T) {
	t.Parallel()
	peer := Peer{ID: "peer-1", URL: "http://peer-1:8080"}
	loc := FileLocation{File: File{Hash: "f1", SHA256: "s1"}, Peers: []Peer{peer}}
	in := LookupResponse{
		Xorbs:   map[string][]Peer{"aa": {peer}},
		SHA256s: map[string]FileLocation{"s1": loc},
		Files:   map[string]FileLocation{"f1": loc},
	}
	want := `{"xorbs":{"aa":[{"id":"peer-1","url":"http://peer-1:8080"}]},` +
		`"sha256s":{"s1":{"hash":"f1","sha256":"s1","peers":[{"id":"peer-1","url":"http://peer-1:8080"}]}},` +
		`"files":{"f1":{"hash":"f1","sha256":"s1","peers":[{"id":"peer-1","url":"http://peer-1:8080"}]}}}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out LookupResponse
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestLookupResponseOmitsEmptyMaps(t *testing.T) {
	t.Parallel()
	for _, in := range []LookupResponse{
		{},
		{Xorbs: map[string][]Peer{}, SHA256s: map[string]FileLocation{}, Files: map[string]FileLocation{}},
	} {
		got, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != `{}` {
			t.Fatalf("encoded %+v\n got %s\nwant {}", in, got)
		}
	}
}

func TestShardResponseWire(t *testing.T) {
	t.Parallel()
	in := ShardResponse{Shard: []byte("shard"), Offsets: map[string][]uint64{"aa": {10, 25}}}
	want := `{"shard":"c2hhcmQ=","offsets":{"aa":[10,25]}}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("encoded\n got %s\nwant %s", got, want)
	}
	var out ShardResponse
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}
