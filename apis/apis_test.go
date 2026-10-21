package apis

import (
	"encoding/json"
	"reflect"
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
	if XorbPathPrefix != "/v1/xorbs/default/" {
		t.Fatalf("XorbPathPrefix = %q", XorbPathPrefix)
	}
	if MaxXorbBytes != 128<<20 {
		t.Fatalf("MaxXorbBytes = %d", MaxXorbBytes)
	}
}

func TestPullRequestWire(t *testing.T) {
	t.Parallel()
	in := PullRequest{Hash: "xorb-hash"}
	want := `{"hash":"xorb-hash"}`
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

func TestAnnounceResponseWire(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   AnnounceResponse
		want string
	}{
		{AnnounceResponse{TTL: 30}, `{"ttl":30}`},
		{AnnounceResponse{Resync: true, TTL: 30}, `{"resync":true,"ttl":30}`},
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
		if out != c.in {
			t.Fatalf("round trip\n got %+v\nwant %+v", out, c.in)
		}
	}
}

func TestPeersResponseWire(t *testing.T) {
	t.Parallel()
	in := PeersResponse{
		Peers:     []PeerStatus{{ID: "peer-1", URL: "http://peer-1:8080", Epoch: "epoch-1", Expiry: time.Date(2026, 9, 23, 14, 0, 0, 0, time.UTC), Holdings: 3}},
		XorbBytes: 1024,
		XorbGets:  XorbGets{Peer: 2, Direct: 1},
	}
	want := `{"peers":[{"id":"peer-1","url":"http://peer-1:8080","epoch":"epoch-1","expiry":"2026-09-23T14:00:00Z","holdings":3}],"xorb_bytes":1024,"xorb_gets":{"peer":2,"direct":1}}`
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
		{PeersResponse{Peers: []PeerStatus{}}, `{"peers":[],"xorb_bytes":0,"xorb_gets":{"peer":0,"direct":0}}`},
		{PeersResponse{}, `{"peers":null,"xorb_bytes":0,"xorb_gets":{"peer":0,"direct":0}}`},
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
