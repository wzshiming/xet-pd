package apis

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewTransport(t *testing.T) {
	t.Parallel()
	tr := NewTransport()
	if tr == http.DefaultTransport || !tr.DisableCompression || tr.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("NewTransport: shared %v, DisableCompression %v, ResponseHeaderTimeout %v, want a clone, true, 30s", tr == http.DefaultTransport, tr.DisableCompression, tr.ResponseHeaderTimeout)
	}
}

func TestNewClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.RedirectHandler("/elsewhere", http.StatusFound))
	defer srv.Close()
	resp, err := NewClient(srv.Client().Transport).Get(srv.URL)
	if err != nil {
		t.Fatalf("NewClient followed the redirect: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("GET = %d, want the 302 itself", resp.StatusCode)
	}
}

func TestPostJSON(t *testing.T) {
	t.Parallel()
	type sent struct{ method, ctype, auth, body string }
	reqs := make(chan sent, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		reqs <- sent{r.Method, r.Header.Get("Content-Type"), r.Header.Get("Authorization"), string(body)}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	for secret, auth := range map[string]string{"s3cret": "Bearer s3cret", "": ""} {
		resp, err := PostJSON(t.Context(), srv.Client(), srv.URL, secret, map[string]int{"n": 1})
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		want := sent{http.MethodPost, "application/json", auth, `{"n":1}`}
		if got := <-reqs; got != want || resp.StatusCode != http.StatusAccepted {
			t.Errorf("PostJSON with secret %q sent %+v, answered %d, want %+v, 202", secret, got, resp.StatusCode, want)
		}
	}
}

func TestDecodeJSON(t *testing.T) {
	t.Parallel()
	const limit = 16
	tests := []struct {
		name, body string
		wantErr    bool
	}{
		{"valid", `{"N":1}`, false},
		{"whitespace up to the limit", `{"N":1}` + strings.Repeat(" ", limit-7), false},
		{"over the limit", `{"N":1}` + strings.Repeat(" ", limit-6), true},
		{"trailing value", `{"N":1}{}`, true},
		{"invalid json", `{"N":`, true},
	}
	for _, tt := range tests {
		var v struct{ N int }
		err := DecodeJSON(strings.NewReader(tt.body), limit, &v)
		if (err != nil) != tt.wantErr || err == nil && v.N != 1 {
			t.Errorf("%s: DecodeJSON = %v, N %d, want error %v", tt.name, err, v.N, tt.wantErr)
		}
	}
}
