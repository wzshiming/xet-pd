package apis

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOrigin(t *testing.T) {
	t.Parallel()
	good := map[string]string{
		"http://h":             "http://h",
		"https://h:65535/":     "https://h:65535",
		"http://127.0.0.1:1":   "http://127.0.0.1:1",
		"http://[::1]":         "http://[::1]",
		"http://[::1]:8080":    "http://[::1]:8080",
		"http://gw.test:08080": "http://gw.test:08080",
	}
	for raw, want := range good {
		if got, err := Origin(raw); err != nil || got != want {
			t.Errorf("Origin(%q) = %q, %v, want %q", raw, got, err, want)
		}
	}
	bad := []string{"", "h", "//h", "ftp://h", "http:h", "http://", "http://:1", "http://[]:1", "http://h:", "http://[::1]:", "http://h:0", "http://h:65536", "http://h:1x", "http://h/p", "http://h?q", "http://h#f", "http://bob:hunter2@h", "http://bob:hunter2@h/%zz", "hunter2://h"}
	for _, raw := range bad {
		got, err := Origin(raw)
		if err == nil {
			t.Errorf("Origin(%q) = %q, want error", raw, got)
		} else if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "@") {
			t.Errorf("Origin(%q) error echoes the input: %v", raw, err)
		}
	}
}

func TestReadJSON(t *testing.T) {
	t.Parallel()
	const limit = 16
	tests := []struct {
		name, body string
		status     int    // http.StatusOK when accepted
		text       string // rejection body, "" when not pinned
	}{
		{"valid", `{"N":1}`, http.StatusOK, ""},
		{"whitespace up to the limit", `{"N":1}` + strings.Repeat(" ", limit-7), http.StatusOK, ""},
		{"value over the limit", `{"N":` + strings.Repeat("1", limit) + `}`, http.StatusRequestEntityTooLarge, ""},
		{"whitespace over the limit", `{"N":1}` + strings.Repeat(" ", limit-6), http.StatusRequestEntityTooLarge, ""},
		{"trailing value", `{"N":1}{}`, http.StatusBadRequest, "trailing data\n"},
		{"invalid json", "nope", http.StatusBadRequest, ""},
	}
	for _, tt := range tests {
		w := httptest.NewRecorder()
		var v struct{ N int }
		got := ReadJSON(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body)), limit, &v)
		want := tt.status == http.StatusOK
		if got != want || w.Code != tt.status || want && v.N != 1 || tt.text != "" && w.Body.String() != tt.text {
			t.Errorf("%s: ReadJSON = %v, %d %q, N %d, want %v, %d %q", tt.name, got, w.Code, w.Body, v.N, want, tt.status, tt.text)
		}
	}
}

// Mixed versions rely on it: a gateway accepts announces carrying fields it predates.
func TestReadJSONIgnoresUnknownFields(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	var v struct{ N int }
	if !ReadJSON(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"N":1,"node":"n1"}`)), 64, &v) || w.Code != http.StatusOK || v.N != 1 {
		t.Fatalf("ReadJSON with an unknown field = %d %q, N %d; want accepted with N 1", w.Code, w.Body, v.N)
	}
}

func TestAuthorize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		secret, header string
		want           bool
	}{
		{"", "", true},
		{"", "Bearer nope", true},
		{"s3cret", "", false},
		{"s3cret", "Bearer wrong", false},
		{"s3cret", "Basic s3cret", false},
		{"s3cret", "bearer s3cret", false},
		{"s3cret", "Bearer s3cret ", false},
		{"s3cret", "Bearer s3cret", true},
	}
	for _, tt := range tests {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		if tt.header != "" {
			r.Header.Set("Authorization", tt.header)
		}
		code, body := http.StatusOK, ""
		if !tt.want {
			code, body = http.StatusUnauthorized, "unauthorized\n"
		}
		if got := Authorize(w, r, tt.secret); got != tt.want || w.Code != code || w.Body.String() != body {
			t.Errorf("Authorize(%q) with secret %q = %v, %d %q, want %v, %d %q", tt.header, tt.secret, got, w.Code, w.Body, tt.want, code, body)
		}
	}
}

func TestXorbURL(t *testing.T) {
	t.Parallel()
	got := XorbURL("http://g:1", "abc")
	if want := "http://g:1/v1/xorbs/default/abc"; got != want || got != "http://g:1"+XorbPathPrefix+"abc" {
		t.Errorf("XorbURL(%q, %q) = %q, want %q", "http://g:1", "abc", got, want)
	}
}
