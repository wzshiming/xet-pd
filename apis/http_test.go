package apis

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOrigin(t *testing.T) {
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

func TestAuthorized(t *testing.T) {
	tests := []struct {
		secret, header string
		want           bool
	}{
		{"s3cret", "", false},
		{"s3cret", "Bearer nope", false},
		{"s3cret", "Basic s3cret", false},
		{"s3cret", "Bearer s3cretx", false},
		{"s3cret", "Bearer s3cret", true},
		{"", "", true},
		{"", "Bearer nope", true},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		if tt.header != "" {
			r.Header.Set("Authorization", tt.header)
		}
		if got := Authorized(r, tt.secret); got != tt.want {
			t.Errorf("Authorized(%q) with secret %q = %v, want %v", tt.header, tt.secret, got, tt.want)
		}
	}
}

func TestReadJSON(t *testing.T) {
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
