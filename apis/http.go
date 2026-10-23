package apis

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Origin canonicalizes an absolute http(s) origin to scheme://host[:port], dropping a lone trailing slash; it rejects userinfo, a path, query or fragment, "host:" and ports outside 1..65535, and its errors never echo the input.
func Origin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("invalid url") // the parse error quotes raw, credentials included
	}
	port, err := strconv.Atoi(u.Port())
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return "", errors.New("scheme is not http(s)")
	case u.Hostname() == "" || u.Opaque != "":
		return "", errors.New("missing host")
	case strings.HasSuffix(u.Host, ":"+u.Port()) && (err != nil || port < 1 || port > 65535):
		return "", errors.New("invalid port")
	case u.User != nil:
		return "", errors.New("userinfo not allowed")
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "":
		return "", errors.New("query and fragment not allowed")
	case u.Path != "" && u.Path != "/":
		return "", errors.New("base path not supported")
	}
	return u.Scheme + "://" + u.Host, nil
}

// Authorize answers 401 unless r carries the bearer secret; an empty secret authorizes every request.
func Authorize(w http.ResponseWriter, r *http.Request, secret string) bool {
	if secret == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+secret)) == 1 {
		return true
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

// XorbURL is origin + XorbPathPrefix + hash: where peers pull a xorb from the gateway, the gateway relays it from a holder and xet clients fetch it as the gateway's reconstruction replies direct them.
func XorbURL(origin, hash string) string { return origin + XorbPathPrefix + hash }

// ReadJSON decodes a request body of exactly one JSON value within limit bytes into v; otherwise it answers 413 or 400 and reports false.
func ReadJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	err := decode(http.MaxBytesReader(w, r.Body, limit), v)
	if err == nil {
		return true
	}
	status := http.StatusBadRequest
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok { // the limit may hit while draining trailing whitespace
		status = http.StatusRequestEntityTooLarge
	}
	http.Error(w, err.Error(), status)
	return false
}

// decode reads exactly one JSON value from r into v; ReadJSON and DecodeJSON share it so requests and replies obey the same rule.
func decode(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	if err := dec.Decode(v); err != nil {
		return err
	}
	switch err := dec.Decode(&struct{}{}); err {
	case io.EOF:
		return nil
	case nil:
		return errors.New("trailing data")
	default:
		return err
	}
}
