// Package clitest holds the helpers the command tests share.
package clitest

import (
	"testing"
	"time"
)

// Getenv looks keys up in the key/value pairs kv, no pairs being an empty environment; a trailing odd key is ignored.
func Getenv(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return func(k string) string { return m[k] }
}

// ChanWriter hands every write to the test as it happens.
type ChanWriter chan string

func (c ChanWriter) Write(p []byte) (int, error) {
	c <- string(p)
	return len(p), nil
}

// Await receives from ch and fails the test after 10s.
func Await[T any](t testing.TB, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}
