package podnode

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// testLogs receives every slog record of the test binary, klog's too, as klog is bridged to slog at package init.
var testLogs syncBuffer

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(&testLogs, nil)))
	leaked, err := os.CreateTemp("", "podnode-stderr-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stderr := os.Stderr
	os.Stderr = leaked
	code := m.Run()
	out, err := os.ReadFile(leaked.Name())
	_ = leaked.Close()
	_ = os.Remove(leaked.Name())
	out = bytes.ReplaceAll(out, []byte("testing: warning: no tests to run\n"), nil) // m.Run's own, when -run selects nothing here
	if err != nil || len(out) > 0 {
		_, _ = fmt.Fprintf(stderr, "podnode tests wrote to stderr instead of slog (read: %v):\n%s", err, out)
		code = 1
	}
	os.Exit(code)
}

// syncBuffer lets parallel tests read the log while their indexes write it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// Since returns what was written after the first n bytes.
func (b *syncBuffer) Since(n int) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf.Bytes()[n:])
}

// newTestIndex waits out the initial list, so the pods a test creates reach the index through the watch, in order.
func newTestIndex(t *testing.T) (*Index, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset()
	idx, err := newIndex(t.Context(), client)
	if err != nil {
		t.Fatalf("newIndex: %v", err)
	}
	t.Cleanup(idx.Close)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "Synced() after the initial list", idx.Synced)
	return idx, client
}

// newPod mirrors the kubelet, whose PodIP repeats the first of PodIPs.
func newPod(ns, name, node string, phase corev1.PodPhase, ips ...string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: phase},
	}
	for _, ip := range ips {
		pod.Status.PodIPs = append(pod.Status.PodIPs, corev1.PodIP{IP: ip})
	}
	if len(ips) > 0 {
		pod.Status.PodIP = ips[0]
	}
	return pod
}

func mustCreate(t *testing.T, client *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	if _, err := client.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

// mustUpdate bumps the resource version: the fake keeps it as given, and informers drop updates that leave it unchanged.
func mustUpdate(t *testing.T, client *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	rv, _ := strconv.Atoi(pod.ResourceVersion)
	pod.ResourceVersion = strconv.Itoa(rv + 1)
	if _, err := client.CoreV1().Pods(pod.Namespace).Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

func mustDelete(t *testing.T, client *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	if err := client.CoreV1().Pods(pod.Namespace).Delete(t.Context(), pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

var settles atomic.Int64

// settle returns once the index has handled every earlier change: events arrive in order, so a pod created now shows last.
func settle(t *testing.T, client *fake.Clientset, idx *Index) {
	t.Helper()
	n := settles.Add(1)
	ip := fmt.Sprintf("fd00:5e77::%x", n)
	mustCreate(t, client, newPod("settle", "settle-"+strconv.FormatInt(n, 10), "node-settle", corev1.PodRunning, ip))
	waitNode(t, idx, ip, "node-settle")
}

func waitNode(t *testing.T, idx *Index, ip, node string) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("NodeOf(%q) = %q", ip, node), func() bool {
		got, ok := idx.NodeOf(ip)
		return ok && got == node
	})
}

func waitUnknown(t *testing.T, idx *Index, ip string) {
	t.Helper()
	clustertest.WaitFor(t, clustertest.WaitTimeout, fmt.Sprintf("NodeOf(%q) unknown", ip), func() bool {
		_, ok := idx.NodeOf(ip)
		return !ok
	})
}

func wantNode(t *testing.T, idx *Index, ip, node string) {
	t.Helper()
	if got, ok := idx.NodeOf(ip); !ok || got != node {
		t.Fatalf("NodeOf(%q) = %q, %v; want %q, true", ip, got, ok, node)
	}
}

func wantUnknown(t *testing.T, idx *Index, ip string) {
	t.Helper()
	if got, ok := idx.NodeOf(ip); ok {
		t.Fatalf("NodeOf(%q) = %q, true; want unknown", ip, got)
	}
}
