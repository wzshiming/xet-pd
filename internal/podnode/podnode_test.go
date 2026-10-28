package podnode

import (
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/wzshiming/xet-pd/internal/clustertest"
)

// The gateway resolves client IPs to nodes through this method set.
var _ interface {
	NodeOf(string) (string, bool)
	Synced() bool
} = (*Index)(nil)

func TestSyncedAfterInitialList(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset(newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.1"))
	var listing atomic.Bool
	gate := make(chan struct{})
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		listing.Store(true)
		<-gate
		return false, nil, nil
	})
	idx, err := newIndex(t.Context(), client)
	if err != nil {
		t.Fatalf("newIndex: %v", err)
	}
	t.Cleanup(idx.Close)
	openGate := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(openGate)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the initial pod list", listing.Load)
	if idx.Synced() {
		t.Fatal("Synced() = true while the initial list is held")
	}
	wantUnknown(t, idx, "10.244.0.1")
	openGate()
	clustertest.WaitFor(t, clustertest.WaitTimeout, "Synced() after the initial list", idx.Synced)
	wantNode(t, idx, "10.244.0.1", "node-a")
}

func TestAddPod(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	mustCreate(t, client, newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.2"))
	waitNode(t, idx, "10.244.0.2", "node-a")
}

func TestUpdatePodIP(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.3")
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.3", "node-a")
	pod.Status.PodIP = "10.244.0.4"
	pod.Status.PodIPs = []corev1.PodIP{{IP: "10.244.0.4"}}
	mustUpdate(t, client, pod)
	waitNode(t, idx, "10.244.0.4", "node-a")
	wantUnknown(t, idx, "10.244.0.3")
}

func TestUpdatePodNode(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.5")
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.5", "node-a")
	pod.Spec.NodeName = "node-b"
	mustUpdate(t, client, pod)
	waitNode(t, idx, "10.244.0.5", "node-b")
}

func TestDeletePod(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.6")
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.6", "node-a")
	mustDelete(t, client, pod)
	waitUnknown(t, idx, "10.244.0.6")
}

func TestSharedIPSurvivesDelete(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	proxy := newPod("kube-system", "kube-proxy-x1", "node-a", corev1.PodRunning, "192.168.0.10")
	exporter := newPod("kube-system", "node-exporter-x1", "node-a", corev1.PodRunning, "192.168.0.10")
	mustCreate(t, client, proxy)
	mustCreate(t, client, exporter)
	waitNode(t, idx, "192.168.0.10", "node-a")
	mustDelete(t, client, proxy)
	settle(t, client, idx)
	wantNode(t, idx, "192.168.0.10", "node-a")
	mustDelete(t, client, exporter)
	waitUnknown(t, idx, "192.168.0.10")
}

// The fake never sends tombstones, so the test hands them to remove as the informer would.
func TestTombstoneReleasesClaim(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	proxy := newPod("kube-system", "kube-proxy-x2", "node-a", corev1.PodRunning, "192.168.0.11")
	exporter := newPod("kube-system", "node-exporter-x2", "node-a", corev1.PodRunning, "192.168.0.11")
	mustCreate(t, client, proxy)
	mustCreate(t, client, exporter)
	settle(t, client, idx)
	wantNode(t, idx, "192.168.0.11", "node-a")
	idx.remove(cache.DeletedFinalStateUnknown{Key: "kube-system/kube-proxy-x2", Obj: proxy})
	wantNode(t, idx, "192.168.0.11", "node-a")
	idx.remove(cache.DeletedFinalStateUnknown{Key: "kube-system/node-exporter-x2", Obj: exporter})
	wantUnknown(t, idx, "192.168.0.11")
}

func TestConflictingClaimsUnknown(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	stale := newPod("default", "web-old", "node-a", corev1.PodRunning, "10.244.0.7")
	mustCreate(t, client, stale)
	waitNode(t, idx, "10.244.0.7", "node-a")
	mustCreate(t, client, newPod("default", "web-new", "node-b", corev1.PodRunning, "10.244.0.7"))
	waitUnknown(t, idx, "10.244.0.7")
	mustDelete(t, client, stale)
	waitNode(t, idx, "10.244.0.7", "node-b")
}

func TestTerminalPodExcluded(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	mustCreate(t, client, newPod("default", "job-done", "node-a", corev1.PodSucceeded, "10.244.0.8"))
	mustCreate(t, client, newPod("default", "job-failed", "node-a", corev1.PodFailed, "10.244.0.9"))
	settle(t, client, idx)
	wantUnknown(t, idx, "10.244.0.8")
	wantUnknown(t, idx, "10.244.0.9")
	web := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.10")
	mustCreate(t, client, web)
	waitNode(t, idx, "10.244.0.10", "node-a")
	web.Status.Phase = corev1.PodSucceeded
	mustUpdate(t, client, web)
	waitUnknown(t, idx, "10.244.0.10")
}

// The fake ignores field selectors, so the reactors check the ones the index sends.
func TestListWatchSelectsNonTerminalPods(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	check := func(verb string, sel fields.Selector) {
		for phase, want := range map[corev1.PodPhase]bool{
			corev1.PodPending: true, corev1.PodRunning: true, corev1.PodSucceeded: false, corev1.PodFailed: false,
		} {
			if got := sel.Matches(fields.Set{"status.phase": string(phase)}); got != want {
				t.Errorf("%s field selector %q matches a %s pod = %v, want %v", verb, sel, phase, got, want)
			}
		}
	}
	var listed, watched atomic.Bool
	client.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		check("list", a.(k8stesting.ListAction).GetListRestrictions().Fields)
		listed.Store(true)
		return false, nil, nil
	})
	client.PrependWatchReactor("pods", func(a k8stesting.Action) (bool, watch.Interface, error) {
		check("watch", a.(k8stesting.WatchAction).GetWatchRestrictions().Fields)
		watched.Store(true)
		return false, nil, nil
	})
	idx, err := newIndex(t.Context(), client)
	if err != nil {
		t.Fatalf("newIndex: %v", err)
	}
	t.Cleanup(idx.Close)
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the pod list and watch", func() bool {
		return listed.Load() && watched.Load()
	})
}

func TestTerminatingPodStillMapped(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.11")
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.11", "node-a")
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	mustUpdate(t, client, pod)
	settle(t, client, idx)
	wantNode(t, idx, "10.244.0.11", "node-a")
}

func TestUnscheduledPodExcluded(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "", corev1.PodPending, "10.244.0.12")
	mustCreate(t, client, pod)
	settle(t, client, idx)
	wantUnknown(t, idx, "10.244.0.12")
	pod.Spec.NodeName = "node-a"
	mustUpdate(t, client, pod)
	waitNode(t, idx, "10.244.0.12", "node-a")
}

func TestNodeOfNormalizes(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	mustCreate(t, client, newPod("default", "v6", "node-a", corev1.PodRunning, "fd00:10:244::13"))
	mustCreate(t, client, newPod("default", "v4", "node-b", corev1.PodRunning, "10.244.0.14"))
	mustCreate(t, client, newPod("default", "v4-mapped", "node-c", corev1.PodRunning, "::ffff:10.244.0.15"))
	waitNode(t, idx, "fd00:10:244::13", "node-a")
	waitNode(t, idx, "10.244.0.14", "node-b")
	waitNode(t, idx, "10.244.0.15", "node-c")
	for _, tc := range []struct{ ip, node string }{
		{"fd00:0010:0244:0000:0000:0000:0000:0013", "node-a"},
		{"fd00:10:244::13%eth0", "node-a"},
		{"::ffff:10.244.0.14", "node-b"},
		{"::ffff:10.244.0.15", "node-c"},
	} {
		wantNode(t, idx, tc.ip, tc.node)
	}
	wantUnknown(t, idx, "not-an-ip")
	wantUnknown(t, idx, "")
}

func TestDualStackPodIPs(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	mustCreate(t, client, newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.16", "fd00:10:244::16"))
	waitNode(t, idx, "10.244.0.16", "node-a")
	waitNode(t, idx, "fd00:10:244::16", "node-a")
}

func TestPodIPWithoutPodIPs(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.17")
	pod.Status.PodIPs = nil
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.17", "node-a")
}

func TestTrimKeepsIndexFields(t *testing.T) {
	t.Parallel()
	now := metav1.Now()
	ips := []corev1.PodIP{{IP: "10.244.0.5"}, {IP: "fd00:10:244::5"}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", ResourceVersion: "42", UID: "uid-1", DeletionTimestamp: &now,
			Labels: map[string]string{"app": "web"}, Annotations: map[string]string{"note": "x"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-1"}},
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
		},
		Spec: corev1.PodSpec{NodeName: "node-a", HostNetwork: true, Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.244.0.5", PodIPs: ips, HostIP: "192.168.0.10",
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: true}},
		},
	}
	want := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", ResourceVersion: "42"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.244.0.5", PodIPs: ips},
	}
	got, err := trim(pod)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("trim(pod) = %+v, %v; want %+v", got, err, want)
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"zone": "z1"}}}
	before := node.DeepCopy()
	got, err = trim(node)
	if err != nil || got != any(node) || !reflect.DeepEqual(node, before) {
		t.Fatalf("trim(node) = %+v, %v; want the node itself, untouched", got, err)
	}
}

func TestCloseStopsUpdates(t *testing.T) {
	t.Parallel()
	idx, client := newTestIndex(t)
	pod := newPod("default", "web", "node-a", corev1.PodRunning, "10.244.0.18")
	mustCreate(t, client, pod)
	waitNode(t, idx, "10.244.0.18", "node-a")
	idx.Close()
	if idx.Synced() {
		t.Fatal("Synced() = true after Close")
	}
	wantUnknown(t, idx, "10.244.0.18")
	pod.Spec.NodeName = "node-b"
	mustUpdate(t, client, pod)
	mustCreate(t, client, newPod("default", "late", "node-b", corev1.PodRunning, "10.244.0.19"))
	wantUnknown(t, idx, "10.244.0.18")
	wantUnknown(t, idx, "10.244.0.19")
	idx.Close()
}

func TestNewRejectsBadConfig(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]*rest.Config{
		"nil":      nil,
		"bad host": {Host: "://bad"},
	} {
		idx, err := New(t.Context(), cfg)
		if err == nil {
			idx.Close()
			t.Errorf("New(%s config) error = nil, want one", name)
		}
	}
}

func TestUnreachableAPIServer(t *testing.T) {
	t.Parallel()
	mark := testLogs.Len()
	idx, err := New(t.Context(), &rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("New with an unreachable apiserver: %v, want no error", err)
	}
	t.Cleanup(idx.Close)
	if idx.Synced() {
		t.Fatal("Synced() = true without an apiserver")
	}
	wantUnknown(t, idx, "10.244.0.20")
	clustertest.WaitFor(t, clustertest.WaitTimeout, `a "podnode: list/watch pods" warning`, func() bool {
		return strings.Contains(testLogs.Since(mark), "podnode: list/watch pods")
	})
	start := time.Now()
	idx.Close()
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Close took %v, want at most 2s", took)
	}
}

func TestKlogRoutedToSlog(t *testing.T) {
	t.Parallel()
	idx, err := newIndex(t.Context(), fake.NewClientset()) // klog is bridged to slog at package init
	if err != nil {
		t.Fatalf("newIndex: %v", err)
	}
	t.Cleanup(idx.Close)
	mark := testLogs.Len()
	klog.ErrorS(errors.New("probe failure"), "podnode klog probe")
	var level string
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the klog probe as a JSON slog record", func() bool {
		for line := range strings.Lines(testLogs.Since(mark)) {
			var rec struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "podnode klog probe" {
				level = rec.Level
				return true
			}
		}
		return false
	})
	if level != "ERROR" {
		t.Fatalf("klog.ErrorS reached slog at level %q, want ERROR", level)
	}
}

func TestConcurrentNewHasNoRace(t *testing.T) {
	// Not parallel and above the other serial tests: a lazy klog setup could race only the binary's first indexes.
	clients := make([]*fake.Clientset, 8) // made before the spinner starts, as the first one parses a large schema
	for i := range clients {
		clients[i] = fake.NewClientset()
	}
	stop, spun := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(spun)
		for range 500 {
			select {
			case <-stop:
				return
			default:
				klog.InfoS("podnode klog probe spin")
			}
		}
	}()
	defer func() {
		close(stop)
		<-spun
	}()
	idxs := make([]*Index, len(clients))
	var wg sync.WaitGroup
	for i, client := range clients {
		wg.Go(func() {
			idx, err := newIndex(t.Context(), client)
			if err != nil {
				t.Errorf("newIndex: %v", err)
				return
			}
			t.Cleanup(idx.Close)
			idxs[i] = idx
		})
	}
	wg.Wait()
	for _, idx := range idxs {
		if idx != nil {
			clustertest.WaitFor(t, clustertest.WaitTimeout, "Synced() after the initial list", idx.Synced)
		}
	}
}

func TestKlogFollowsLaterDefaultLogger(t *testing.T) {
	// Not parallel: it replaces the process-wide slog default, which also redirects the log package.
	newTestIndex(t)
	defer log.SetOutput(log.Writer())
	defer log.SetFlags(log.Flags())
	prev := slog.Default()
	defer slog.SetDefault(prev)
	var late syncBuffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&late, nil)))
	mark := testLogs.Len()
	klog.ErrorS(nil, "podnode klog probe late", "k", "v")
	var level, k string
	clustertest.WaitFor(t, clustertest.WaitTimeout, "the klog probe in the slog default set after the index", func() bool {
		for line := range strings.Lines(late.Since(0)) {
			var rec struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
				K     string `json:"k"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && strings.Contains(rec.Msg, "podnode klog probe late") {
				level, k = rec.Level, rec.K
				return true
			}
		}
		return false
	})
	if level != "ERROR" || k != "v" {
		t.Fatalf("klog.ErrorS reached the new slog default with level %q, k %q; want ERROR, v", level, k)
	}
	if strings.Contains(testLogs.Since(mark), "podnode klog probe late") {
		t.Fatal("klog.ErrorS also reached the slog default replaced before it")
	}
}
