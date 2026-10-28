// Package podnode keeps a live index from pod IP to node name by watching every pod in the cluster.
package podnode

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	closeTimeout = 5 * time.Second
	warnInterval = time.Minute
	activePods   = "status.phase!=Succeeded,status.phase!=Failed"
)

// klog's logger is an unlocked global, safe to set only in init, so it defers to whatever slog's default is at log time.
func init() {
	klog.SetSlogLogger(slog.New(currentDefault{}))
}

type currentDefault struct {
	with []func(slog.Handler) slog.Handler
}

func (currentDefault) Enabled(ctx context.Context, level slog.Level) bool {
	return slog.Default().Handler().Enabled(ctx, level)
}

func (h currentDefault) Handle(ctx context.Context, r slog.Record) error {
	d := slog.Default().Handler()
	for _, with := range h.with {
		d = with(d)
	}
	return d.Handle(ctx, r)
}

func (h currentDefault) WithAttrs(attrs []slog.Attr) slog.Handler {
	return currentDefault{append(slices.Clip(h.with), func(d slog.Handler) slog.Handler { return d.WithAttrs(attrs) })}
}

func (h currentDefault) WithGroup(name string) slog.Handler {
	return currentDefault{append(slices.Clip(h.with), func(d slog.Handler) slog.Handler { return d.WithGroup(name) })}
}

// classicListWatch keeps the reflector off watch-list, which retries an unreachable API server without calling the watch error handler or heeding cancellation.
type classicListWatch struct{ cache.ListWatch }

func (*classicListWatch) IsWatchListSemanticsUnSupported() bool { return true }

// podRecord is what one pod adds to the index; the zero value adds nothing.
type podRecord struct {
	node string
	ips  []netip.Addr // sorted and compacted so that equal IP sets compare equal
}

// Index maps pod IPs to node names; an IP claimed by pods on different nodes maps to none.
type Index struct {
	mu      sync.RWMutex
	pods    map[string]podRecord             // by informer key
	claims  map[netip.Addr]map[string]string // pod key to node, per IP
	nodes   map[netip.Addr]string            // set only where every claim on the IP names the same node
	closed  bool
	cancel  context.CancelFunc
	stopped chan struct{}
	synced  func() bool
	once    sync.Once
}

// New starts watching the pods of the cluster cfg reaches without waiting for the first list; an unreachable API server only keeps Synced false.
func New(ctx context.Context, cfg *rest.Config) (*Index, error) {
	if cfg == nil {
		return nil, errors.New("kubernetes config is required")
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newIndex(ctx, client)
}

func newIndex(ctx context.Context, client kubernetes.Interface) (*Index, error) {
	x := &Index{
		pods:    map[string]podRecord{},
		claims:  map[netip.Addr]map[string]string{},
		nodes:   map[netip.Addr]string{},
		stopped: make(chan struct{}),
	}
	pods := client.CoreV1().Pods(metav1.NamespaceAll)
	informer := cache.NewSharedIndexInformerWithOptions(&classicListWatch{cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
			o.FieldSelector = activePods
			return pods.List(ctx, o)
		},
		WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
			o.FieldSelector = activePods
			return pods.Watch(ctx, o)
		},
	}}, &corev1.Pod{}, cache.SharedIndexInformerOptions{})
	if err := informer.SetTransform(trim); err != nil {
		return nil, err
	}
	var lastWarn time.Time // the reflector calls the handler from a single goroutine
	err := informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, _ *cache.Reflector, err error) {
		if ctx.Err() == nil && time.Since(lastWarn) >= warnInterval {
			lastWarn = time.Now()
			slog.Warn("podnode: list/watch pods", "err", err, "synced", x.Synced())
		}
	})
	if err != nil {
		return nil, err
	}
	reg, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    x.update,
		UpdateFunc: func(_, obj any) { x.update(obj) },
		DeleteFunc: x.remove,
	})
	if err != nil {
		return nil, err
	}
	x.synced = reg.HasSynced
	ctx, x.cancel = context.WithCancel(ctx)
	go func() {
		defer close(x.stopped)
		informer.RunWithContext(ctx)
	}()
	return x, nil
}

// NodeOf answers from the pods seen so far, so before Synced it may miss pods that exist.
func (x *Index) NodeOf(ip string) (node string, ok bool) {
	addr, ok := normalize(ip)
	if !ok {
		return "", false
	}
	x.mu.RLock()
	defer x.mu.RUnlock()
	node, ok = x.nodes[addr]
	return node, ok
}

// Synced reports whether every pod of the first full list has been indexed; it then stays true until Close.
func (x *Index) Synced() bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return !x.closed && x.synced()
}

// Close waits at most closeTimeout for the watch to stop; from then on x maps no IP and ignores late events.
func (x *Index) Close() {
	x.once.Do(func() {
		x.mu.Lock()
		x.closed = true
		x.pods, x.claims, x.nodes = nil, nil, nil
		x.mu.Unlock()
		x.cancel()
		select {
		case <-x.stopped:
		case <-time.After(closeTimeout):
			slog.Warn("podnode: pod informer still running after close", "timeout", closeTimeout)
		}
	})
}

func (x *Index) update(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(pod); err == nil {
		x.set(key, contribution(pod))
	}
}

func (x *Index) remove(obj any) {
	if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err == nil {
		x.set(key, podRecord{})
	}
}

// set replaces what the pod under key adds to the index with rec.
func (x *Index) set(key string, rec podRecord) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	old := x.pods[key]
	if old.node == rec.node && slices.Equal(old.ips, rec.ips) {
		return
	}
	for _, ip := range old.ips {
		delete(x.claims[ip], key)
		x.derive(ip)
	}
	delete(x.pods, key)
	if rec.node == "" {
		return
	}
	x.pods[key] = rec
	for _, ip := range rec.ips {
		if x.claims[ip] == nil {
			x.claims[ip] = map[string]string{}
		}
		x.claims[ip][key] = rec.node
		x.derive(ip)
	}
}

func (x *Index) derive(ip netip.Addr) {
	node := ""
	for _, n := range x.claims[ip] {
		if node != "" && n != node {
			delete(x.nodes, ip)
			return
		}
		node = n
	}
	if node == "" {
		delete(x.claims, ip)
		delete(x.nodes, ip)
		return
	}
	x.nodes[ip] = node
}

func contribution(pod *corev1.Pod) podRecord {
	if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return podRecord{}
	}
	podIPs := pod.Status.PodIPs
	if len(podIPs) == 0 {
		podIPs = []corev1.PodIP{{IP: pod.Status.PodIP}}
	}
	var ips []netip.Addr
	for _, ip := range podIPs {
		if addr, ok := normalize(ip.IP); ok {
			ips = append(ips, addr)
		}
	}
	if len(ips) == 0 {
		return podRecord{}
	}
	slices.SortFunc(ips, netip.Addr.Compare)
	return podRecord{node: pod.Spec.NodeName, ips: slices.Compact(ips)}
}

func normalize(ip string) (netip.Addr, bool) {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.WithZone("").Unmap(), true
}

// trim keeps the fields the index reads plus ResourceVersion, by which the informer tells updates from relists.
func trim(obj any) (any, error) {
	if pod, ok := obj.(*corev1.Pod); ok {
		*pod = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, ResourceVersion: pod.ResourceVersion},
			Spec:       corev1.PodSpec{NodeName: pod.Spec.NodeName},
			Status:     corev1.PodStatus{Phase: pod.Status.Phase, PodIP: pod.Status.PodIP, PodIPs: pod.Status.PodIPs},
		}
	}
	return obj, nil
}
