package gateway

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// gatewayMetrics counts gateway events in a registry of its own; a nil one counts nothing.
type gatewayMetrics struct {
	reg                      *prometheus.Registry
	picks                    [3]prometheus.Counter // by pickOutcome
	relays, announces, pulls *prometheus.CounterVec
}

func newGatewayMetrics() *gatewayMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	// Creating every label value up front exports it at zero before its first count.
	counters := func(name, help, label string, values ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, []string{label})
		for _, value := range values {
			v.WithLabelValues(value)
		}
		reg.MustRegister(v)
		return v
	}
	picks := counters("xetpd_gateway_locality_picks_total", "Peer choices behind reconstruction URLs: local on the client's node, fallback on another node, none when no peer is live.", "outcome")
	return &gatewayMetrics{
		reg:       reg,
		picks:     [3]prometheus.Counter{pickLocal: picks.WithLabelValues("local"), pickFallback: picks.WithLabelValues("fallback"), pickNone: picks.WithLabelValues("none")},
		relays:    counters("xetpd_gateway_relay_opens_total", "Opens of xorb ranges relayed from live holders.", "result", "ok", "no_holder", "canceled", "error"),
		announces: counters("xetpd_gateway_announces_total", "Peer announces received; rejected ones failed authorization or validation.", "result", "ok", "resync", "rejected"),
		pulls:     counters("xetpd_gateway_pull_requests_total", "Pull requests sent to seeds; ok means the seed answered 202 Accepted.", "result", "ok", "error"),
	}
}

func (m *gatewayMetrics) pick(o pickOutcome) {
	if m != nil {
		m.picks[o].Inc()
	}
}

func (m *gatewayMetrics) relay(result string) {
	if m != nil {
		m.relays.WithLabelValues(result).Inc()
	}
}

func (m *gatewayMetrics) announce(result string) {
	if m != nil {
		m.announces.WithLabelValues(result).Inc()
	}
}

func (m *gatewayMetrics) pullRequest(ok bool) {
	switch {
	case m == nil:
	case ok:
		m.pulls.WithLabelValues("ok").Inc()
	default:
		m.pulls.WithLabelValues("error").Inc()
	}
}

func (m *gatewayMetrics) handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Descriptors carry no values, so the registries of all gateways share them.
var (
	descXorbGets       = prometheus.NewDesc("xetpd_gateway_xorb_gets_total", "Xorb GETs received; requester is peer when the request carries the peer header, else direct.", []string{"requester"}, nil)
	descPeers          = prometheus.NewDesc("xetpd_gateway_peers", "Live peers.", nil, nil)
	descPeerHoldings   = prometheus.NewDesc("xetpd_gateway_peer_holdings", "Xorbs announced as held, summed over live peers.", nil, nil)
	descCacheBytes     = prometheus.NewDesc("xetpd_gateway_xorb_cache_bytes", "Bytes of local xorbs.", nil, nil)
	descCacheEntries   = prometheus.NewDesc("xetpd_gateway_xorb_cache_entries", "Local xorbs.", nil, nil)
	descCacheLimit     = prometheus.NewDesc("xetpd_gateway_xorb_cache_limit_bytes", "Soft cap on local xorb bytes, 0 for unlimited.", nil, nil)
	descPins           = prometheus.NewDesc("xetpd_gateway_xorb_pins", "Xorbs pinned against deletion by in-flight writes and verifications.", nil, nil)
	descWriters        = prometheus.NewDesc("xetpd_gateway_xorb_writers", "Xorbs whose write lock is held or waited for.", nil, nil)
	descPullsInFlight  = prometheus.NewDesc("xetpd_gateway_pull_requests_in_flight", "Pull requests to seeds in flight.", nil, nil)
	descLocalitySynced = prometheus.NewDesc("xetpd_gateway_locality_synced", "1 once the locality index has seen the whole cluster, always 1 without a locality resolver.", nil, nil)
)

// gatewayState reports at scrape time what the gateway tracks anyway.
type gatewayState struct{ g *Gateway }

func (s gatewayState) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

// Collect takes each lock on its own and none across a send.
func (s gatewayState) Collect(ch chan<- prometheus.Metric) {
	g := s.g
	ch <- prometheus.MustNewConstMetric(descXorbGets, prometheus.CounterValue, float64(g.peerGets.Load()), "peer")
	ch <- prometheus.MustNewConstMetric(descXorbGets, prometheus.CounterValue, float64(g.directGets.Load()), "direct")
	peers := g.reg.snapshot()
	holdings := 0
	for _, p := range peers {
		holdings += p.Holdings
	}
	stats := g.staging.stats()
	pinned, writing := g.st.stats()
	synced := 0.0
	if g.localitySynced() {
		synced = 1
	}
	gauge := func(d *prometheus.Desc, v float64) { ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v) }
	gauge(descPeers, float64(len(peers)))
	gauge(descPeerHoldings, float64(holdings))
	gauge(descCacheBytes, float64(stats.bytes))
	gauge(descCacheEntries, float64(stats.entries))
	gauge(descCacheLimit, float64(g.staging.cfg.cacheSize))
	gauge(descPins, float64(pinned))
	gauge(descWriters, float64(writing))
	gauge(descPullsInFlight, float64(stats.inflight))
	gauge(descLocalitySynced, synced)
}
