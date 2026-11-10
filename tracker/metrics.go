package tracker

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// trackerMetrics counts tracker events in a registry of its own.
type trackerMetrics struct {
	reg              *prometheus.Registry
	announces, seeds *prometheus.CounterVec
	lookups          prometheus.Counter
}

func newTrackerMetrics() *trackerMetrics {
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
	lookups := prometheus.NewCounter(prometheus.CounterOpts{Name: "xetpd_tracker_lookups_total", Help: "Lookups answered."})
	reg.MustRegister(lookups)
	return &trackerMetrics{
		reg:       reg,
		announces: counters("xetpd_tracker_announces_total", "Peer announces received; rejected ones failed authorization or validation.", "result", "ok", "resync", "rejected"),
		seeds:     counters("xetpd_tracker_seed_requests_total", "Seed pull requests sent to peers; ok means the peer answered 202 Accepted.", "result", "ok", "error"),
		lookups:   lookups,
	}
}

func (m *trackerMetrics) announce(result string) {
	m.announces.WithLabelValues(result).Inc()
}

func (m *trackerMetrics) lookup() {
	m.lookups.Inc()
}

func (m *trackerMetrics) seedRequest(ok bool) {
	if ok {
		m.seeds.WithLabelValues("ok").Inc()
	} else {
		m.seeds.WithLabelValues("error").Inc()
	}
}

func (m *trackerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Descriptors carry no values, so the registries of all trackers share them.
var (
	descSeedsInFlight = prometheus.NewDesc("xetpd_tracker_seed_requests_in_flight", "Seed pull requests in flight.", nil, nil)
	descPeers         = prometheus.NewDesc("xetpd_tracker_peers", "Live peers.", nil, nil)
	descUpstreamPeers = prometheus.NewDesc("xetpd_tracker_upstream_peers", "Live peers that reach the external hub.", nil, nil)
	descXorbs         = prometheus.NewDesc("xetpd_tracker_xorbs", "Distinct xorbs with at least one live holder.", nil, nil)
	descShards        = prometheus.NewDesc("xetpd_tracker_shards", "Distinct shards with at least one live holder.", nil, nil)
)

// trackerState reports at scrape time what the tracker tracks anyway.
type trackerState struct{ tr *Tracker }

func (s trackerState) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

// Collect holds no lock across a send.
func (s trackerState) Collect(ch chan<- prometheus.Metric) {
	peers, xorbs, shards := s.tr.reg.stats()
	upstreams := len(s.tr.reg.upstreams())
	gauge := func(d *prometheus.Desc, v int) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, float64(v))
	}
	gauge(descSeedsInFlight, len(s.tr.seeder.sem))
	gauge(descPeers, peers)
	gauge(descUpstreamPeers, upstreams)
	gauge(descXorbs, xorbs)
	gauge(descShards, shards)
}
