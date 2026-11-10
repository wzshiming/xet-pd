package peer

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// peerMetrics counts peer events in a registry of its own.
type peerMetrics struct {
	reg                               *prometheus.Registry
	gets, pulls, announces, shardGets *prometheus.CounterVec
	restoreDropped                    prometheus.Counter
}

func newPeerMetrics() *peerMetrics {
	m := &peerMetrics{
		reg:            prometheus.NewRegistry(),
		gets:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xetpd_peer_xorb_gets_total", Help: "Xorb GETs answered by status; requester is peer when the request carries the peer header, else client."}, []string{"requester", "status"}),
		pulls:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xetpd_peer_pulls_total", Help: "Xorb pulls from other peers; timeout means the pull timeout ran out, queueing included."}, []string{"result"}),
		announces:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xetpd_peer_announces_total", Help: "Announces sent to the tracker; resync means the tracker asked for the whole inventory."}, []string{"result"}),
		restoreDropped: prometheus.NewCounter(prometheus.CounterOpts{Name: "xetpd_peer_restore_dropped_total", Help: "Stored xorbs deleted at startup for failing validation."}),
		shardGets:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xetpd_peer_shard_gets_total", Help: "Shard GETs answered, by status."}, []string{"status"}),
	}
	// Creating every fixed label value up front exports it at zero before its first count; other statuses appear once seen.
	for _, requester := range []string{"client", "peer"} {
		for _, status := range []string{"200", "206", "404"} {
			m.gets.WithLabelValues(requester, status)
		}
	}
	for _, v := range []string{"ok", "not_found", "canceled", "timeout", "error"} {
		m.pulls.WithLabelValues(v)
	}
	for _, v := range []string{"ok", "resync", "error"} {
		m.announces.WithLabelValues(v)
	}
	for _, v := range []string{"200", "404"} {
		m.shardGets.WithLabelValues(v)
	}
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m.gets, m.pulls, m.announces, m.shardGets, m.restoreDropped)
	return m
}

func (m *peerMetrics) get(requester string, status int) {
	m.gets.WithLabelValues(requester, strconv.Itoa(status)).Inc()
}

func (m *peerMetrics) shardGet(status int) {
	m.shardGets.WithLabelValues(strconv.Itoa(status)).Inc()
}

func (m *peerMetrics) pull(result string) {
	m.pulls.WithLabelValues(result).Inc()
}

func (m *peerMetrics) announce(result string) {
	m.announces.WithLabelValues(result).Inc()
}

func (m *peerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Descriptors carry no values, so the registries of all peers share them.
var (
	descShards        = prometheus.NewDesc("xetpd_peer_shards", "Known shards: files whose shard and xorb offsets this peer holds, as announces list them.", nil, nil)
	descCacheBytes    = prometheus.NewDesc("xetpd_peer_xorb_cache_bytes", "Bytes of committed xorbs.", nil, nil)
	descCacheEntries  = prometheus.NewDesc("xetpd_peer_xorb_cache_entries", "Committed xorbs.", nil, nil)
	descCacheLimit    = prometheus.NewDesc("xetpd_peer_xorb_cache_limit_bytes", "Cap on committed xorb bytes, 0 for unlimited.", nil, nil)
	descPullsInFlight = prometheus.NewDesc("xetpd_peer_pulls_in_flight", "Pulls in progress, queued ones included.", nil, nil)
	descCooldowns     = prometheus.NewDesc("xetpd_peer_cooldowns", "Xorbs whose last pull failed and whose cooldown has not ended.", nil, nil)
)

// peerState reports at scrape time what the peer tracks anyway.
type peerState struct{ p *Peer }

func (s peerState) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

func (s peerState) Collect(ch chan<- prometheus.Metric) {
	stats := s.p.stats()
	gauge := func(d *prometheus.Desc, v float64) { ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v) }
	gauge(descShards, float64(stats.shards))
	gauge(descCacheBytes, float64(stats.bytes))
	gauge(descCacheEntries, float64(stats.entries))
	gauge(descCacheLimit, float64(s.p.cfg.XorbCacheSize))
	gauge(descPullsInFlight, float64(stats.pulls))
	gauge(descCooldowns, float64(stats.cooldowns))
}
