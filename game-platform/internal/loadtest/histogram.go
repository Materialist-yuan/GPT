package loadtest

import (
	"sort"
	"sync/atomic"
	"time"
)

// histogram retains a fixed amount of memory. Percentiles are rounded up to
// logarithmic bucket boundaries (at most 10% or one microsecond).
type histogram struct {
	bounds [256]int64
	counts [256]atomic.Int64
	count  atomic.Int64
	sum    atomic.Int64
	max    atomic.Int64
}

func newHistogram() *histogram {
	h := &histogram{}
	h.bounds[0] = 1
	for i := 1; i < len(h.bounds); i++ {
		next := (h.bounds[i-1]*110 + 99) / 100
		if next <= h.bounds[i-1] {
			next = h.bounds[i-1] + 1
		}
		h.bounds[i] = next
	}
	return h
}

func (h *histogram) record(d time.Duration) {
	us := (d.Nanoseconds() + 999) / 1000
	if us < 1 {
		us = 1
	}
	i := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] >= us })
	if i == len(h.bounds) {
		i--
	}
	h.counts[i].Add(1)
	h.sum.Add(d.Nanoseconds())
	h.count.Add(1)
	for old := h.max.Load(); d.Nanoseconds() > old; old = h.max.Load() {
		if h.max.CompareAndSwap(old, d.Nanoseconds()) {
			break
		}
	}
}

type Latency struct {
	Samples int64   `json:"samples"`
	MeanMS  float64 `json:"mean_ms"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}

func (h *histogram) snapshot() Latency {
	n := h.count.Load()
	r := Latency{Samples: n, MaxMS: float64(h.max.Load()) / 1e6}
	if n == 0 {
		return r
	}
	r.MeanMS = float64(h.sum.Load()) / float64(n) / 1e6
	percentile := func(p int64) float64 {
		target := (n*p + 99) / 100
		var total int64
		for i := range h.counts {
			total += h.counts[i].Load()
			if total >= target {
				return float64(h.bounds[i]) / 1000
			}
		}
		return r.MaxMS
	}
	r.P50MS, r.P95MS, r.P99MS = percentile(50), percentile(95), percentile(99)
	return r
}
