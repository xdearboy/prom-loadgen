package gen

import (
	"fmt"
	"math"
	"sort"

	"github.com/prometheus/prometheus/prompb"
)

type Label = prompb.Label

const (
	namespaces       = 100
	podsPerNamespace = 250
	instancesPerPod  = 3
	slots            = namespaces * podsPerNamespace * instancesPerPod
)

var metricNames = []string{
	"bench_cpu_cores",
	"bench_memory_bytes",
	"bench_http_requests_total",
	"bench_http_request_duration_seconds_sum",
	"bench_http_request_duration_seconds_count",
	"bench_queue_length",
	"bench_container_ready",
	"bench_restarts_total",
}

// past this the label sets repeat
var MaxSeries = len(metricNames) * slots

var counterMetrics = map[string]bool{
	"bench_http_requests_total":                 true,
	"bench_http_request_duration_seconds_sum":   true,
	"bench_http_request_duration_seconds_count": true,
	"bench_restarts_total":                      true,
}

var containerMetrics = map[string]bool{
	"bench_container_ready": true,
	"bench_queue_length":    true,
}

var endpointMetrics = map[string]bool{
	"bench_cpu_cores":    true,
	"bench_memory_bytes": true,
}

var containers = []string{"app", "sidecar", "istio-proxy"}
var statuses = []string{"true", "false", "unknown"}

type Series struct {
	Index  int
	Labels []Label
	Gauge  bool
	Base   float64
	Amp    float64
	Rate   float64
}

func Build(series int, seed uint64) ([]Series, error) {
	if series < 0 || series > MaxSeries {
		return nil, fmt.Errorf("series %d out of range 0..%d", series, MaxSeries)
	}
	shift := int(seed % slots)
	out := make([]Series, 0, series)
	for i := 0; i < series; i++ {
		slot := (i/len(metricNames) + shift) % slots
		out = append(out, newSeries(i, metricNames[i%len(metricNames)], slot, seed))
	}
	return out, nil
}

func newSeries(i int, name string, slot int, seed uint64) Series {
	h := splitmix64(uint64(i) ^ seed)
	base := 0.05 + unitFloat(h)*4
	return Series{
		Index:  i,
		Labels: labelsFor(name, slot),
		Gauge:  !counterMetrics[name],
		Base:   base,
		Amp:    base * 0.5,
		Rate:   0.5 + unitFloat(h^0x9E3779B9)*40,
	}
}

func labelsFor(name string, slot int) []Label {
	namespace := slot % namespaces
	pod := (slot / namespaces) % podsPerNamespace
	instance := slot / (namespaces * podsPerNamespace)
	container := containers[slot%len(containers)]
	labels := []Label{
		{Name: "__name__", Value: name},
		{Name: "job", Value: "bench"},
		{Name: "namespace", Value: fmt.Sprintf("ns-%02d", namespace)},
		{Name: "pod", Value: fmt.Sprintf("app-%04d-%s", pod, container)},
		{Name: "instance", Value: fmt.Sprintf("10.42.%d.%d:8080", namespace%256, instance)},
	}
	if containerMetrics[name] {
		labels = append(labels,
			Label{Name: "container", Value: container},
			Label{Name: "status", Value: statuses[slot%len(statuses)]},
		)
	}
	if endpointMetrics[name] {
		labels = append(labels, Label{Name: "endpoint", Value: "https-metrics"})
	}
	sort.Slice(labels, func(a, b int) bool { return labels[a].Name < labels[b].Name })
	return labels
}

// pure in (series, ts) so every engine gets the same value for a sample
func (s Series) ValueAt(tsMs int64) float64 {
	if !s.Gauge {
		return s.Base + s.Rate*float64(tsMs)/1000
	}
	idx := uint64(s.Index) + 1
	noise := 2*unitFloat(splitmix64(idx^uint64(tsMs)*0x51ED2701)) - 1
	drift := 2*unitFloat(splitmix64(idx^(uint64(tsMs/300_000)*0x9E3779B97F4A7C15))) - 1
	wave := math.Sin(float64(tsMs)/600_000 + float64(idx%251))
	return s.Base + s.Amp*(0.6*noise+0.25*drift+0.3*wave)
}

func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

func unitFloat(x uint64) float64 {
	return float64(x>>11) / float64(uint64(1)<<53)
}
