package gen

import (
	"math"
	"strings"
	"testing"
)

func build(t *testing.T, series int, seed uint64) []Series {
	t.Helper()
	out, err := Build(series, seed)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func fingerprint(s Series) string {
	parts := make([]string, 0, len(s.Labels))
	for _, l := range s.Labels {
		parts = append(parts, l.Name+"="+l.Value)
	}
	return strings.Join(parts, ",")
}

func name(s Series) string {
	for _, l := range s.Labels {
		if l.Name == "__name__" {
			return l.Value
		}
	}
	return ""
}

func TestBuildIsDeterministic(t *testing.T) {
	a, b := build(t, 500, 42), build(t, 500, 42)
	for i := range a {
		if fingerprint(a[i]) != fingerprint(b[i]) || a[i].ValueAt(1_700_000_000_000) != b[i].ValueAt(1_700_000_000_000) {
			t.Fatalf("series %d differs between two builds", i)
		}
	}
}

func TestSmallerBuildIsAPrefix(t *testing.T) {
	small, large := build(t, 1000, 7), build(t, 50000, 7)
	for i := range small {
		if fingerprint(small[i]) != fingerprint(large[i]) {
			t.Fatalf("series %d depends on the total count", i)
		}
	}
}

func TestSeedChangesSeries(t *testing.T) {
	a, b := build(t, 200, 1), build(t, 200, 2)
	for i := range a {
		if fingerprint(a[i]) != fingerprint(b[i]) {
			return
		}
	}
	t.Fatal("seed does not affect series identity")
}

func TestEveryLabelSetIsUniqueUpToMaxSeries(t *testing.T) {
	seen := make(map[string]int, MaxSeries)
	for _, s := range build(t, MaxSeries, 7) {
		fp := fingerprint(s)
		if prev, ok := seen[fp]; ok {
			t.Fatalf("duplicate label set at %d and %d: %s", prev, s.Index, fp)
		}
		seen[fp] = s.Index
	}
}

func TestBuildRejectsMoreThanMaxSeries(t *testing.T) {
	if _, err := Build(MaxSeries+1, 1); err == nil {
		t.Fatal("building past MaxSeries must fail, it would repeat label sets")
	}
}

func TestLabelsSortedAndNamed(t *testing.T) {
	s := build(t, 300, 1)[2]
	for i := 1; i < len(s.Labels); i++ {
		if s.Labels[i-1].Name >= s.Labels[i].Name {
			t.Fatalf("labels not sorted: %v", s.Labels)
		}
	}
	if s.Labels[0].Name != "__name__" {
		t.Fatalf("unexpected name label: %v", s.Labels)
	}
}

func TestEveryMetricIsPresent(t *testing.T) {
	names := map[string]int{}
	for _, s := range build(t, 300, 1) {
		names[name(s)]++
	}
	if len(names) != len(metricNames) {
		t.Fatalf("got %d metric names, want %d", len(names), len(metricNames))
	}
}

func TestCountersAreMonotonic(t *testing.T) {
	counters, gauges := 0, 0
	for _, s := range build(t, 5000, 3) {
		if s.Gauge {
			gauges++
			continue
		}
		counters++
		prev := s.ValueAt(1_700_000_000_000)
		for ts := int64(1_700_000_015_000); ts <= 1_700_300_000_000; ts += 15_000 {
			cur := s.ValueAt(ts)
			if cur < prev {
				t.Fatalf("counter %s went backwards at %d: %v -> %v", fingerprint(s), ts, prev, cur)
			}
			prev = cur
		}
	}
	if counters == 0 || gauges == 0 {
		t.Fatalf("unexpected mix: %d counters, %d gauges", counters, gauges)
	}
}

func TestSummarySumIsACounter(t *testing.T) {
	for _, s := range build(t, 16, 1) {
		if name(s) == "bench_http_request_duration_seconds_sum" && s.Gauge {
			t.Fatal("a _sum series must be a counter, rate() over a gauge sees resets")
		}
	}
}

func TestGaugesStayInBand(t *testing.T) {
	for _, s := range build(t, 5000, 5) {
		if !s.Gauge {
			continue
		}
		for _, ts := range []int64{1_700_000_000_000, 1_700_060_000_000, 1_700_600_000_000} {
			v := s.ValueAt(ts)
			if v < s.Base-s.Amp*1.6 || v > s.Base+s.Amp*1.6 {
				t.Fatalf("gauge %s out of band: %v base %v amp %v", fingerprint(s), v, s.Base, s.Amp)
			}
		}
	}
}

func TestGaugeNoiseChangesBetweenSamples(t *testing.T) {
	s := build(t, 1, 9)[0]
	if !s.Gauge {
		t.Fatal("series 0 is expected to be a gauge")
	}
	jumps := 0
	for k := int64(0); k < 20; k++ {
		ts := 1_700_000_000_000 + k*15_000
		if d := math.Abs(s.ValueAt(ts+15_000) - s.ValueAt(ts)); d > 0.05*s.Amp {
			jumps++
		}
	}
	if jumps < 10 {
		t.Fatalf("only %d of 20 consecutive samples moved, noise is not per sample", jumps)
	}
}

func TestBuildRespectsSeriesCount(t *testing.T) {
	for _, n := range []int{0, 1, 10, 1000, 100000} {
		if got := len(build(t, n, 1)); got != n {
			t.Fatalf("Build(%d) returned %d series", n, got)
		}
	}
}
