package stats

import (
	"math"
	"sort"
	"sync"
	"time"
)

type Summary struct {
	Count  int64   `json:"count"`
	MinMS  float64 `json:"min_ms"`
	P50MS  float64 `json:"p50_ms"`
	P90MS  float64 `json:"p90_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
	MeanMS float64 `json:"mean_ms"`
}

type Recorder struct {
	mu      sync.Mutex
	samples []float64
	sum     float64
	count   int64
}

func NewRecorder(capacity int) *Recorder {
	return &Recorder{samples: make([]float64, 0, capacity)}
}

func (r *Recorder) Observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	r.mu.Lock()
	r.samples = append(r.samples, ms)
	r.sum += ms
	r.count++
	r.mu.Unlock()
}

func (r *Recorder) Count() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	sorted := make([]float64, len(r.samples))
	copy(sorted, r.samples)
	sum, count := r.sum, r.count
	r.mu.Unlock()

	if count == 0 {
		return Summary{}
	}
	sort.Float64s(sorted)
	return Summary{
		Count:  count,
		MinMS:  sorted[0],
		P50MS:  quantileSorted(sorted, 0.50),
		P90MS:  quantileSorted(sorted, 0.90),
		P95MS:  quantileSorted(sorted, 0.95),
		P99MS:  quantileSorted(sorted, 0.99),
		MaxMS:  sorted[len(sorted)-1],
		MeanMS: sum / float64(count),
	}
}

func quantileSorted(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[clamp(rank, 0, len(sorted)-1)]
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
