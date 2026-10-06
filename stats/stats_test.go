package stats

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestQuantileNearestRank(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := map[float64]float64{
		0:    1,
		0.1:  1,
		0.5:  5,
		0.9:  9,
		0.95: 10,
		1:    10,
	}
	for q, want := range cases {
		if got := Quantile(values, q); got != want {
			t.Errorf("Quantile(%v) = %v, want %v", q, got, want)
		}
	}
}

func TestQuantileDoesNotMutateInput(t *testing.T) {
	values := []float64{3, 1, 2}
	Quantile(values, 0.5)
	if values[0] != 3 || values[1] != 1 || values[2] != 2 {
		t.Fatalf("input mutated: %v", values)
	}
}

func TestQuantileEmpty(t *testing.T) {
	if got := Quantile(nil, 0.99); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

func TestRecorderSummary(t *testing.T) {
	r := NewRecorder(4)
	for i := 1; i <= 100; i++ {
		r.Observe(time.Duration(i) * time.Millisecond)
	}
	s := r.Summary()
	if s.Count != 100 {
		t.Fatalf("count = %d, want 100", s.Count)
	}
	if s.MinMS != 1 || s.MaxMS != 100 {
		t.Fatalf("min/max = %v/%v, want 1/100", s.MinMS, s.MaxMS)
	}
	if math.Abs(s.MeanMS-50.5) > 1e-9 {
		t.Fatalf("mean = %v, want 50.5", s.MeanMS)
	}
	if s.P50MS != 50 || s.P95MS != 95 || s.P99MS != 99 {
		t.Fatalf("percentiles = %v/%v/%v", s.P50MS, s.P95MS, s.P99MS)
	}
}

func TestRecorderEmptySummary(t *testing.T) {
	if s := NewRecorder(1).Summary(); s.Count != 0 || s.P99MS != 0 {
		t.Fatalf("got %+v, want zero summary", s)
	}
}

func TestRecorderObserveN(t *testing.T) {
	r := NewRecorder(4)
	r.ObserveN(10*time.Millisecond, 5)
	r.ObserveN(20*time.Millisecond, 5)
	s := r.Summary()
	if s.Count != 10 {
		t.Fatalf("count = %d, want 10", s.Count)
	}
	if math.Abs(s.MeanMS-15) > 1e-9 {
		t.Fatalf("mean = %v, want 15", s.MeanMS)
	}
	if r.Count() != 10 {
		t.Fatalf("Count() = %d, want 10", r.Count())
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder(16)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				r.Observe(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if got := r.Count(); got != 8000 {
		t.Fatalf("count = %d, want 8000", got)
	}
}
