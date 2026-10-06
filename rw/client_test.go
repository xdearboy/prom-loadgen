package rw

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/xdearboy/prom-loadgen/gen"
	"github.com/xdearboy/prom-loadgen/results"
)

const epoch = int64(1_767_225_600_000)

type receiver struct {
	mu      sync.Mutex
	samples map[int64]int
	status  int
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("Content-Encoding") != "snappy" {
		http.Error(w, "missing snappy encoding", http.StatusUnsupportedMediaType)
		return
	}
	compressed, _ := io.ReadAll(req.Body)
	body, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var wr prompb.WriteRequest
	if err := wr.Unmarshal(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	for _, ts := range wr.Timeseries {
		for _, s := range ts.Samples {
			r.samples[s.Timestamp]++
		}
	}
	status := r.status
	r.mu.Unlock()
	w.WriteHeader(status)
}

func run(t *testing.T, status int, ramp []Step) ([]results.IngestStep, *receiver) {
	t.Helper()
	recv := &receiver{samples: map[int64]int{}, status: status}
	srv := httptest.NewServer(recv)
	defer srv.Close()
	series, err := gen.Build(30, 1)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := Run(context.Background(), Config{
		Target:      srv.URL,
		Series:      series,
		Ramp:        ramp,
		Interval:    50 * time.Millisecond,
		BatchSeries: 7,
		Workers:     3,
		EpochMs:     epoch,
		Client:      srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return steps, recv
}

func TestRunSendsEveryStepWithContiguousPinnedTimestamps(t *testing.T) {
	steps, recv := run(t, http.StatusNoContent, []Step{{10, 150 * time.Millisecond}, {30, 100 * time.Millisecond}})

	want := []struct{ ticks, requests, sent int64 }{{3, 6, 30}, {2, 10, 60}}
	for i, w := range want {
		s := steps[i]
		if s.Ticks != w.ticks || s.Requests != w.requests || s.RequestsOK != w.requests || s.SamplesSent != w.sent || s.SamplesFailed != 0 {
			t.Fatalf("step %d sent %d requests and %d samples over %d ticks, want %+v", i, s.Requests, s.SamplesSent, s.Ticks, w)
		}
		if s.ElapsedSeconds <= 0 || !s.FinishedAt.After(s.StartedAt) {
			t.Fatalf("step %d has no measured window", i)
		}
	}
	if steps[1].StartedAt.Before(steps[0].FinishedAt) {
		t.Fatal("steps overlap in wall time")
	}

	wantTS := map[int64]int{epoch: 10, epoch + 50: 10, epoch + 100: 10, epoch + 150: 30, epoch + 200: 30}
	if len(recv.samples) != len(wantTS) {
		t.Fatalf("got timestamps %v, want %v", recv.samples, wantTS)
	}
	for ts, n := range wantTS {
		if recv.samples[ts] != n {
			t.Fatalf("timestamp %d carried %d samples, want %d (all %v)", ts, recv.samples[ts], n, recv.samples)
		}
	}
}

func TestRunCountsRejectedRequestsAndLostSamples(t *testing.T) {
	steps, _ := run(t, http.StatusServiceUnavailable, []Step{{10, 100 * time.Millisecond}})
	s := steps[0]
	if s.Requests != 4 || s.HTTP5xx != 4 || s.RequestsOK != 0 || s.SamplesSent != 20 || s.SamplesFailed != 20 {
		t.Fatalf("got %d requests, %d 5xx, %d ok, %d failed samples; want 4 requests failing with 5xx and 20 lost samples",
			s.Requests, s.HTTP5xx, s.RequestsOK, s.SamplesFailed)
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	series, _ := gen.Build(5, 1)
	base := Config{Target: "http://x", Series: series, Interval: time.Second, BatchSeries: 1, Workers: 1, EpochMs: epoch, Client: http.DefaultClient}
	cases := map[string]func(*Config){
		"empty ramp":          func(c *Config) { c.Ramp = nil },
		"too many series":     func(c *Config) { c.Ramp = []Step{{6, time.Minute}} },
		"step below a tick":   func(c *Config) { c.Ramp = []Step{{5, time.Millisecond}} },
		"unpinned epoch":      func(c *Config) { c.Ramp = []Step{{5, time.Minute}}; c.EpochMs = 0 },
		"no workers":          func(c *Config) { c.Ramp = []Step{{5, time.Minute}}; c.Workers = 0 },
		"no target or client": func(c *Config) { c.Ramp = []Step{{5, time.Minute}}; c.Client = nil },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := Run(context.Background(), cfg); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestSplitKeepsEverySeriesOnce(t *testing.T) {
	series, _ := gen.Build(23, 1)
	total := 0
	for _, b := range split(series, 5) {
		if len(b) == 0 || len(b) > 5 {
			t.Fatalf("batch of %d", len(b))
		}
		total += len(b)
	}
	if total != 23 {
		t.Fatalf("batches hold %d series, want 23", total)
	}
}
