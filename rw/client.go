package rw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/xdearboy/prom-loadgen/gen"
	"github.com/xdearboy/prom-loadgen/results"
	"github.com/xdearboy/prom-loadgen/stats"
)

type Step struct {
	Series   int
	Duration time.Duration
}

type Config struct {
	Target      string
	Series      []gen.Series
	Ramp        []Step
	Interval    time.Duration
	BatchSeries int
	Workers     int
	EpochMs     int64
	Client      *http.Client
	OnStepEnd   func(results.IngestStep) results.HeadStats
}

func (c Config) validate() error {
	switch {
	case c.Target == "" || c.Client == nil:
		return errors.New("target and client are required")
	case c.Interval <= 0 || c.BatchSeries <= 0 || c.Workers <= 0:
		return errors.New("interval, batch series and workers must be positive")
	case c.EpochMs <= 0:
		return errors.New("epoch must be pinned")
	case len(c.Ramp) == 0:
		return errors.New("ramp is empty")
	}
	for i, s := range c.Ramp {
		if s.Series <= 0 || s.Series > len(c.Series) {
			return fmt.Errorf("step %d: series %d out of range 1..%d", i, s.Series, len(c.Series))
		}
		if s.Duration < c.Interval {
			return fmt.Errorf("step %d: duration %s is shorter than the interval", i, s.Duration)
		}
	}
	return nil
}

func Run(ctx context.Context, cfg Config) ([]results.IngestStep, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var out []results.IngestStep
	epoch := cfg.EpochMs
	for i, step := range cfg.Ramp {
		res := runStep(ctx, cfg, step, epoch)
		res.Index = i
		epoch += step.Duration.Milliseconds()
		if cfg.OnStepEnd != nil {
			res.Head = cfg.OnStepEnd(res)
		}
		out = append(out, res)
		if err := ctx.Err(); err != nil {
			return out, err
		}
	}
	return out, nil
}

type counters struct {
	mu       sync.Mutex
	step     results.IngestStep
	latency  *stats.Recorder
	maxNotes int
}

func (c *counters) record(series int, wire, compressed int, elapsed time.Duration, status int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &c.step
	s.Requests++
	s.SamplesSent += int64(series)
	s.BytesWire += int64(wire)
	s.BytesSnappy += int64(compressed)
	note := ""
	switch {
	case err != nil:
		s.ClientErrors++
		note = err.Error()
	case status >= 200 && status < 300:
		s.RequestsOK++
		c.latency.Observe(elapsed)
		return
	case status >= 500:
		s.HTTP5xx++
		note = fmt.Sprintf("http %d", status)
	default:
		s.HTTP4xx++
		note = fmt.Sprintf("http %d rejected", status)
	}
	s.SamplesFailed += int64(series)
	if len(s.ErrorSamples) < c.maxNotes {
		s.ErrorSamples = append(s.ErrorSamples, note)
	}
}

func runStep(ctx context.Context, cfg Config, step Step, epoch int64) results.IngestStep {
	series := cfg.Series[:step.Series]
	batches := split(series, cfg.BatchSeries)
	ticks := int64(step.Duration / cfg.Interval)
	c := &counters{latency: stats.NewRecorder(int(ticks) * len(batches)), maxNotes: 20}
	sem := make(chan struct{}, cfg.Workers)
	var wg sync.WaitGroup

	start := time.Now()
send:
	for k := int64(0); k < ticks; k++ {
		ts := epoch + k*cfg.Interval.Milliseconds()
		for _, batch := range batches {
			select {
			case <-ctx.Done():
				break send
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(batch []gen.Series) {
				defer wg.Done()
				defer func() { <-sem }()
				began := time.Now()
				wire, compressed, status, err := send(ctx, cfg, batch, ts)
				c.record(len(batch), wire, compressed, time.Since(began), status, err)
			}(batch)
		}
		// fixed offsets from start, a slow round trip must not shift later ticks
		sleepUntil(ctx, start.Add(time.Duration(k+1)*cfg.Interval))
	}
	wg.Wait()
	finished := time.Now()

	s := c.step
	s.Series = step.Series
	s.DurationSeconds = step.Duration.Seconds()
	s.IntervalSeconds = cfg.Interval.Seconds()
	s.StartedAt = start.UTC()
	s.FinishedAt = finished.UTC()
	s.ElapsedSeconds = finished.Sub(start).Seconds()
	s.Ticks = ticks
	s.RequestLatency = c.latency.Summary()
	s.AchievedSamplesPerSec = float64(s.SamplesSent-s.SamplesFailed) / s.ElapsedSeconds
	s.AchievedRequestsPerSec = float64(s.RequestsOK) / s.ElapsedSeconds
	return s
}

func send(ctx context.Context, cfg Config, batch []gen.Series, tsMs int64) (wire, compressed, status int, err error) {
	req := prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, len(batch))}
	for i, s := range batch {
		req.Timeseries[i] = prompb.TimeSeries{
			Labels:  s.Labels,
			Samples: []prompb.Sample{{Value: s.ValueAt(tsMs), Timestamp: tsMs}},
		}
	}
	body, err := req.Marshal()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("marshal write request: %w", err)
	}
	payload := snappy.Encode(nil, body)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Target, bytes.NewReader(payload))
	if err != nil {
		return len(body), len(payload), 0, err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := cfg.Client.Do(httpReq)
	if err != nil {
		return len(body), len(payload), 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return len(body), len(payload), resp.StatusCode, nil
}

func sleepUntil(ctx context.Context, t time.Time) {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func split(series []gen.Series, size int) [][]gen.Series {
	var out [][]gen.Series
	for len(series) > size {
		out = append(out, series[:size])
		series = series[size:]
	}
	return append(out, series)
}
