package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/xdearboy/prom-loadgen/gen"
	"github.com/xdearboy/prom-loadgen/prom"
	"github.com/xdearboy/prom-loadgen/results"
	"github.com/xdearboy/prom-loadgen/rw"
)

func main() {
	var (
		target      = flag.String("target", "", "engine base url, for example http://prompp-0815:9090")
		engineName  = flag.String("engine", "", "engine name from the registry")
		runID       = flag.String("run-id", "", "run id recorded in the artifact")
		out         = flag.String("out", "", "path for the ingest result json")
		ramp        = flag.String("ramp", "", "steps as series:seconds, for example 50000:300,200000:480")
		seed        = flag.Uint64("seed", 20261005, "series generation seed")
		interval    = flag.Duration("interval", 15*time.Second, "sample interval per series")
		batchSeries = flag.Int("batch-series", 5000, "series per remote write request")
		workers     = flag.Int("workers", 4, "concurrent remote write requests")
		epochMs     = flag.Int64("epoch-ms", 0, "timestamp of the first sample, pinned for byte identical streams")
		timeout     = flag.Duration("http-timeout", time.Minute, "remote write request timeout")
	)
	flag.Parse()

	if *target == "" || *engineName == "" || *out == "" {
		log.Fatal("-target, -engine and -out are required")
	}
	steps, err := parseRamp(*ramp)
	if err != nil {
		log.Fatal(err)
	}
	series, err := gen.Build(maxSeries(steps), *seed)
	if err != nil {
		log.Fatal(err)
	}
	logf("run %s engine %s ramp %s interval %s seed %d", *runID, *engineName, *ramp, *interval, *seed)

	api := prom.New(*target, *timeout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res := results.IngestResult{
		RunID:            *runID,
		Engine:           *engineName,
		Target:           *target,
		IntervalSeconds:  interval.Seconds(),
		SeriesPlanned:    len(series),
		BatchSeries:      *batchSeries,
		Workers:          *workers,
		EpochMs:          *epochMs,
		TimestampsPinned: true,
		StartedAt:        time.Now().UTC(),
	}
	var runErr error
	res.Steps, runErr = rw.Run(ctx, rw.Config{
		Target:      *target + "/api/v1/write",
		Series:      series,
		Ramp:        steps,
		Interval:    *interval,
		BatchSeries: *batchSeries,
		Workers:     *workers,
		EpochMs:     *epochMs,
		Client:      &http.Client{Timeout: *timeout},
		OnStepEnd:   func(step results.IngestStep) results.HeadStats { return readHead(api, step) },
	})
	res.FinishedAt = time.Now().UTC()
	if len(res.Steps) > 0 {
		if err := results.WriteJSON(*out, res); err != nil {
			log.Fatalf("write result: %v", err)
		}
		printSummary(res)
	}
	switch {
	case runErr != nil:
		log.Fatalf("ingest: %v", runErr)
	case res.FailedSamples() > 0:
		log.Fatalf("ingest lost %d samples", res.FailedSamples())
	}
}

func readHead(api *prom.Client, step results.IngestStep) results.HeadStats {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status, err := api.TSDBStatus(ctx)
	if err != nil {
		return results.HeadStats{Error: err.Error()}
	}
	h := status.HeadStats
	return results.HeadStats{
		NumSeries:   h.NumSeries,
		NumChunks:   h.NumChunks,
		NumLabelSet: h.NumLabelSet,
		MinTimeMs:   h.MinTimeMs,
		MaxTimeMs:   h.MaxTimeMs,
	}
}

func parseRamp(spec string) ([]rw.Step, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("-ramp is required")
	}
	var steps []rw.Step
	for _, part := range strings.Split(spec, ",") {
		count, seconds, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("ramp step %q: want series:seconds", part)
		}
		n, err := strconv.Atoi(count)
		if err != nil {
			return nil, fmt.Errorf("ramp step %q: %w", part, err)
		}
		d, err := time.ParseDuration(seconds + "s")
		if err != nil {
			return nil, fmt.Errorf("ramp step %q: %w", part, err)
		}
		steps = append(steps, rw.Step{Series: n, Duration: d})
	}
	return steps, nil
}

func maxSeries(steps []rw.Step) int {
	n := 0
	for _, s := range steps {
		n = max(n, s.Series)
	}
	return n
}

func printSummary(res results.IngestResult) {
	for _, s := range res.Steps {
		fmt.Printf("series=%-7d samples/s=%-7.0f elapsed=%-6.0fs p50=%-6.1fms p99=%-6.1fms ok=%d 4xx=%d 5xx=%d client=%d head=%d\n",
			s.Series, s.AchievedSamplesPerSec, s.ElapsedSeconds, s.RequestLatency.P50MS, s.RequestLatency.P99MS,
			s.RequestsOK, s.HTTP4xx, s.HTTP5xx, s.ClientErrors, s.Head.NumSeries)
		for _, e := range s.ErrorSamples {
			logf("  error: %s", e)
		}
		if s.Overran() {
			logf("  step %d overran: %.0fs for a %.0fs step, the offered rate was not sustained", s.Index, s.ElapsedSeconds, s.DurationSeconds)
		}
	}
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}
