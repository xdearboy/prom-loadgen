package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/xdearboy/prom-loadgen/prom"
	"github.com/xdearboy/prom-loadgen/queries"
	"github.com/xdearboy/prom-loadgen/results"
	"github.com/xdearboy/prom-loadgen/stats"
)

func main() {
	var (
		target      = flag.String("target", "", "engine base url")
		engineName  = flag.String("engine", "", "engine name")
		runID       = flag.String("run-id", "", "run id")
		suite       = flag.String("suite", "core", "query suite")
		concurrency = flag.Int("concurrency", 4, "parallel requests of the same query")
		perQuery    = flag.Duration("per-query", 15*time.Second, "time budget for each query")
		minRuns     = flag.Int("min-runs", 8, "measured requests per query even when the budget is spent")
		warmup      = flag.Int("warmup", 1, "unmeasured requests per query before measuring")
		mode        = flag.String("mode", "bench", "bench or dump")
		out         = flag.String("out", "", "path for the result json")
		timeout     = flag.Duration("query-timeout", 2*time.Minute, "per query timeout")
		lookback    = flag.Duration("range-offset", 10*time.Minute, "how far behind now a range query ends")
		phase       = flag.String("phase", "query", "phase label")
		atMs        = flag.Int64("at-ms", 0, "pin the query evaluation time for byte identical dumps")
	)
	flag.Parse()

	if *target == "" || *engineName == "" {
		log.Fatal("both -target and -engine are required")
	}
	suiteQueries, err := queries.Get(*suite)
	if err != nil {
		log.Fatal(err)
	}
	if *concurrency < 1 || *minRuns < 1 || *warmup < 0 {
		log.Fatal("need concurrency >= 1, min-runs >= 1 and warmup >= 0")
	}

	r := runner{
		client:   prom.New(*target, *timeout),
		timeout:  *timeout,
		lookback: *lookback,
		atMs:     *atMs,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var res any
	if *mode == "dump" {
		d := r.dump(ctx, suiteQueries)
		d.RunID, d.Engine, d.Target, d.Suite = *runID, *engineName, *target, *suite
		printDump(d)
		res = d
	} else {
		b := r.bench(ctx, suiteQueries, *concurrency, *perQuery, *minRuns, *warmup)
		b.RunID, b.Engine, b.Target, b.Suite, b.Phase = *runID, *engineName, *target, *suite, *phase
		printBench(b)
		res = b
	}
	if *out != "" {
		if err := results.WriteJSON(*out, res); err != nil {
			log.Fatal(err)
		}
	}
}

type runner struct {
	client   *prom.Client
	timeout  time.Duration
	lookback time.Duration
	atMs     int64
}

func (r runner) request(q queries.Query) (string, url.Values, error) {
	end := time.Now().Add(-r.lookback)
	if r.atMs > 0 {
		end = time.UnixMilli(r.atMs).UTC()
	}
	if q.Type == queries.Instant {
		path, params := prom.InstantRequest(q.Expr, end, r.timeout)
		return path, params, nil
	}
	step, err := q.StepDuration()
	if err != nil {
		return "", nil, err
	}
	window, err := q.WindowDuration()
	if err != nil {
		return "", nil, err
	}
	path, params := prom.RangeRequest(q.Expr, end.Add(-window), end, step, r.timeout)
	return path, params, nil
}

func (r runner) bench(ctx context.Context, qs []queries.Query, concurrency int, perQuery time.Duration, minRuns, warmup int) *results.QueryResult {
	res := &results.QueryResult{Concurrency: concurrency, StartedAt: time.Now().UTC()}
	for _, q := range qs {
		if ctx.Err() != nil {
			break
		}
		m := &measurement{query: q, latency: stats.NewRecorder(1024)}
		for i := 0; i < warmup && ctx.Err() == nil; i++ {
			r.once(ctx, &measurement{query: q, latency: stats.NewRecorder(1)})
		}
		loop(ctx, concurrency, perQuery, minRuns, func() { r.once(ctx, m) })
		res.Queries = append(res.Queries, m.snapshot())
	}
	res.FinishedAt = time.Now().UTC()
	res.DurationSec = res.FinishedAt.Sub(res.StartedAt).Seconds()
	return res
}

func loop(ctx context.Context, concurrency int, perQuery time.Duration, minRuns int, run func()) {
	deadline := time.Now().Add(perQuery)
	var (
		mu      sync.Mutex
		started int
		wg      sync.WaitGroup
	)
	claim := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if started >= minRuns && !time.Now().Before(deadline) {
			return false
		}
		started++
		return true
	}
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil && claim() {
				run()
			}
		}()
	}
	wg.Wait()
}

type measurement struct {
	query       queries.Query
	latency     *stats.Recorder
	mu          sync.Mutex
	requests    int64
	errors      int64
	series      int64
	empty       int64
	serverTotal float64
	errorSample string
}

func (m *measurement) record(err error, elapsed time.Duration, series int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests++
	m.serverTotal += elapsed.Seconds()
	if err != nil {
		m.errors++
		if m.errorSample == "" {
			m.errorSample = err.Error()
		}
		return
	}
	m.latency.Observe(elapsed)
	m.series = series
	if series == 0 {
		m.empty++
	}
}

func (m *measurement) snapshot() results.QueryMetric {
	m.mu.Lock()
	defer m.mu.Unlock()
	return results.QueryMetric{
		Name:        m.query.Name,
		Expr:        m.query.Expr,
		Category:    m.query.Category,
		Type:        string(m.query.Type),
		Requests:    m.requests,
		Errors:      m.errors,
		Latency:     m.latency.Summary(),
		Series:      m.series,
		EmptyRuns:   m.empty,
		ServerMSAvg: m.serverTotal / float64(max(m.requests, 1)),
		ErrorSample: m.errorSample,
	}
}

func (r runner) once(ctx context.Context, m *measurement) {
	path, params, err := r.request(m.query)
	if err != nil {
		m.record(err, 0, 0)
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	series, elapsed, err := r.client.Probe(reqCtx, path, params)
	m.record(err, time.Duration(elapsed*float64(time.Second)), series)
}

func (r runner) dump(ctx context.Context, qs []queries.Query) *results.DumpResult {
	res := &results.DumpResult{GeneratedAt: time.Now().UTC()}
	for _, q := range qs {
		entry := results.DumpEntry{Name: q.Name, Expr: q.Expr, Type: string(q.Type)}
		path, params, err := r.request(q)
		if err == nil {
			var matrix prom.Matrix
			reqCtx, cancel := context.WithTimeout(ctx, r.timeout)
			matrix, entry.Elapsed, err = r.client.Fetch(reqCtx, path, params)
			cancel()
			if err == nil {
				sum := sha256.Sum256([]byte(prom.CanonicalSeries(matrix)))
				entry.SHA256 = hex.EncodeToString(sum[:])
				entry.Series = int64(len(matrix.Result))
				for _, s := range matrix.Result {
					entry.Points += int64(len(s.Values))
				}
			}
		}
		if err != nil {
			entry.Error = err.Error()
		}
		res.Entries = append(res.Entries, entry)
	}
	return res
}

func printBench(res *results.QueryResult) {
	fmt.Printf(`{"run_id":%q,"engine":%q,"concurrency":%d,"requests":%d,"errors":%d}`+"\n",
		res.RunID, res.Engine, res.Concurrency, res.TotalRequests(), res.TotalErrors())
	for _, q := range res.Queries {
		fmt.Printf("  %-26s %-9s reqs=%-6d p50=%-8.1f p95=%-8.1f p99=%-8.1f err=%d series=%d\n",
			q.Name, q.Type, q.Requests, q.Latency.P50MS, q.Latency.P95MS, q.Latency.P99MS, q.Errors, q.Series)
	}
}

func printDump(res *results.DumpResult) {
	fmt.Printf(`{"run_id":%q,"engine":%q,"entries":%d}`+"\n", res.RunID, res.Engine, len(res.Entries))
	for _, e := range res.Entries {
		fmt.Printf("  %-26s %-9s series=%-6d points=%-7d sha=%.12s err=%s\n",
			e.Name, e.Type, e.Series, e.Points, e.SHA256, e.Error)
	}
}
