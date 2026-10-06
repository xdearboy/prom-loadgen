package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
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
		maxRuns     = flag.Int("max-runs", 2000, "upper bound of measured requests per query")
		warmup      = flag.Int("warmup", 1, "unmeasured requests per query before measuring")
		mode        = flag.String("mode", "bench", "bench or dump")
		out         = flag.String("out", "", "path for the result json")
		timeout     = flag.Duration("query-timeout", 2*time.Minute, "per query timeout")
		lookback    = flag.Duration("range-offset", 10*time.Minute, "how far behind now a range query ends")
		categories  = flag.String("categories", "", "comma separated category filter")
		only        = flag.String("queries", "", "comma separated query name filter")
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
	suiteQueries = filter(suiteQueries, *categories, *only)
	if len(suiteQueries) == 0 {
		log.Fatal("query filter removed every query")
	}

	client := prom.New(*target, *timeout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *mode == "dump" {
		res := dump(ctx, client, *runID, *engineName, *target, *suite, suiteQueries, *timeout, *atMs, *lookback)
		if err := save(*out, res); err != nil {
			log.Fatal(err)
		}
		printDump(res)
		return
	}

	if *concurrency < 1 || *minRuns < 1 || *maxRuns < *minRuns || *warmup < 0 {
		log.Fatal("need concurrency >= 1, min-runs >= 1, max-runs >= min-runs and warmup >= 0")
	}
	b := budget{PerQuery: *perQuery, MinRuns: *minRuns, MaxRuns: *maxRuns, Warmup: *warmup}
	res := bench(ctx, client, *runID, *engineName, *target, *suite, *phase,
		suiteQueries, *concurrency, b, *timeout, *lookback, *atMs)
	if err := save(*out, res); err != nil {
		log.Fatal(err)
	}
	printBench(res)
}

func filter(in []queries.Query, categories, names string) []queries.Query {
	wantCategory := splitList(categories)
	wantName := splitList(names)
	if len(wantCategory) == 0 && len(wantName) == 0 {
		return in
	}
	out := make([]queries.Query, 0, len(in))
	for _, q := range in {
		if len(wantCategory) > 0 && !contains(wantCategory, q.Category) {
			continue
		}
		if len(wantName) > 0 && !contains(wantName, q.Name) {
			continue
		}
		out = append(out, q)
	}
	return out
}

type budget struct {
	PerQuery time.Duration
	MinRuns  int
	MaxRuns  int
	Warmup   int
}

func bench(ctx context.Context, client *prom.Client, runID, engine, target, suite, phase string,
	suiteQueries []queries.Query, concurrency int, b budget, timeout, lookback time.Duration, atMs int64) *results.QueryResult {

	res := &results.QueryResult{
		RunID:       runID,
		Engine:      engine,
		Target:      target,
		Suite:       suite,
		Concurrency: concurrency,
		Phase:       phase,
		StartedAt:   time.Now().UTC(),
	}
	for _, q := range suiteQueries {
		if ctx.Err() != nil {
			break
		}
		m := newMetricRun(q, int64(b.MaxRuns))
		for i := 0; i < b.Warmup && ctx.Err() == nil; i++ {
			runOnce(ctx, client, newMetricRun(q, 1), timeout, lookback, atMs)
		}
		loop(ctx, concurrency, b, func() { runOnce(ctx, client, m, timeout, lookback, atMs) })
		res.Queries = append(res.Queries, m.snapshot())
	}
	res.FinishedAt = time.Now().UTC()
	res.DurationSec = res.FinishedAt.Sub(res.StartedAt).Seconds()
	return res
}

func loop(ctx context.Context, concurrency int, b budget, run func()) {
	deadline := time.Now().Add(b.PerQuery)
	var (
		mu      sync.Mutex
		started int
		wg      sync.WaitGroup
	)
	claim := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if started >= b.MaxRuns || (started >= b.MinRuns && !time.Now().Before(deadline)) {
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

type metricRun struct {
	query       queries.Query
	latency     *stats.Recorder
	mu          sync.Mutex
	requests    int64
	errors      int64
	series      int64
	points      int64
	empty       int64
	serverTotal float64
	errorSample string
}

func newMetricRun(q queries.Query, capacity int64) *metricRun {
	if capacity < 16 {
		capacity = 16
	}
	return &metricRun{query: q, latency: stats.NewRecorder(int(capacity))}
}

func (m *metricRun) record(err error, elapsed time.Duration, matrix prom.Matrix) {
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
	m.series = int64(len(matrix.Result))
	points := int64(0)
	for _, s := range matrix.Result {
		points += int64(len(s.Values))
	}
	m.points = points
	if len(matrix.Result) == 0 {
		m.empty++
	}
}

func (m *metricRun) snapshot() results.QueryMetric {
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
		Points:      m.points,
		EmptyRuns:   m.empty,
		ServerMSAvg: m.serverTotal / float64(maxInt64(m.requests, 1)),
		ErrorSample: m.errorSample,
	}
}

func runOnce(ctx context.Context, client *prom.Client, m *metricRun, timeout, lookback time.Duration, atMs int64) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		matrix  prom.Matrix
		elapsed float64
		err     error
	)
	end := evaluationEnd(atMs, lookback)
	switch m.query.Type {
	case queries.Instant:
		matrix, elapsed, err = client.Query(reqCtx, m.query.Expr, end, timeout)
	case queries.Range:
		stepDur, stepErr := m.query.StepDuration()
		window, windowErr := m.query.WindowDuration()
		switch {
		case stepErr != nil:
			err = stepErr
		case windowErr != nil:
			err = windowErr
		default:
			matrix, elapsed, err = client.QueryRange(reqCtx, m.query.Expr,
				end.Add(-window), end,
				stepDur, timeout)
		}
	}
	m.record(err, time.Duration(elapsed*float64(time.Second)), matrix)
}

func evaluationEnd(atMs int64, lookback time.Duration) time.Time {
	if atMs > 0 {
		return time.UnixMilli(atMs).UTC()
	}
	return time.Now().Add(-lookback)
}

func dump(ctx context.Context, client *prom.Client, runID, engine, target, suite string,
	suiteQueries []queries.Query, timeout time.Duration, atMs int64, lookback time.Duration) *results.DumpResult {

	res := &results.DumpResult{
		RunID:       runID,
		Engine:      engine,
		Target:      target,
		Suite:       suite,
		GeneratedAt: time.Now().UTC(),
	}
	end := evaluationEnd(atMs, lookback)
	for _, q := range suiteQueries {
		entry := results.DumpEntry{Name: q.Name, Expr: q.Expr, Type: string(q.Type)}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		var (
			matrix  prom.Matrix
			elapsed float64
			err     error
		)
		if q.Type == queries.Instant {
			matrix, elapsed, err = client.Query(reqCtx, q.Expr, end, timeout)
		} else {
			stepDur, stepErr := q.StepDuration()
			window, windowErr := q.WindowDuration()
			switch {
			case stepErr != nil:
				err = stepErr
			case windowErr != nil:
				err = windowErr
			default:
				matrix, elapsed, err = client.QueryRange(reqCtx, q.Expr, end.Add(-window), end,
					stepDur, timeout)
			}
		}
		cancel()
		entry.Elapsed = elapsed
		if err != nil {
			entry.Error = err.Error()
			res.Entries = append(res.Entries, entry)
			continue
		}
		sum := sha256.Sum256([]byte(prom.CanonicalSeries(matrix)))
		entry.SHA256 = hex.EncodeToString(sum[:])
		entry.Series = int64(len(matrix.Result))
		for _, s := range matrix.Result {
			entry.Points += int64(len(s.Values))
		}
		res.Entries = append(res.Entries, entry)
	}
	return res
}

func save(path string, v any) error {
	if path == "" {
		return nil
	}
	return results.WriteJSON(path, v)
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
		fmt.Printf("  %-26s %-9s series=%-6d points=%-7d sha=%s err=%s\n",
			e.Name, e.Type, e.Series, e.Points, shortSHA(e.SHA256), e.Error)
	}
}

func shortSHA(sha string) string {
	if len(sha) <= 12 {
		return sha
	}
	return sha[:12]
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
