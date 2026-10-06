package queries

import (
	"fmt"
	"time"
)

type Type string

const (
	Instant Type = "instant"
	Range   Type = "range"
)

type Query struct {
	Name     string
	Expr     string
	Category string
	Type     Type
	Step     string
	Window   string
	Heavy    bool
}

var metric = "bench_cpu_cores"
var counter = "bench_http_requests_total"
var durationSum = "bench_http_request_duration_seconds_sum"
var durationCount = "bench_http_request_duration_seconds_count"

var core = []Query{
	{Name: "selector", Category: "select", Expr: metric, Type: Instant},
	{Name: "selector_labels", Category: "select", Expr: fmt.Sprintf(`bench_memory_bytes{namespace=~"ns-0[1-5]", job="bench"}`), Type: Instant},
	{Name: "matchers_numeric", Category: "select", Expr: fmt.Sprintf(`bench_queue_length > 1.5`), Type: Instant},
	{Name: "absent", Category: "select", Expr: `absent(bench_missing_metric)`, Type: Instant},
	{Name: "regex_name", Category: "select", Expr: `{__name__=~"bench_http_.*"}`, Type: Instant, Heavy: true},

	{Name: "sum", Category: "aggregate", Expr: `sum(bench_cpu_cores)`, Type: Instant},
	{Name: "sum_by_namespace", Category: "aggregate", Expr: `sum by (namespace) (bench_cpu_cores)`, Type: Instant},
	{Name: "count_by_job", Category: "aggregate", Expr: `count by (job) (bench_cpu_cores)`, Type: Instant},
	{Name: "avg_over_pods", Category: "aggregate", Expr: `avg by (namespace) (bench_memory_bytes)`, Type: Instant},
	{Name: "topk", Category: "aggregate", Expr: `topk(20, bench_cpu_cores)`, Type: Instant},
	{Name: "bottomk", Category: "aggregate", Expr: `bottomk(5, bench_cpu_cores)`, Type: Instant},
	{Name: "count_values", Category: "aggregate", Expr: `count_values("status", bench_container_ready)`, Type: Instant},
	{Name: "stddev", Category: "aggregate", Expr: `stddev by (namespace) (bench_memory_bytes)`, Type: Instant},

	{Name: "rate", Category: "function", Expr: fmt.Sprintf(`rate(%s[5m])`, counter), Type: Instant},
	{Name: "irate", Category: "function", Expr: fmt.Sprintf(`irate(%s[1m])`, counter), Type: Instant},
	{Name: "increase", Category: "function", Expr: fmt.Sprintf(`increase(%s[30m])`, counter), Type: Instant},
	{Name: "avg_over_time", Category: "function", Expr: fmt.Sprintf(`avg_over_time(%s[10m])`, metric), Type: Instant},
	{Name: "max_over_time", Category: "function", Expr: fmt.Sprintf(`max_over_time(%s[15m])`, metric), Type: Instant},
	{Name: "quantile_over_time", Category: "function", Expr: fmt.Sprintf(`quantile_over_time(0.95, %s[10m])`, metric), Type: Instant},
	{Name: "rate_histogram", Category: "function",
		Expr: fmt.Sprintf(`rate(%s[5m]) / rate(%s[5m])`, durationSum, durationCount), Type: Instant},

	{Name: "join", Category: "combine", Expr: fmt.Sprintf(`sum by (namespace) (%s) / on (namespace) sum by (namespace) (bench_memory_bytes)`, counter), Type: Instant},
	{Name: "vector_matching", Category: "combine", Expr: `bench_cpu_cores * on (namespace) group_left() count by (namespace) (bench_queue_length)`, Type: Instant},
	{Name: "binary_scalar", Category: "combine", Expr: `sum(bench_cpu_cores) / count(count by (namespace) (bench_cpu_cores))`, Type: Instant},
	{Name: "clamp", Category: "combine", Expr: `clamp_max(clamp_min(bench_memory_bytes, 0), 1e12)`, Type: Instant},
	{Name: "label_replace", Category: "combine", Expr: `label_replace(bench_cpu_cores, "tier", "front", "namespace", "ns-0.*")`, Type: Instant},

	{Name: "range_gauge", Category: "range", Expr: metric, Type: Range, Step: "30s", Window: "1h"},
	{Name: "range_sum_rate", Category: "range", Expr: fmt.Sprintf(`sum by (namespace) (rate(%s[5m]))`, counter), Type: Range, Step: "60s", Window: "3h", Heavy: true},
	{Name: "range_subquery", Category: "range", Expr: fmt.Sprintf(`max_over_time(%s[30m:1m])`, metric), Type: Range, Step: "60s", Window: "2h"},
	{Name: "range_quantile", Category: "range", Expr: fmt.Sprintf(`quantile_over_time(0.9, %s[20m])`, metric), Type: Range, Step: "60s", Window: "3h"},

	{Name: "recording_rule_shape", Category: "range", Expr: fmt.Sprintf(`sum(rate(%s[5m]))`, counter), Type: Range, Step: "30s", Window: "1h"},
	{Name: "alerting_shape", Category: "range", Expr: fmt.Sprintf(`sum by (namespace) (rate(%s[5m])) > 1000`, counter), Type: Range, Step: "30s", Window: "1h"},
}

var extra = []Query{
	{Name: "group_left_many", Category: "combine", Expr: fmt.Sprintf(`bench_memory_bytes * on (namespace, pod) group_left() %s`, metric), Type: Instant},
	{Name: "double_subquery", Category: "range", Expr: fmt.Sprintf(`max_over_time(sum by (namespace) (rate(%s[1m]))[10m:1m])`, counter), Type: Range, Step: "60s", Window: "1h"},
	{Name: "sort_desc", Category: "combine", Expr: `sort_desc(sum by (namespace) (bench_memory_bytes))`, Type: Instant},
	{Name: "offset", Category: "range", Expr: metric, Type: Range, Step: "30s", Window: "30m", Heavy: true},
	{Name: "last_over_time_all", Category: "function", Expr: `count(last_over_time({__name__=~"bench_.*"}[5m]))`, Type: Instant, Heavy: true},
	{Name: "or_fallback", Category: "combine", Expr: `bench_missing_metric or bench_cpu_cores`, Type: Instant},
	{Name: "nested_aggregate", Category: "aggregate", Expr: `sum(topk(50, bench_cpu_cores))`, Type: Instant},
}

var suites = map[string][]Query{
	"core":  core,
	"heavy": append(append([]Query{}, core...), extra...),
}

func Names() []string {
	return []string{"core", "heavy"}
}

func Get(name string) ([]Query, error) {
	q, ok := suites[name]
	if !ok {
		return nil, fmt.Errorf("unknown suite %q, have %v", name, Names())
	}
	return q, nil
}

func MustGet(name string) []Query {
	q, err := Get(name)
	if err != nil {
		panic(err)
	}
	return q
}

func (q Query) StepDuration() (time.Duration, error) {
	switch q.Step {
	case "":
		return 0, nil
	case "30s":
		return 30 * time.Second, nil
	case "60s":
		return 60 * time.Second, nil
	}
	return 0, fmt.Errorf("query %s: unsupported step %q", q.Name, q.Step)
}

func (q Query) WindowDuration() (time.Duration, error) {
	switch q.Window {
	case "":
		return 0, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "3h":
		return 3 * time.Hour, nil
	}
	return 0, fmt.Errorf("query %s: unsupported window %q", q.Name, q.Window)
}
