package prom

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTSDBStatusUnwrapsDataEnvelope(t *testing.T) {
	const payload = `{"status":"success","data":{
		"headStats":{"numSeries":50864,"numLabelPairs":1517,"chunkCount":50864,
			"minTime":1767225600000,"maxTime":1791230004587},
		"seriesCountByMetricName":[{"name":"bench_memory_bytes","value":6250}]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/status/tsdb" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	status, err := New(srv.URL, 5*time.Second).TSDBStatus(context.Background())
	if err != nil {
		t.Fatalf("TSDBStatus: %v", err)
	}
	if status.HeadStats.NumSeries != 50864 {
		t.Errorf("NumSeries = %d, want 50864 (data envelope not unwrapped)", status.HeadStats.NumSeries)
	}
	if status.HeadStats.MinTimeMs != 1767225600000 || status.HeadStats.MaxTimeMs != 1791230004587 {
		t.Errorf("time range = %d..%d, want 1767225600000..1791230004587", status.HeadStats.MinTimeMs, status.HeadStats.MaxTimeMs)
	}
	if len(status.Series) != 1 || status.Series[0].Name != "bench_memory_bytes" || status.Series[0].Value != 6250 {
		t.Errorf("series = %+v, want one bench_memory_bytes=6250 entry", status.Series)
	}
}

func TestTSDBStatusReportsNonSuccessStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","error":"boom"}`))
	}))
	defer srv.Close()

	if _, err := New(srv.URL, 5*time.Second).TSDBStatus(context.Background()); err == nil {
		t.Fatal("want an error for a non success api status, got nil")
	}
}

func TestFormatTimeUsesSecondsNotMilliseconds(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{time.Unix(1767225600, 0), "1767225600.000"},
		{time.Unix(1767225645, 500_000_000), "1767225645.500"},
		{time.UnixMilli(1767225600000), "1767225600.000"},
		{time.UnixMilli(1767225645123), "1767225645.123"},
	}
	for _, tc := range cases {
		if got := formatTime(tc.in); got != tc.want {
			t.Errorf("formatTime(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestQuerySendsSecondPrecisionTime(t *testing.T) {
	var gotQuery, gotTime, gotStep string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		gotTime = r.URL.Query().Get("time")
		gotStep = r.URL.Query().Get("step")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer srv.Close()

	c := New(srv.URL, 5*time.Second)
	if _, _, err := c.Query(context.Background(), "bench_cpu_cores", time.UnixMilli(1767225645123), 5*time.Second); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if gotQuery != "bench_cpu_cores" {
		t.Errorf("query = %q", gotQuery)
	}
	if gotTime != "1767225645.123" {
		t.Errorf("time = %q, want 1767225645.123 (seconds, not milliseconds)", gotTime)
	}

	if _, _, err := c.QueryRange(context.Background(), "bench_cpu_cores",
		time.UnixMilli(1767225600000), time.UnixMilli(1767225600000+3600000), 30*time.Second, 5*time.Second); err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if gotStep != "30" {
		t.Errorf("step = %q, want 30", gotStep)
	}
	if got := srvURLQuery(t, srv.URL, "/api/v1/query_range", map[string]string{
		"query": "bench_cpu_cores", "start": "1767225600", "end": "1767225660", "step": "15",
	}); got == "" {
		t.Error("expected a rendered query_range url")
	}
}

func srvURLQuery(t *testing.T, base, path string, params map[string]string) string {
	t.Helper()
	u := base + path
	first := true
	for k, v := range params {
		if first {
			u += "?" + k + "=" + v
			first = false
		} else {
			u += "&" + k + "=" + v
		}
	}
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get %s: %v", u, err)
	}
	defer resp.Body.Close()
	return u
}
