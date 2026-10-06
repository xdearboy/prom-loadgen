package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		HTTP:    &http.Client{Timeout: timeout},
	}
}

type QueryResponse struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType,omitempty"`
	Error     string          `json:"error,omitempty"`
	Warnings  []string        `json:"warnings,omitempty"`
	Stats     json.RawMessage `json:"stats,omitempty"`
}

type Matrix struct {
	ResultType string       `json:"resultType"`
	Result     []SeriesData `json:"result"`
}

type SeriesData struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

func (c *Client) Ready(ctx context.Context) error {
	_, _, err := c.BuildInfo(ctx)
	return err
}

func (c *Client) BuildInfo(ctx context.Context) (version string, revision string, err error) {
	body, err := c.get(ctx, "/api/v1/status/buildinfo")
	if err != nil {
		return "", "", err
	}
	var out struct {
		Data struct {
			Version   string `json:"version"`
			Revision  string `json:"revision"`
			Branch    string `json:"branch"`
			GoVersion string `json:"goVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", "", fmt.Errorf("buildinfo: %w", err)
	}
	return out.Data.Version, out.Data.Revision, nil
}

func (c *Client) RuntimeInfo(ctx context.Context) (map[string]string, error) {
	body, err := c.get(ctx, "/api/v1/status/runtimeinfo")
	if err != nil {
		return nil, err
	}
	var out struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("runtimeinfo: %w", err)
	}
	info := make(map[string]string, len(out.Data))
	for k, v := range out.Data {
		info[k] = fmt.Sprint(v)
	}
	return info, nil
}

func (c *Client) Flags(ctx context.Context) (map[string]string, error) {
	body, err := c.get(ctx, "/api/v1/status/flags")
	if err != nil {
		return nil, err
	}
	var out struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("flags: %w", err)
	}
	return out.Data, nil
}

type HeadStats struct {
	NumSeries   int64 `json:"numSeries"`
	NumChunks   int64 `json:"chunkCount"`
	NumLabelSet int64 `json:"numLabelPairs"`
	MinTimeMs   int64 `json:"minTime"`
	MaxTimeMs   int64 `json:"maxTime"`
}

type SeriesCount struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
}

type TSDBStatus struct {
	HeadStats HeadStats     `json:"headStats"`
	Series    []SeriesCount `json:"seriesCountByMetricName"`
}

func (c *Client) TSDBStatus(ctx context.Context) (TSDBStatus, error) {
	var out TSDBStatus
	body, err := c.get(ctx, "/api/v1/status/tsdb")
	if err != nil {
		return out, err
	}
	var envelope struct {
		Status string     `json:"status"`
		Data   TSDBStatus `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return out, fmt.Errorf("tsdb status: %w", err)
	}
	if envelope.Status != "" && envelope.Status != "success" {
		return out, fmt.Errorf("tsdb status: api returned status %q", envelope.Status)
	}
	return envelope.Data, nil
}

func (c *Client) Query(ctx context.Context, expr string, ts time.Time, timeout time.Duration) (Matrix, float64, error) {
	params := url.Values{"query": {expr}}
	if !ts.IsZero() {
		params.Set("time", formatTime(ts))
	}
	params.Set("limit", "0")
	if timeout > 0 {
		params.Set("timeout", timeout.String())
	}
	return c.query(ctx, "/api/v1/query", params)
}

func (c *Client) QueryRange(ctx context.Context, expr string, start, end time.Time, step time.Duration, timeout time.Duration) (Matrix, float64, error) {
	params := url.Values{
		"query": {expr},
		"start": {formatTime(start)},
		"end":   {formatTime(end)},
		"step":  {formatSeconds(step.Seconds())},
		"limit": {"0"},
	}
	if timeout > 0 {
		params.Set("timeout", timeout.String())
	}
	return c.query(ctx, "/api/v1/query_range", params)
}

func (c *Client) query(ctx context.Context, path string, params url.Values) (Matrix, float64, error) {
	var m Matrix
	start := time.Now()
	body, err := c.get(ctx, path+"?"+params.Encode())
	elapsed := time.Since(start).Seconds()
	if err != nil {
		return m, elapsed, err
	}
	var resp QueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return m, elapsed, fmt.Errorf("decode %s: %w", path, err)
	}
	if resp.Status != "success" {
		return m, elapsed, fmt.Errorf("%s: %s: %s", path, resp.ErrorType, resp.Error)
	}
	if err := json.Unmarshal(resp.Data, &m); err != nil {
		return m, elapsed, fmt.Errorf("decode %s data: %w", path, err)
	}
	return m, elapsed, nil
}

func (c *Client) Metrics(ctx context.Context) (map[string]float64, error) {
	body, err := c.get(ctx, "/metrics")
	if err != nil {
		return nil, err
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	out := make(map[string]float64, len(families))
	for name, mf := range families {
		for _, m := range mf.GetMetric() {
			if v, ok := gaugeOrCounter(m); ok {
				out[name] = v
			}
		}
	}
	return out, nil
}

func gaugeOrCounter(m *dto.Metric) (float64, bool) {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue(), true
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue(), true
	case m.GetUntyped() != nil:
		return m.GetUntyped().GetValue(), true
	default:
		return 0, false
	}
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, fmt.Errorf("%s: http %d: %s", path, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return io.ReadAll(resp.Body)
}

func formatTime(t time.Time) string {
	secs := float64(t.UnixNano()) / float64(time.Second)
	return strconv.FormatFloat(secs, 'f', 3, 64)
}

func formatSeconds(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func CanonicalSeries(m Matrix) string {
	type point struct {
		Labels string
		Values string
	}
	rendered := make([]point, 0, len(m.Result))
	for _, s := range m.Result {
		labels := make([]string, 0, len(s.Metric))
		for k, v := range s.Metric {
			labels = append(labels, k+"="+v)
		}
		sort.Strings(labels)
		values := make([]string, 0, len(s.Values))
		for _, v := range s.Values {
			values = append(values, fmt.Sprint(v...))
		}
		rendered = append(rendered, point{Labels: strings.Join(labels, ","), Values: strings.Join(values, ";")})
	}
	sort.Slice(rendered, func(i, j int) bool { return rendered[i].Labels < rendered[j].Labels })
	var b strings.Builder
	for _, r := range rendered {
		b.WriteString(r.Labels)
		b.WriteByte('{')
		b.WriteString(r.Values)
		b.WriteString("}\n")
	}
	return b.String()
}
