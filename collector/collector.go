package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/xdearboy/prom-loadgen/kube"
	"github.com/xdearboy/prom-loadgen/prom"
	"github.com/xdearboy/prom-loadgen/results"
)

type Config struct {
	Engine    string
	Target    string
	Namespace string
	NodeName  string
	RunID     string
	Phase     string
	Interval  time.Duration
	Duration  time.Duration
	PodLabel  string
	UseCgroup bool
}

type Collector struct {
	cfg            Config
	engine         *prom.Client
	kube           *kube.Client
	podName        string
	walCompression string
	flagsRead      bool
	degraded       []string
}

func joinError(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}

func New(cfg Config) *Collector {
	c := &Collector{cfg: cfg, engine: prom.New(cfg.Target, 30*time.Second)}
	if cfg.UseCgroup {
		if client, err := kube.InCluster(); err == nil {
			c.kube = client
		} else {
			c.degraded = append(c.degraded, "kube api: "+err.Error())
		}
	}
	return c
}

func (c *Collector) Run(ctx context.Context, out io.Writer) error {
	deadline := time.Now().Add(c.cfg.Duration)
	pending := time.Now()
	for {
		if err := c.sample(ctx); err != nil && len(c.degraded) < 8 {
			c.degraded = append(c.degraded, err.Error())
		}
		sample, err := c.Sample(ctx)
		if err != nil {
			sample.Error = joinError(sample.Error, err.Error())
		}
		body, err := json.Marshal(sample)
		if err != nil {
			return err
		}
		if _, err := out.Write(append(body, '\n')); err != nil {
			return err
		}
		if c.cfg.Duration > 0 && !time.Now().Before(deadline) {
			return nil
		}
		pending = pending.Add(c.cfg.Interval)
		if wait := time.Until(pending); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		} else {
			pending = time.Now()
		}
	}
}

func (c *Collector) sample(ctx context.Context) error {
	if c.kube != nil && c.podName == "" {
		pods, err := c.kube.PodsInNamespace(ctx, c.cfg.Namespace, c.cfg.PodLabel)
		if err != nil {
			return err
		}
		for _, p := range pods {
			if p.Spec.NodeName == c.cfg.NodeName {
				c.podName = p.Metadata.Name
				break
			}
		}
	}
	return nil
}

func (c *Collector) Sample(ctx context.Context) (results.Sample, error) {
	now := time.Now().UTC()
	s := results.Sample{
		TS:     now,
		RunID:  c.cfg.RunID,
		Engine: c.cfg.Engine,
		Phase:  c.cfg.Phase,
	}
	self, err := c.engine.Metrics(ctx)
	if err != nil {
		return s, fmt.Errorf("engine metrics: %w", err)
	}
	s.Self = pick(self, selfMetrics)

	if status, err := c.engine.TSDBStatus(ctx); err == nil {
		s.Head = map[string]float64{
			"numSeries":     float64(status.HeadStats.NumSeries),
			"chunkCount":    float64(status.HeadStats.NumChunks),
			"numLabelPairs": float64(status.HeadStats.NumLabelSet),
			"minTimeMs":     float64(status.HeadStats.MinTimeMs),
			"maxTimeMs":     float64(status.HeadStats.MaxTimeMs),
		}
	}
	if info, err := c.engine.RuntimeInfo(ctx); err == nil {
		s.GOMEMLimit = info["GOMEMLIMIT"]
		s.GOGC = info["GOGC"]
		s.GOMAXPROCS = info["GOMAXPROCS"]
	}
	if !c.flagsRead {
		if flags, err := c.engine.Flags(ctx); err == nil {
			c.flagsRead = true
			c.walCompression = flags["storage.tsdb.wal-compression"]
		}
	}
	s.WALCompression = c.walCompression
	if version, _, err := c.engine.BuildInfo(ctx); err == nil {
		s.Version = version
	}
	if c.kube != nil && c.podName != "" {
		if data, err := c.cadvisor(ctx); err == nil {
			s.Cadvisor = data
		} else if len(c.degraded) < 8 {
			c.degraded = append(c.degraded, "cadvisor: "+err.Error())
		}
	}
	if len(c.degraded) > 0 {
		s.Error = strings.Join(c.degraded, "; ")
	}
	return s, nil
}

var selfMetrics = []string{
	"process_resident_memory_bytes",
	"process_virtual_memory_bytes",
	"process_cpu_seconds_total",
	"go_memstats_heap_alloc_bytes",
	"go_memstats_heap_inuse_bytes",
	"go_memstats_sys_bytes",
	"go_memstats_alloc_bytes_total",
	"go_goroutines",
	"go_threads",
	"prometheus_tsdb_head_series",
	"prometheus_tsdb_wal_fsync_duration_seconds_sum",
	"prometheus_tsdb_wal_fsync_duration_seconds_count",
	"prometheus_tsdb_head_chunks_loaded",
	"prometheus_remote_storage_samples_in_total",
	"prometheus_remote_storage_samples_pending",
	"prompp_common_jemalloc_memory_allocated_bytes",
	"prompp_common_jemalloc_memory_in_use_bytes",
	"prompp_common_jemalloc_resident_memory_bytes",
	"prompp_common_jemalloc_memory_threshold_bytes",
}

var cadvisorMetrics = map[string][]string{
	"engine": {
		"container_memory_working_set_bytes",
		"container_memory_usage_bytes",
		"container_memory_rss",
		"container_memory_failcnt",
		"container_cpu_usage_seconds_total",
		"container_spec_memory_limit_bytes",
		"container_oom_events_total",
	},
	"node": {
		"machine_memory_bytes",
		"machine_memory_available_bytes",
		"machine_cpu_cores",
		"node_memory_working_set_bytes",
		"node_cpu_seconds_total",
		"filesystem_avail_bytes",
		"filesystem_capacity_bytes",
	},
}

func (c *Collector) cadvisor(ctx context.Context) (map[string]float64, error) {
	body, err := c.kube.NodeMetrics(ctx, c.cfg.NodeName, "metrics/cadvisor")
	if err != nil {
		return nil, err
	}
	families, err := parseText(body)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for name, mf := range families {
		if want, ok := cadvisorMetrics["engine"]; ok && contains(want, name) {
			for _, m := range mf.GetMetric() {
				if labelsMatch(m, map[string]string{
					"namespace": c.cfg.Namespace,
					"pod":       c.podName,
					"container": "engine",
				}) {
					out["engine_"+name] = value(m)
				}
			}
			continue
		}
		if contains(cadvisorMetrics["node"], name) {
			for _, m := range mf.GetMetric() {
				if name == "node_cpu_seconds_total" && m.GetLabel() != nil && labelValue(m, "mode") != "idle" {
					continue
				}
				if len(m.GetLabel()) == 0 || (name != "node_cpu_seconds_total" && name != "filesystem_avail_bytes" && name != "filesystem_capacity_bytes") {
					out["node_"+name] = value(m)
				}
			}
		}
	}
	if _, ok := out["engine_container_memory_working_set_bytes"]; !ok {
		return out, fmt.Errorf("no container metrics for pod %s", c.podName)
	}
	return out, nil
}

func parseText(body []byte) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	return families, nil
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		if labelValue(m, k) != v {
			return false
		}
	}
	return true
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func value(m *dto.Metric) float64 {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	default:
		return 0
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func pick(all map[string]float64, names []string) map[string]float64 {
	out := make(map[string]float64, len(names))
	for _, n := range names {
		if v, ok := all[n]; ok {
			out[n] = v
		}
	}
	return out
}
