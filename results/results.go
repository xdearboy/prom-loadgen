package results

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xdearboy/prom-loadgen/stats"
)

const (
	KindIngest = "ingest"
	KindQuery  = "query"
	KindDump   = "dump"
	KindMeta   = "run"
)

type HeadStats struct {
	NumSeries   int64  `json:"num_series,omitempty"`
	NumChunks   int64  `json:"num_chunks,omitempty"`
	NumLabelSet int64  `json:"num_label_pairs,omitempty"`
	MinTimeMs   int64  `json:"head_min_time_ms,omitempty"`
	MaxTimeMs   int64  `json:"head_max_time_ms,omitempty"`
	Error       string `json:"error,omitempty"`
}

type StepResources struct {
	Samples            int     `json:"samples"`
	RSSSelfAvgBytes    float64 `json:"rss_self_avg_bytes"`
	RSSSelfP95Bytes    float64 `json:"rss_self_p95_bytes"`
	RSSSelfMaxBytes    float64 `json:"rss_self_max_bytes"`
	WorkingSetAvgBytes float64 `json:"working_set_avg_bytes"`
	WorkingSetMaxBytes float64 `json:"working_set_max_bytes"`
	CPUCoresAvg        float64 `json:"cpu_cores_avg"`
	CPUCoresMax        float64 `json:"cpu_cores_max"`
	GoMemHeapAvgBytes  float64 `json:"go_mem_heap_avg_bytes"`
	GoMemSysAvgBytes   float64 `json:"go_mem_sys_avg_bytes"`
	GoroutinesAvg      float64 `json:"goroutines_avg"`
	DataDirBytes       int64   `json:"data_dir_bytes"`
	WALBytes           int64   `json:"wal_bytes"`
	BlocksBytes        int64   `json:"blocks_bytes"`
	RSSBytesPerSeries  float64 `json:"rss_bytes_per_series"`
	DiskBytesPerSeries float64 `json:"disk_bytes_per_series"`
	OOMKills           int64   `json:"oom_kills"`
	Restarts           int64   `json:"restarts"`
}

type IngestStep struct {
	Index                  int            `json:"index"`
	Series                 int            `json:"series"`
	DurationSeconds        float64        `json:"duration_seconds"`
	IntervalSeconds        float64        `json:"interval_seconds"`
	StartedAt              time.Time      `json:"started_at"`
	FinishedAt             time.Time      `json:"finished_at"`
	ElapsedSeconds         float64        `json:"elapsed_seconds"`
	Ticks                  int64          `json:"ticks"`
	Requests               int64          `json:"requests"`
	RequestsOK             int64          `json:"requests_ok"`
	HTTP4xx                int64          `json:"http_4xx"`
	HTTP5xx                int64          `json:"http_5xx"`
	ClientErrors           int64          `json:"client_errors"`
	SamplesSent            int64          `json:"samples_sent"`
	SamplesFailed          int64          `json:"samples_failed"`
	BytesWire              int64          `json:"bytes_wire"`
	BytesSnappy            int64          `json:"bytes_snappy"`
	AchievedSamplesPerSec  float64        `json:"achieved_samples_per_sec"`
	AchievedRequestsPerSec float64        `json:"achieved_requests_per_sec"`
	RequestLatency         stats.Summary  `json:"request_latency"`
	ErrorSamples           []string       `json:"error_samples,omitempty"`
	Head                   HeadStats      `json:"head"`
	Resources              *StepResources `json:"resources,omitempty"`
}

// a step that ran long means the offered rate was not sustained
func (s IngestStep) Overran() bool {
	return s.ElapsedSeconds > s.DurationSeconds+s.IntervalSeconds
}

type IngestResult struct {
	RunID            string       `json:"run_id"`
	Engine           string       `json:"engine"`
	Target           string       `json:"target"`
	IntervalSeconds  float64      `json:"interval_seconds"`
	SeriesPlanned    int          `json:"series_planned"`
	BatchSeries      int          `json:"batch_series"`
	Workers          int          `json:"workers"`
	EpochMs          int64        `json:"epoch_ms"`
	TimestampsPinned bool         `json:"timestamps_pinned"`
	StartedAt        time.Time    `json:"started_at"`
	FinishedAt       time.Time    `json:"finished_at"`
	Steps            []IngestStep `json:"steps"`
}

func (r *IngestResult) TotalSamples() int64 {
	var n int64
	for _, s := range r.Steps {
		n += s.SamplesSent
	}
	return n
}

func (r *IngestResult) FailedSamples() int64 {
	var n int64
	for _, s := range r.Steps {
		n += s.SamplesFailed
	}
	return n
}

type QueryMetric struct {
	Name        string        `json:"name"`
	Expr        string        `json:"expr"`
	Category    string        `json:"category"`
	Type        string        `json:"type"`
	Requests    int64         `json:"requests"`
	Errors      int64         `json:"errors"`
	Latency     stats.Summary `json:"latency"`
	Series      int64         `json:"result_series"`
	Points      int64         `json:"result_points"`
	EmptyRuns   int64         `json:"empty_results"`
	ServerMSAvg float64       `json:"server_seconds_avg"`
	ErrorSample string        `json:"error_sample,omitempty"`
}

type QueryResources struct {
	Samples            int     `json:"samples"`
	CPUCoresAvg        float64 `json:"cpu_cores_avg"`
	CPUCoresMax        float64 `json:"cpu_cores_max"`
	RSSSelfAvgBytes    float64 `json:"rss_self_avg_bytes"`
	WorkingSetAvgBytes float64 `json:"working_set_avg_bytes"`
	WorkingSetMaxBytes float64 `json:"working_set_max_bytes"`
}

type QueryResult struct {
	RunID       string          `json:"run_id"`
	Engine      string          `json:"engine"`
	Target      string          `json:"target"`
	Suite       string          `json:"suite"`
	Concurrency int             `json:"concurrency"`
	DurationSec float64         `json:"duration_seconds"`
	Phase       string          `json:"phase"`
	StartedAt   time.Time       `json:"started_at"`
	FinishedAt  time.Time       `json:"finished_at"`
	Queries     []QueryMetric   `json:"queries"`
	Resources   *QueryResources `json:"resources,omitempty"`
}

func (r *QueryResult) TotalRequests() int64 {
	var n int64
	for _, q := range r.Queries {
		n += q.Requests
	}
	return n
}

func (r *QueryResult) TotalErrors() int64 {
	var n int64
	for _, q := range r.Queries {
		n += q.Errors
	}
	return n
}

func (r *QueryResult) ByName(name string) (QueryMetric, bool) {
	for _, q := range r.Queries {
		if q.Name == name {
			return q, true
		}
	}
	return QueryMetric{}, false
}

type DumpEntry struct {
	Name    string  `json:"name"`
	Expr    string  `json:"expr"`
	Type    string  `json:"type"`
	Series  int64   `json:"series"`
	Points  int64   `json:"points"`
	SHA256  string  `json:"sha256"`
	Elapsed float64 `json:"elapsed_seconds"`
	Error   string  `json:"error,omitempty"`
}

type DumpResult struct {
	RunID       string      `json:"run_id"`
	Engine      string      `json:"engine"`
	Target      string      `json:"target"`
	Suite       string      `json:"suite"`
	EpochMs     int64       `json:"epoch_ms"`
	GeneratedAt time.Time   `json:"generated_at"`
	Entries     []DumpEntry `json:"entries"`
}

func (r *DumpResult) ByName(name string) (DumpEntry, bool) {
	for _, e := range r.Entries {
		if e.Name == name {
			return e, true
		}
	}
	return DumpEntry{}, false
}

type Sample struct {
	TS             time.Time          `json:"ts"`
	RunID          string             `json:"run_id"`
	Engine         string             `json:"engine"`
	Phase          string             `json:"phase"`
	Self           map[string]float64 `json:"self,omitempty"`
	Head           map[string]float64 `json:"head,omitempty"`
	Cadvisor       map[string]float64 `json:"cadvisor,omitempty"`
	GOMEMLimit     string             `json:"gomemlimit,omitempty"`
	GOGC           string             `json:"gogc,omitempty"`
	GOMAXPROCS     string             `json:"gomaxprocs,omitempty"`
	WALCompression string             `json:"wal_compression,omitempty"`
	Version        string             `json:"version,omitempty"`
	Error          string             `json:"error,omitempty"`
}

type DiskSample struct {
	TS           time.Time `json:"ts"`
	RunID        string    `json:"run_id"`
	Engine       string    `json:"engine"`
	DataDirBytes int64     `json:"data_dir_bytes"`
	WALBytes     int64     `json:"wal_bytes"`
	BlocksBytes  int64     `json:"blocks_bytes"`
}

type DiskArtifact struct {
	RunID   string       `json:"run_id"`
	Engine  string       `json:"engine"`
	Samples []DiskSample `json:"samples"`
}

func (d *DiskArtifact) At(t time.Time) (DiskSample, bool) {
	var out DiskSample
	found := false
	for _, s := range d.Samples {
		if !s.TS.After(t) {
			out = s
			found = true
		}
	}
	return out, found
}

type NodeInfo struct {
	Name             string `json:"name"`
	KernelVersion    string `json:"kernel_version"`
	OSImage          string `json:"os_image"`
	Arch             string `json:"arch"`
	CPUModel         string `json:"cpu_model"`
	CPUCores         int    `json:"cpu_cores"`
	MemTotalBytes    int64  `json:"mem_total_bytes"`
	FSAvailableBytes int64  `json:"fs_available_bytes"`
	FSCapacityBytes  int64  `json:"fs_capacity_bytes"`
}

type EngineMeta struct {
	Name     string   `json:"name"`
	Image    string   `json:"image"`
	Version  string   `json:"version"`
	Args     []string `json:"args"`
	CPUMilli int64    `json:"cpu_milli_limit"`
	MemBytes int64    `json:"memory_limit_bytes"`
	Env      []string `json:"env,omitempty"`
	Notes    string   `json:"notes,omitempty"`
}

type RunMeta struct {
	RunID      string            `json:"run_id"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	GitCommit  string            `json:"git_commit,omitempty"`
	Harness    string            `json:"harness_version"`
	Node       NodeInfo          `json:"node"`
	Engines    []EngineMeta      `json:"engines"`
	Notes      []string          `json:"notes,omitempty"`
	Extra      map[string]string `json:"extra,omitempty"`
}

func (m *RunMeta) Engine(name string) (EngineMeta, bool) {
	for _, e := range m.Engines {
		if e.Name == name {
			return e, true
		}
	}
	return EngineMeta{}, false
}

func WriteJSON(path string, v any) error {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ReadJSON(path string, v any) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

func RunDir(root, runID string) string { return filepath.Join(root, runID) }

func EngineDir(root, runID, engine string) string {
	return filepath.Join(RunDir(root, runID), engine)
}

func ArtifactPath(root, runID, kind, engine string) string {
	name := kind + ".json"
	if engine == "" {
		return filepath.Join(RunDir(root, runID), name)
	}
	return filepath.Join(EngineDir(root, runID, engine), name)
}

func DiskSamplesPath(root, runID, engine string) string {
	return filepath.Join(EngineDir(root, runID, engine), "disk.jsonl")
}

func SamplesPath(root, runID, engine, phase string) string {
	name := "samples"
	if phase != "" {
		name += "." + phase
	}
	return filepath.Join(EngineDir(root, runID, engine), name+".jsonl")
}

func ListArtifacts(root, runID, kind string) ([]string, error) {
	dir := RunDir(root, runID)
	engines, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, engine := range engines {
		if !engine.IsDir() {
			continue
		}
		out = append(out, ArtifactVariants(root, runID, kind, engine.Name())...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s artifacts in %s", kind, dir)
	}
	return out, nil
}

func ArtifactVariants(root, runID, kind, engine string) []string {
	engineDir := EngineDir(root, runID, engine)
	entries, err := os.ReadDir(engineDir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		if name == kind+".json" || strings.HasPrefix(name, kind+"-") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, filepath.Join(engineDir, name))
	}
	return out
}

func ListEngines(root, runID string) ([]string, error) {
	dir := RunDir(root, runID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func ReadDiskSamples(path string) ([]DiskSample, error) { return readLines[DiskSample](path) }

func ReadSamples(path string) ([]Sample, error) { return readLines[Sample](path) }

func readLines[T any](path string) ([]T, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []T
	for i, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, i+1, err)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, errors.New("no records in " + path)
	}
	return out, nil
}
