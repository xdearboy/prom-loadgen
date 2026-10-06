package collector

import "testing"

func TestParseTextReadsGaugesAndCounters(t *testing.T) {
	families, err := parseText([]byte("# TYPE process_resident_memory_bytes gauge\nprocess_resident_memory_bytes 4096\n# TYPE c_total counter\nc_total 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := value(families["process_resident_memory_bytes"].GetMetric()[0]); got != 4096 {
		t.Fatalf("gauge = %v", got)
	}
	if got := value(families["c_total"].GetMetric()[0]); got != 7 {
		t.Fatalf("counter = %v", got)
	}
}
