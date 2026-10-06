package prom

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

const answer = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[1,"2"]},{"metric":{"a":"2"},"value":[1,"3"]},{"metric":{},"value":[1,"4"]}]}}`

func TestCountSeriesIgnoresChunkBoundaries(t *testing.T) {
	readers := map[string]io.Reader{
		"whole": strings.NewReader(answer),
		"byte":  iotest.OneByteReader(strings.NewReader(answer)),
		"half":  iotest.HalfReader(strings.NewReader(answer)),
	}
	for name, r := range readers {
		got, ok, err := countSeries(r)
		if err != nil || !ok || got != 3 {
			t.Errorf("%s: series=%d success=%v err=%v, want 3 true nil", name, got, ok, err)
		}
	}
}

func TestProbeCountsWithoutDecodingAndReportsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") == "bad" {
			http.Error(w, `{"status":"error","error":"parse"}`, http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, answer)
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second)

	path, params := InstantRequest("up", time.Unix(10, 0), time.Minute)
	if n, _, err := c.Probe(context.Background(), path, params); err != nil || n != 3 {
		t.Fatalf("series=%d err=%v, want 3", n, err)
	}
	path, params = InstantRequest("bad", time.Unix(10, 0), time.Minute)
	if _, _, err := c.Probe(context.Background(), path, params); err == nil || !strings.Contains(err.Error(), "http 400") {
		t.Fatalf("want an http 400 error, got %v", err)
	}
}

func TestProbeRejectsAnswerThatIsNotSuccess(t *testing.T) {
	if _, ok, _ := countSeries(strings.NewReader(`{"status":"error","error":"x"}`)); ok {
		t.Fatal("an error answer must not count as success")
	}
}
