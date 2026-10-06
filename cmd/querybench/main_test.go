package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func countRuns(t *testing.T, concurrency int, perQuery time.Duration, minRuns int, each time.Duration) (int64, time.Duration) {
	t.Helper()
	var n atomic.Int64
	start := time.Now()
	loop(context.Background(), concurrency, perQuery, minRuns, func() {
		time.Sleep(each)
		n.Add(1)
	})
	return n.Load(), time.Since(start)
}

func TestLoopRepeatsQueryUntilBudgetIsSpent(t *testing.T) {
	n, elapsed := countRuns(t, 1, 200*time.Millisecond, 1, 10*time.Millisecond)
	if n < 10 {
		t.Fatalf("got %d runs in a 200ms budget of 10ms requests, the query must repeat", n)
	}
	if elapsed > time.Second {
		t.Fatalf("loop ran %s, far past its 200ms budget", elapsed)
	}
}

func TestLoopHonoursMinRunsAfterBudget(t *testing.T) {
	n, _ := countRuns(t, 2, 0, 7, time.Millisecond)
	if n != 7 {
		t.Fatalf("got %d runs, want exactly min-runs 7 when the budget is zero", n)
	}
}

func TestLoopRunsWorkersInParallel(t *testing.T) {
	_, elapsed := countRuns(t, 8, 0, 8, 100*time.Millisecond)
	if elapsed > 400*time.Millisecond {
		t.Fatalf("8 requests on 8 workers took %s, they did not run concurrently", elapsed)
	}
}

func TestLoopStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int64
	loop(ctx, 2, time.Hour, 1, func() {
		if n.Add(1) == 5 {
			cancel()
		}
	})
	if got := n.Load(); got > 10 {
		t.Fatalf("loop kept going after cancel, %d runs", got)
	}
}
