package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/xdearboy/prom-loadgen/collector"
)

func main() {
	var (
		engineName = flag.String("engine", "", "engine name")
		target     = flag.String("target", "", "engine base url")
		namespace  = flag.String("namespace", "prompp-bench", "namespace of the engine pod")
		nodeName   = flag.String("node", "", "node the engine pod runs on")
		runID      = flag.String("run-id", "", "run id")
		phase      = flag.String("phase", "ingest", "phase label")
		interval   = flag.Duration("interval", 5e9, "sampling interval")
		duration   = flag.Duration("duration", 0, "stop after this long, zero means run forever")
		podLabel   = flag.String("pod-label", "", "label selector for the engine pod")
		useCgroup  = flag.Bool("cgroup", true, "read cAdvisor metrics through the kubelet api proxy")
		outPath    = flag.String("out", "", "write jsonl here instead of stdout")
	)
	flag.Parse()

	if *engineName == "" || *target == "" {
		log.Fatal("both -engine and -target are required")
	}
	if *podLabel == "" {
		*podLabel = "app.kubernetes.io/instance=" + *engineName
	}

	cfg := collector.Config{
		Engine:    *engineName,
		Target:    *target,
		Namespace: *namespace,
		NodeName:  *nodeName,
		RunID:     *runID,
		Phase:     *phase,
		Interval:  *interval,
		Duration:  *duration,
		PodLabel:  *podLabel,
		UseCgroup: *useCgroup,
	}
	c := collector.New(cfg)

	out := os.Stdout
	if *outPath != "" {
		if dir := filepath.Dir(*outPath); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				log.Fatal(err)
			}
		}
		file, err := os.Create(*outPath)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		out = file
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "collecting %s every %s for %s\n", *engineName, *interval, *duration)
	if err := c.Run(ctx, out); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
