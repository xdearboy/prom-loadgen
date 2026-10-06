# prom-loadgen

Deterministic remote write load generator, PromQL query benchmark and resource
collector for comparing Prometheus compatible engines.

    cmd/loadgen     ramp active series through remote write
    cmd/querybench  repeat a PromQL suite at a given concurrency, or dump result hashes
    cmd/collector   sample engine self metrics and cAdvisor into jsonl

Series are a pure function of the seed, so the first n series of a larger run
are the same as a run of n, and every engine receives identical samples when
`-epoch-ms` is pinned.

    loadgen -target http://engine:9090 -engine name -run-id id -epoch-ms 1767225600000 \
      -ramp 50000:300,200000:480 -out ingest.json

`results` holds the artifact schema the commands write.
