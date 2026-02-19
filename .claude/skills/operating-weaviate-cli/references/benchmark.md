# Benchmark Reference

Run QPS (Queries Per Second) benchmarks against Weaviate collections.

## Basic Benchmark
```bash
weaviate-cli benchmark qps --collection Movies --json
```

## Custom Benchmark
```bash
weaviate-cli benchmark qps \
  --collection Movies \
  --query-type hybrid \
  --max-duration 300 \
  --test-duration 10 \
  --warmup-duration 5 \
  --limit 10 \
  --consistency-level QUORUM \
  --json
```

## Fixed QPS Test
```bash
weaviate-cli benchmark qps --collection Movies --qps 100 --json
```

## Multi-Tenant Benchmark
```bash
weaviate-cli benchmark qps --collection Movies --tenant "Tenant-0" --json
```

## CSV Output with Graph
```bash
weaviate-cli benchmark qps \
  --collection Movies \
  --output csv \
  --generate-graph \
  --file-alias staging \
  --json
```

## Custom Query Terms
```bash
weaviate-cli benchmark qps \
  --collection Movies \
  --query-terms "science fiction" \
  --query-terms "romantic comedy" \
  --json
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--collection` | Movies | Collection to benchmark |
| `--query-type` | hybrid | Search type: hybrid, bm25, near_text |
| `--max-duration` | 300 | Maximum total test duration (seconds) |
| `--test-duration` | 10 | Duration of each test phase (seconds) |
| `--warmup-duration` | 5 | Warmup phase duration (seconds) |
| `--limit` | 10 | Results per query |
| `--consistency-level` | QUORUM | ONE, QUORUM, ALL |
| `--qps` | auto | Fixed QPS (auto-increments if not set) |
| `--concurrency` | auto | Concurrency level |
| `--latency-threshold` | 10000 | Max latency in ms before stopping |
| `--certainty` | false | Compute certainty percentiles |
| `--fail-on-timeout` | false | Fail if any request exceeds 10s timeout |
| `--output` | stdout | Output format: stdout, csv |
| `--generate-graph` | false | Generate graph (requires --output=csv) |
| `--file-alias` | none | Identifier for output filenames |
| `--tenant` | none | Tenant for multi-tenant benchmarks |
| `--query-terms` | default | Custom query terms (repeatable) |

## Prerequisites

1. Collection must exist with data
2. For `near_text` query type: collection must have a vectorizer
3. For `--tenant`: collection must be multi-tenant with the specified tenant in `hot`/`active` state
4. For `--generate-graph`: requires `--output=csv`

## Notes

- Without `--qps`, the benchmark auto-increments QPS until the latency threshold is hit or max duration is reached
- With `--qps N`, runs at a fixed rate for the specified duration
- The benchmark uses async operations for high throughput
- CSV output includes detailed per-phase metrics
