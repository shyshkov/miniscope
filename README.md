# miniscope

A tiny, from-scratch time-series metrics engine in Go. It demonstrates the
modern lakehouse-observability approach: store telemetry at **full fidelity** on
**cheap, columnar object storage**, then engineer the **query path**
(compression, pruning, indexes) to be fast enough to rival purpose-built TSDBs.

It's a learning/portfolio project — the goal is to actually *build* the core
internals (Gorilla compression, block pruning, flush-to-disk, remote-write
ingest), not just describe them.

## What works now

- **Gorilla compression** (`internal/chunk`): delta-of-delta timestamps + XOR'd
  float values, written through a bit-addressable stream. ~10× on regular
  counters, less on noisy gauges. Round-trip tested.
- **In-memory head block** (`internal/tsdb`): recent samples in Gorilla chunks,
  keyed by sorted labels (the "series ID"), like the Prometheus head.
- **Block + manifest pruning** (`internal/tsdb/block.go`): immutable blocks with
  a min/max manifest per column; queries skip whole blocks on time *or any
  label* without reading them — the Iceberg file-pruning mechanism.
- **Persistent DB with flush-to-disk** (`internal/tsdb/db.go`, `disk.go`): the
  head holds one time window; full windows are sealed to immutable block files
  on disk (= object storage) and **evicted from RAM**. Resident memory tracks
  one window, not total volume — so ingestion is bounded by disk, not memory.
  Queries span head + disk, pruning blocks via their on-disk manifest; survives
  process restart (cold reopen).
- **Ingestion + query service** (`internal/api`, `cmd/metricsd`): an HTTP
  service with a JSON ingest endpoint, a **Prometheus remote-write** endpoint
  (snappy + protobuf, hand-decoded in `internal/remotewrite`), and a JSON query
  endpoint with selector + time range.

The only external dependency is `github.com/golang/snappy`.

## Run it

```
go test ./...            # all packages: codec, remote-write round-trip, HTTP e2e
go run ./cmd/miniscope   # ingest 1000 series, report compression, query
go run ./cmd/prune-demo  # show block pruning (1 of 8 blocks read) with reasons
go run ./cmd/flush-demo  # flush-to-disk: heap stays ~flat while disk volume grows
go run ./cmd/metricsd    # run the service on :9090
```

Drive the service:

```
curl -XPOST :9090/write -d '{"series":[{"labels":{"__name__":"cpu","host":"h1"},
  "samples":[{"t":1000,"v":10},{"t":2000,"v":11.5}]}]}'
curl ':9090/query?match=__name__=cpu&match=host=h1'
```

A real Prometheus can ship to it directly:

```yaml
remote_write:
  - url: http://localhost:9090/api/v1/write
```

## What each component demonstrates

| Code | TSDB internal |
|---|---|
| `chunk/xor.go` | Gorilla encoding: XOR float window + delta-of-delta timestamps |
| `chunk/bstream.go` | bit-packing — how chunks actually get small |
| `tsdb/head.go` | in-memory head block / hot data |
| `tsdb/labels.go` | label set as series identity |
| `tsdb/block.go` | block manifest min/max pruning (time + any label column) |
| `tsdb/disk.go`, `db.go` | flush-to-disk, cold reopen, memory bounded by window |
| `remotewrite/` | hand-decoded Prometheus remote-write protobuf wire format |

## Roadmap

- **Real Parquet/Iceberg backend:** persist sealed blocks as Parquet files,
  optionally via `apache/iceberg-go`, and prune files via the manifest. The
  in-memory `Block` already models this.
- **Inverted index:** label → series postings for fast matching at high
  cardinality.
- **Benchmark:** synthetic load visualizing *blocks pruned vs scanned* and
  latency, quantifying the cost/latency trade-off.
