// Command prune-demo shows min/max pruning generalized from time to an arbitrary
// label column — the mechanism Iceberg uses to skip data files without reading
// them. It lays out blocks partitioned by (time window x host shard), runs a
// selective query, and prints which blocks were pruned and why.
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

func main() {
	const (
		metric     = "cpu_usage"
		hosts      = 50
		endpoints  = 4
		stepMillis = 60_000 // 1-minute samples
	)
	day := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	r := rand.New(rand.NewSource(7))

	// Partition like a real table: 4 six-hour time windows x 2 host shards.
	// Each (window, shard) pair becomes one block ~ one Parquet file.
	windows := []struct{ name string; start, end int64 }{
		{"00-06", day + 0*6*60*stepMillis, day + 1*6*60*stepMillis},
		{"06-12", day + 1*6*60*stepMillis, day + 2*6*60*stepMillis},
		{"12-18", day + 2*6*60*stepMillis, day + 3*6*60*stepMillis},
		{"18-24", day + 3*6*60*stepMillis, day + 4*6*60*stepMillis},
	}
	shards := []struct{ name string; lo, hi int }{
		{"A", 0, 24},  // host-00 .. host-24
		{"B", 25, 49}, // host-25 .. host-49
	}

	var table tsdb.Table
	for _, w := range windows {
		for _, sh := range shards {
			bb := tsdb.NewBlockBuilder(fmt.Sprintf("block[w=%s,shard=%s]", w.name, sh.name))
			for h := sh.lo; h <= sh.hi; h++ {
				for e := 0; e < endpoints; e++ {
					labels := tsdb.NewLabels(map[string]string{
						"__name__": metric,
						"host":     fmt.Sprintf("host-%02d", h),
						"endpoint": fmt.Sprintf("/api/v%d", e),
					})
					v := 20 + r.Float64()*40
					for ts := w.start; ts < w.end; ts += stepMillis {
						v += r.NormFloat64() * 0.7
						bb.Append(labels, ts, v)
					}
				}
			}
			table.Blocks = append(table.Blocks, bb.Seal())
		}
	}

	// Show the manifests — the cheap metadata pruning reads instead of the data.
	fmt.Println("=== block manifests (what the engine sees before reading) ===")
	for _, b := range table.Blocks {
		host := b.Manifest.Cols["host"]
		fmt.Printf("%-24s  t=[%s..%s]  host=[%s..%s]  series=%d\n",
			b.ID,
			time.UnixMilli(b.Manifest.MinT).UTC().Format("15:04"),
			time.UnixMilli(b.Manifest.MaxT).UTC().Format("15:04"),
			host.Min, host.Max, b.Manifest.NumSeries)
	}

	// Query: one host, a 2-hour window. Prunes on BOTH time and the host column.
	sel := tsdb.Selector{
		{Name: "__name__", Value: metric},
		{Name: "host", Value: "host-07"},
	}
	qStart := day + 8*60*stepMillis  // 08:00 -> only the 06-12 window overlaps
	qEnd := qStart + 120*stepMillis  // 10:00
	fmt.Printf("\n=== query: %s host=host-07, 08:00-10:00 ===\n", metric)

	t0 := time.Now()
	rep := table.Query(sel, qStart, qEnd)
	dur := time.Since(t0)

	for _, d := range rep.Decisions {
		if d.Skipped {
			fmt.Printf("  PRUNE  %-24s  reason: %s\n", d.BlockID, d.Reason)
		} else {
			fmt.Printf("  SCAN   %-24s  -> %d matching series\n", d.BlockID, d.Matches)
		}
	}

	fmt.Printf("\nblocks scanned:   %d of %d  (pruned %d without reading)\n",
		rep.Scanned, len(table.Blocks), rep.Pruned)
	fmt.Printf("series returned:  %d\n", len(rep.Results))
	fmt.Printf("query latency:    %s\n", dur)
	fmt.Println("\nnote: min/max proves ABSENCE, not presence — a scanned block may still")
	fmt.Println("hold no match (a false positive). Skipping a block that HAD a match would")
	fmt.Println("be a correctness bug, so pruning is always conservative.")
}
