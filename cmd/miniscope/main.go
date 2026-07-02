// Command miniscope is a runnable demo of the engine: it ingests synthetic
// telemetry into the Gorilla-compressed head block, runs a label selector over
// a time range, and reports the storage footprint — the core "full fidelity on
// cheap storage" thesis in miniature.
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

func main() {
	head := tsdb.NewHead()

	// Synthetic fleet: one metric, many hosts and endpoints -> many series.
	const (
		hosts       = 50
		endpoints   = 20
		samples     = 1440 // one day at 1-minute resolution
		stepMillis  = 60_000
		metricName  = "http_request_duration_ms"
	)
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	r := rand.New(rand.NewSource(42))

	for h := 0; h < hosts; h++ {
		for e := 0; e < endpoints; e++ {
			labels := tsdb.NewLabels(map[string]string{
				"__name__": metricName,
				"host":     fmt.Sprintf("host-%02d", h),
				"endpoint": fmt.Sprintf("/api/v%d", e),
			})
			v := 20 + r.Float64()*5
			ts := start
			for i := 0; i < samples; i++ {
				v += r.NormFloat64() * 0.8 // slow random walk -> XOR-friendly
				if v < 1 {
					v = 1
				}
				head.Append(labels, ts, v)
				ts += stepMillis
			}
		}
	}

	numSeries, numSamples, compBytes := head.Stats()
	raw := numSamples * 16
	fmt.Println("=== ingest ===")
	fmt.Printf("series:           %d\n", numSeries)
	fmt.Printf("samples:          %d\n", numSamples)
	fmt.Printf("raw size:         %.2f MB (16 B/sample)\n", float64(raw)/1e6)
	fmt.Printf("gorilla size:     %.2f MB (%.2f B/sample)\n", float64(compBytes)/1e6, float64(compBytes)/float64(numSamples))
	fmt.Printf("compression:      %.1fx\n\n", float64(raw)/float64(compBytes))

	// Query: one host, a 2-hour window. The time-overlap prune skips the bulk
	// of series before any chunk is decoded.
	sel := tsdb.Selector{
		{Name: "__name__", Value: metricName},
		{Name: "host", Value: "host-07"},
	}
	qStart := start + 8*60*stepMillis  // 08:00
	qEnd := qStart + 120*stepMillis    // 10:00

	t0 := time.Now()
	res := head.Query(sel, qStart, qEnd)
	dur := time.Since(t0)

	fmt.Println("=== query host-07, 08:00-10:00 ===")
	fmt.Printf("matched series:   %d\n", len(res))
	fmt.Printf("query latency:    %s\n", dur)
	if len(res) > 0 {
		s := res[0]
		fmt.Printf("example series:   %s\n", s.Labels)
		fmt.Printf("points returned:  %d\n", len(s.Samples))
		if len(s.Samples) >= 3 {
			fmt.Printf("first 3 points:   ")
			for _, p := range s.Samples[:3] {
				fmt.Printf("(%s=%.2f) ", time.UnixMilli(p.T).UTC().Format("15:04"), p.V)
			}
			fmt.Println()
		}
	}
}
