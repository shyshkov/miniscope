// Command flush-demo proves the RAM-ceiling fix: it ingests far more data than
// fits comfortably in memory, flushing each time window to disk, and prints heap
// usage as volume grows. Resident memory stays flat (it tracks one window) while
// on-disk volume climbs without bound — the difference between Phase 1 (all in
// RAM) and Phase 2 (cheap object storage absorbs the volume).
package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

func heapMB() float64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / 1e6
}

func main() {
	dir, err := os.MkdirTemp("", "miniscope-db-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	const (
		series     = 2000
		days       = 3
		stepMillis = 15_000        // 15s scrape
		blockDur   = 3600_000      // 1h windows -> one block per hour
	)
	db, err := tsdb.Open(dir, blockDur)
	if err != nil {
		panic(err)
	}

	labels := make([]tsdb.Labels, series)
	for i := range labels {
		labels[i] = tsdb.NewLabels(map[string]string{
			"__name__": "http_request_duration_ms",
			"host":     fmt.Sprintf("host-%04d", i%500),
			"endpoint": fmt.Sprintf("/api/v%d", i%20),
		})
	}

	fmt.Printf("ingesting %d series x %d days @ %ds into %s\n\n", series, days, stepMillis/1000, dir)
	fmt.Printf("%-10s %12s %10s %12s %10s\n", "ingested", "heap(MB)", "blocks", "disk(MB)", "B/sample")
	fmt.Println("---------------------------------------------------------------")

	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	end := start + int64(days)*24*3600_000
	r := rand.New(rand.NewSource(11))
	vals := make([]float64, series)
	for i := range vals {
		vals[i] = 20 + r.Float64()*30
	}

	var ingested int64
	report := int64(0)
	for ts := start; ts < end; ts += stepMillis {
		for i := 0; i < series; i++ {
			vals[i] += r.NormFloat64() * 0.7
			db.Append(labels[i], ts, vals[i])
			ingested++
		}
		if ingested-report >= 10_000_000 {
			report = ingested
			_, _, nblocks, diskBytes := db.Stats()
			fmt.Printf("%-10d %12.1f %10d %12.1f %10.3f\n",
				ingested, heapMB(), nblocks, float64(diskBytes)/1e6,
				heapMB()*1e6/float64(ingested))
		}
	}
	db.Flush()

	headS, flushedS, nblocks, diskBytes := db.Stats()
	total := headS + flushedS
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("DONE: %d samples total | head(RAM)=%d flushed(disk)=%d\n", total, headS, flushedS)
	fmt.Printf("blocks on disk: %d | disk size: %.1f MB | final heap: %.1f MB\n",
		nblocks, float64(diskBytes)/1e6, heapMB())
	rawTB := float64(total) * 16 / 1e12
	fmt.Printf("raw-equivalent volume: %.4f TB held in %.1f MB RAM\n", rawTB, heapMB())

	// Query spanning many on-disk blocks: pruning still skips most of them.
	sel := tsdb.Selector{
		{Name: "__name__", Value: "http_request_duration_ms"},
		{Name: "host", Value: "host-0007"},
	}
	qStart := start + 26*3600_000 // day 2, 02:00
	qEnd := qStart + 2*3600_000
	t0 := time.Now()
	res, scanned, pruned := db.Query(sel, qStart, qEnd)
	fmt.Printf("\nquery host-0007 over 2h on day 2: %d series, blocks scanned=%d pruned=%d, %s\n",
		len(res), scanned, pruned, time.Since(t0))
	db.Close()
}
