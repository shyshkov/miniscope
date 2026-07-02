package tsdb

import (
	"fmt"
	"testing"
)

func TestDBFlushQueryAndReopen(t *testing.T) {
	dir := t.TempDir()
	const blockDur = 3600_000 // 1h windows
	db, err := Open(dir, blockDur)
	if err != nil {
		t.Fatal(err)
	}

	// Ingest 5 hours of 1-minute samples for 3 hosts -> forces ~4 flushes.
	start := int64(1_700_000_000_000)
	for h := 0; h < 3; h++ {
		labels := NewLabels(map[string]string{"__name__": "cpu", "host": fmt.Sprintf("host-%d", h)})
		for i := 0; i < 300; i++ { // 300 minutes = 5h
			if err := db.Append(labels, start+int64(i)*60_000, float64(i+h)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Flush(); err != nil { // flush the trailing partial window
		t.Fatal(err)
	}

	_, flushed, nblocks, _ := db.Stats()
	if nblocks < 4 {
		t.Fatalf("expected several blocks, got %d", nblocks)
	}

	// Query host-1 over a 2-hour window that spans multiple on-disk blocks.
	sel := Selector{{Name: "__name__", Value: "cpu"}, {Name: "host", Value: "host-1"}}
	qStart := start + 90*60_000  // 01:30
	qEnd := start + 210*60_000   // 03:30
	res, scanned, pruned := db.Query(sel, qStart, qEnd)
	if len(res) != 1 {
		t.Fatalf("want 1 series, got %d", len(res))
	}
	if got := len(res[0].Samples); got != 121 { // inclusive 01:30..03:30
		t.Fatalf("want 121 samples, got %d", got)
	}
	if pruned == 0 {
		t.Fatalf("expected some blocks pruned by time, got scanned=%d pruned=%d", scanned, pruned)
	}
	db.Close()

	// Reopen from disk only (cold start) and re-run the query: durability check.
	db2, err := Open(dir, blockDur)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	res2, _, _ := db2.Query(sel, qStart, qEnd)
	if len(res2) != 1 || len(res2[0].Samples) != 121 {
		t.Fatalf("after reopen: want 1 series/121 samples, got %d series", len(res2))
	}
	_ = flushed
}
