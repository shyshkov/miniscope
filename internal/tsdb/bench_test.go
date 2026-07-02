package tsdb

import (
	"fmt"
	"runtime"
	"testing"
)

// BenchmarkAppend measures sustained single-node ingest throughput into the
// in-memory head, so we can extrapolate to "GB/day" and find the bottleneck.
func BenchmarkAppend(b *testing.B) {
	const series = 10000
	labels := make([]Labels, series)
	for i := range labels {
		labels[i] = NewLabels(map[string]string{
			"__name__": "http_request_duration_ms",
			"host":     fmt.Sprintf("host-%04d", i%500),
			"endpoint": fmt.Sprintf("/api/v%d", i%20),
		})
	}
	h := NewHead()
	ts := int64(1_700_000_000_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := labels[i%series]
		h.Append(s, ts+int64(i/series)*15000, float64(i))
	}
	b.StopTimer()

	// Derived, human-facing numbers.
	secs := b.Elapsed().Seconds()
	if secs > 0 {
		persec := float64(b.N) / secs
		const wireBytesPerSample = 16 // 8B ts + 8B value, ignoring labels
		bytesPerDay := persec * wireBytesPerSample * 86400
		b.ReportMetric(persec, "samples/sec")
		b.ReportMetric(bytesPerDay/1e12, "TB/day")
	}
}

// TestMemoryFootprint shows that the head grows unbounded in RAM — the reason a
// terabyte cannot be ingested without flushing to object storage.
func TestMemoryFootprint(t *testing.T) {
	const series, perSeries = 2000, 5000
	h := NewHead()
	labels := make([]Labels, series)
	for i := range labels {
		labels[i] = NewLabels(map[string]string{"__name__": "m", "id": fmt.Sprintf("%05d", i)})
	}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ts := int64(1_700_000_000_000)
	for j := 0; j < perSeries; j++ {
		for i := 0; i < series; i++ {
			h.Append(labels[i], ts+int64(j)*15000, float64(j)+0.5)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)

	_, nSamples, comp := h.Stats()
	grew := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)
	t.Logf("%d samples held in RAM: heap grew %.1f MB, compressed chunks %.1f MB (%.1f B/sample held)",
		nSamples, float64(grew)/1e6, float64(comp)/1e6, float64(grew)/float64(nSamples))
}
