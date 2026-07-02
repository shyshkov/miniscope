package tsdb

import (
	"sort"
	"sync"

	"github.com/vshyshkovskyi/miniscope/internal/chunk"
)

// Head is the in-memory block holding the most recent samples, exactly like the
// Prometheus head block. New samples land here in Gorilla-compressed chunks;
// later phases flush full chunks to immutable Parquet blocks on object storage.
type Head struct {
	mu     sync.RWMutex
	series map[string]*memSeries
}

type memSeries struct {
	labels Labels
	chunk  *chunk.XORChunk
	minT   int64
	maxT   int64
}

func NewHead() *Head {
	return &Head{series: make(map[string]*memSeries)}
}

// Append adds a sample to the series identified by labels, creating it on first
// sight. Timestamps for a given series must be non-decreasing.
func (h *Head) Append(labels Labels, t int64, v float64) {
	id := labels.String()
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[id]
	if !ok {
		s = &memSeries{labels: labels, chunk: chunk.NewXORChunk(), minT: t}
		h.series[id] = s
	}
	s.chunk.Append(t, v)
	s.maxT = t
}

// Sample is a decoded point returned by a query.
type Sample struct {
	T int64
	V float64
}

// SeriesResult is the set of samples for one matched series within a time range.
type SeriesResult struct {
	Labels  Labels
	Samples []Sample
}

// Query returns all series matching sel whose samples fall in [mint, maxt].
// Series whose time bounds do not overlap the range are skipped without
// decoding — the in-memory analogue of the block pruning that real object-store
// reads rely on.
func (h *Head) Query(sel Selector, mint, maxt int64) []SeriesResult {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var out []SeriesResult
	for _, s := range h.series {
		if s.maxT < mint || s.minT > maxt { // time-overlap prune
			continue
		}
		if !sel.Matches(s.labels) {
			continue
		}
		var samples []Sample
		it := s.chunk.Iterator()
		for it.Next() {
			t, v := it.At()
			if t < mint || t > maxt {
				continue
			}
			samples = append(samples, Sample{t, v})
		}
		if len(samples) > 0 {
			out = append(out, SeriesResult{Labels: s.labels, Samples: samples})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.String() < out[j].Labels.String() })
	return out
}

// Stats summarizes the head for the demo/benchmark output.
func (h *Head) Stats() (numSeries, numSamples, compressedBytes int) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, s := range h.series {
		numSeries++
		numSamples += s.chunk.NumSamples()
		compressedBytes += len(s.chunk.Bytes())
	}
	return
}
