package tsdb

import (
	"fmt"

	"github.com/vshyshkovskyi/miniscope/internal/chunk"
)

// A Block is a sealed, immutable set of series — the in-process stand-in for a
// Parquet data file on object storage. Its Manifest is the handful of summary
// statistics an engine reads *before* deciding whether to open the file. This
// is the same idea Apache Iceberg stores per data file.
type Block struct {
	ID       string
	series   map[string]*memSeries
	Manifest Manifest
}

// Manifest holds the cheap metadata used for pruning: the block's time bounds
// plus, for every label, the lexicographic min/max of the values present. Real
// Iceberg keeps min/max (and null/value counts) for every column of every file.
type Manifest struct {
	MinT, MaxT int64
	Cols       map[string]ColStat // label name -> value range in this block
	NumSeries  int
}

type ColStat struct{ Min, Max string }

// BlockBuilder accumulates samples, then Seal()s them into an immutable Block
// with a computed manifest.
type BlockBuilder struct {
	id     string
	series map[string]*memSeries
}

func NewBlockBuilder(id string) *BlockBuilder {
	return &BlockBuilder{id: id, series: make(map[string]*memSeries)}
}

func (b *BlockBuilder) Append(labels Labels, t int64, v float64) {
	id := labels.String()
	s, ok := b.series[id]
	if !ok {
		s = &memSeries{labels: labels, chunk: chunk.NewXORChunk(), minT: t}
		b.series[id] = s
	}
	s.chunk.Append(t, v)
	s.maxT = t
}

// Seal computes the manifest (the min/max roll-up) and returns the immutable block.
func (b *BlockBuilder) Seal() *Block {
	return &Block{ID: b.id, series: b.series, Manifest: computeManifest(b.series)}
}

// computeManifest rolls a set of series up into the cheap min/max metadata used
// for pruning: block time bounds plus, per label, the lexicographic value range.
func computeManifest(series map[string]*memSeries) Manifest {
	m := Manifest{Cols: make(map[string]ColStat), MinT: 1<<63 - 1, MaxT: -1 << 63}
	for _, s := range series {
		m.NumSeries++
		if s.minT < m.MinT {
			m.MinT = s.minT
		}
		if s.maxT > m.MaxT {
			m.MaxT = s.maxT
		}
		for _, l := range s.labels {
			cs, ok := m.Cols[l.Name]
			if !ok {
				m.Cols[l.Name] = ColStat{Min: l.Value, Max: l.Value}
				continue
			}
			if l.Value < cs.Min {
				cs.Min = l.Value
			}
			if l.Value > cs.Max {
				cs.Max = l.Value
			}
			m.Cols[l.Name] = cs
		}
	}
	return m
}

// CanSkip reports whether the block can be pruned for this query using ONLY the
// manifest — no chunk is decoded, no series is touched. It returns a human
// reason for the demo.
//
// The rule is conservative: min/max can prove a value is ABSENT, never that it
// is present. So we only skip when the metadata guarantees no match. A surviving
// block may still turn out to hold nothing — a false positive, which is fine; a
// false negative (skipping a block that had a match) would be a correctness bug.
func (b *Block) CanSkip(sel Selector, mint, maxt int64) (bool, string) {
	return canSkip(b.Manifest, sel, mint, maxt)
}

// canSkip is the pruning test, shared by in-memory and on-disk blocks. It uses
// ONLY the manifest — no chunk is decoded.
func canSkip(m Manifest, sel Selector, mint, maxt int64) (bool, string) {
	// 1. Time pruning: do the block's time bounds overlap the query window?
	if m.MaxT < mint || m.MinT > maxt {
		return true, fmt.Sprintf("time bounds [%d,%d] miss query [%d,%d]",
			m.MinT, m.MaxT, mint, maxt)
	}
	// 2. Column pruning: for each equality matcher, is the wanted value outside
	//    the block's [min,max] for that label? Same min/max test, any column.
	for _, mt := range sel {
		cs, ok := m.Cols[mt.Name]
		if !ok {
			continue // label absent from manifest: be conservative, don't skip
		}
		if mt.Value < cs.Min || mt.Value > cs.Max {
			return true, fmt.Sprintf("%s=%q outside block range [%q..%q]",
				mt.Name, mt.Value, cs.Min, cs.Max)
		}
	}
	return false, ""
}

// scan decodes the (already non-pruned) block and returns matching samples.
func (b *Block) scan(sel Selector, mint, maxt int64) []SeriesResult {
	return scanSeries(b.series, sel, mint, maxt)
}

// scanSeries filters an in-memory series map by selector and time range,
// decoding chunks only for matching series.
func scanSeries(series map[string]*memSeries, sel Selector, mint, maxt int64) []SeriesResult {
	var out []SeriesResult
	for _, s := range series {
		if s.maxT < mint || s.minT > maxt || !sel.Matches(s.labels) {
			continue
		}
		var samples []Sample
		it := s.chunk.Iterator()
		for it.Next() {
			t, v := it.At()
			if t >= mint && t <= maxt {
				samples = append(samples, Sample{t, v})
			}
		}
		if len(samples) > 0 {
			out = append(out, SeriesResult{Labels: s.labels, Samples: samples})
		}
	}
	return out
}

// Table is an ordered set of blocks — the stand-in for an Iceberg table's data
// files. Query prunes first, then scans only the survivors, reporting what it
// skipped so the mechanism is visible.
type Table struct{ Blocks []*Block }

type QueryReport struct {
	Results   []SeriesResult
	Decisions []BlockDecision
	Scanned   int
	Pruned    int
}

type BlockDecision struct {
	BlockID string
	Skipped bool
	Reason  string
	Matches int
}

func (t *Table) Query(sel Selector, mint, maxt int64) QueryReport {
	var rep QueryReport
	for _, blk := range t.Blocks {
		if skip, reason := blk.CanSkip(sel, mint, maxt); skip {
			rep.Pruned++
			rep.Decisions = append(rep.Decisions, BlockDecision{blk.ID, true, reason, 0})
			continue
		}
		res := blk.scan(sel, mint, maxt)
		rep.Scanned++
		rep.Results = append(rep.Results, res...)
		rep.Decisions = append(rep.Decisions, BlockDecision{blk.ID, false, "scanned", len(res)})
	}
	return rep
}
