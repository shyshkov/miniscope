package chunk

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func roundTrip(t *testing.T, in []sample) {
	t.Helper()
	c := NewXORChunk()
	for _, s := range in {
		c.Append(s.t, s.v)
	}
	if c.NumSamples() != len(in) {
		t.Fatalf("num samples: got %d want %d", c.NumSamples(), len(in))
	}
	it := c.Iterator()
	var got []sample
	for it.Next() {
		ts, v := it.At()
		got = append(got, sample{ts, v})
	}
	if it.Err() != nil {
		t.Fatalf("iterator error: %v", it.Err())
	}
	if len(got) != len(in) {
		t.Fatalf("decoded %d samples, want %d", len(got), len(in))
	}
	for i := range in {
		if got[i].t != in[i].t || math.Float64bits(got[i].v) != math.Float64bits(in[i].v) {
			t.Fatalf("sample %d: got (%d,%v) want (%d,%v)", i, got[i].t, got[i].v, in[i].t, in[i].v)
		}
	}
}

func TestRegularScrape(t *testing.T) {
	var in []sample
	ts := int64(1_700_000_000_000)
	v := 42.0
	for i := 0; i < 1000; i++ {
		ts += 15_000 // perfectly regular 15s step -> dod == 0
		v += 0.5
		in = append(in, sample{ts, v})
	}
	roundTrip(t, in)
}

func TestJitteryAndFlat(t *testing.T) {
	var in []sample
	ts := int64(1_700_000_000_000)
	v := 100.0
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		ts += 15_000 + int64(r.Intn(2000)-1000) // jitter exercises the DOD branches
		if i%3 != 0 {
			v += float64(r.Intn(10)) - 5 // flat runs exercise the unchanged-value path
		}
		in = append(in, sample{ts, v})
	}
	roundTrip(t, in)
}

func TestEdgeValues(t *testing.T) {
	in := []sample{
		{1, 0},
		{2, math.MaxFloat64},
		{3, -math.MaxFloat64},
		{4, math.SmallestNonzeroFloat64},
		{5, math.Inf(1)},
		{6, math.Inf(-1)},
		{7, 0},
		{8, 1.0},
	}
	roundTrip(t, in)
}

// CompressionRatio is reported by `go test -v` so the codec's payoff is visible.
func TestCompressionRatio(t *testing.T) {
	c := NewXORChunk()
	ts := int64(1_700_000_000_000)
	v := 0.0
	const n = 10000
	for i := 0; i < n; i++ {
		ts += 15_000
		v += 1
		c.Append(ts, v)
	}
	raw := n * 16 // 8 bytes timestamp + 8 bytes float64, uncompressed
	got := len(c.Bytes())
	t.Logf("compressed %d samples: %d bytes vs %d raw (%.1fx, %.2f bytes/sample)",
		n, got, raw, float64(raw)/float64(got), float64(got)/float64(n))
}
