// Package chunk implements Gorilla-style compression for time-series chunks:
// delta-of-delta encoding for timestamps and XOR encoding for float values.
// This is the encoding at the heart of Prometheus' TSDB and Facebook's Gorilla
// paper, and it is one of the most important internals in any TSDB.
//
// The intuition:
//   - Timestamps in a scrape are almost perfectly regular (e.g. every 15s), so
//     the *second* derivative (delta-of-delta) is usually 0 and costs 1 bit.
//   - Consecutive float values change slowly, so XOR with the previous value
//     yields a word with long runs of leading/trailing zero bits; we only store
//     the meaningful middle window.
package chunk

import (
	"math"
	"math/bits"
)

// XORChunk is an append-only, Gorilla-compressed sequence of (timestamp, value)
// samples. A single appender is embedded for simplicity (one writer at a time).
type XORChunk struct {
	b   bstream
	num uint16

	// appender carry-state
	t       int64
	v       float64
	tDelta  uint64
	leading uint8
	trailing uint8
}

// leadingNone marks "no XOR window established yet", forcing the first value
// delta to emit a fresh window.
const leadingNone = 0xff

func NewXORChunk() *XORChunk { return &XORChunk{leading: leadingNone} }

func (c *XORChunk) NumSamples() int { return int(c.num) }
func (c *XORChunk) Bytes() []byte   { return c.b.bytes() }

// Append adds a sample. Timestamps must be non-decreasing.
func (c *XORChunk) Append(t int64, v float64) {
	switch c.num {
	case 0:
		// First sample: store the timestamp as a varint and the raw value bits.
		c.writeVarint(t)
		c.b.writeBits(math.Float64bits(v), 64)
	case 1:
		// Second sample: store the first delta and a value-delta.
		c.tDelta = uint64(t - c.t)
		c.writeUvarint(c.tDelta)
		c.writeVDelta(v)
	default:
		// Steady state: delta-of-delta for time, value-delta for the value.
		tDelta := uint64(t - c.t)
		dod := int64(tDelta - c.tDelta)
		c.writeDOD(dod)
		c.tDelta = tDelta
		c.writeVDelta(v)
	}
	c.t = t
	c.v = v
	c.num++
}

// writeDOD encodes the delta-of-delta with a variable-length control prefix,
// spending fewer bits on the (very common) small deltas.
func (c *XORChunk) writeDOD(dod int64) {
	switch {
	case dod == 0:
		c.b.writeBit(zero)
	case fitsBits(dod, 14):
		c.b.writeBits(0b10, 2)
		c.b.writeBits(uint64(dod), 14)
	case fitsBits(dod, 17):
		c.b.writeBits(0b110, 3)
		c.b.writeBits(uint64(dod), 17)
	case fitsBits(dod, 20):
		c.b.writeBits(0b1110, 4)
		c.b.writeBits(uint64(dod), 20)
	default:
		c.b.writeBits(0b1111, 4)
		c.b.writeBits(uint64(dod), 64)
	}
}

func (c *XORChunk) writeVDelta(v float64) {
	vDelta := math.Float64bits(v) ^ math.Float64bits(c.v)
	if vDelta == 0 {
		// Value unchanged: a single 0 bit. The cheapest case, and extremely
		// common for flat gauges/counters between scrapes.
		c.b.writeBit(zero)
		return
	}
	c.b.writeBit(one)

	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))
	if leading >= 32 {
		leading = 31 // leading is stored in 5 bits, so clamp to 31
	}

	if c.leading != leadingNone && leading >= c.leading && trailing >= c.trailing {
		// Reuse the previous significant-bit window: 1 control bit + payload.
		c.b.writeBit(zero)
		c.b.writeBits(vDelta>>c.trailing, 64-int(c.leading)-int(c.trailing))
	} else {
		// New window: store leading-zero count (5b), significant-bit count (6b),
		// then the significant bits.
		c.leading, c.trailing = leading, trailing
		c.b.writeBit(one)
		c.b.writeBits(uint64(leading), 5)
		sig := 64 - int(leading) - int(trailing)
		c.b.writeBits(uint64(sig), 6) // sig==64 stores as 0; reader restores it
		c.b.writeBits(vDelta>>trailing, sig)
	}
}

func (c *XORChunk) writeVarint(x int64) {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	c.writeUvarint(ux)
}

func (c *XORChunk) writeUvarint(x uint64) {
	for x >= 0x80 {
		c.b.writeByte(byte(x) | 0x80)
		x >>= 7
	}
	c.b.writeByte(byte(x))
}

// fitsBits reports whether the signed value v fits in nbits two's-complement.
func fitsBits(v int64, nbits uint) bool {
	hi := int64(1) << (nbits - 1)
	return v >= -hi && v < hi
}

// XORIterator decodes an XORChunk back into (timestamp, value) samples.
type XORIterator struct {
	r    *bstreamReader
	num  uint16
	read uint16

	t        int64
	v        float64
	tDelta   uint64
	leading  uint8
	trailing uint8
	err      error
}

func (c *XORChunk) Iterator() *XORIterator {
	return &XORIterator{r: newReader(c.b.bytes()), num: c.num}
}

// NewIterator decodes raw chunk bytes (as returned by XORChunk.Bytes) holding
// numSamples samples. Used to read chunks back from on-disk blocks without
// reconstructing the appender.
func NewIterator(b []byte, numSamples int) *XORIterator {
	return &XORIterator{r: newReader(b), num: uint16(numSamples)}
}

func (it *XORIterator) At() (int64, float64) { return it.t, it.v }
func (it *XORIterator) Err() error           { return it.err }

func (it *XORIterator) Next() bool {
	if it.err != nil || it.read >= it.num {
		return false
	}
	switch it.read {
	case 0:
		t, err := it.readVarint()
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.r.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t, it.v = t, math.Float64frombits(v)
	case 1:
		td, err := it.readUvarint()
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = td
		it.t += int64(td)
		if !it.readValue() {
			return false
		}
	default:
		dod, ok := it.readDOD()
		if !ok {
			return false
		}
		it.tDelta = uint64(int64(it.tDelta) + dod)
		it.t += int64(it.tDelta)
		if !it.readValue() {
			return false
		}
	}
	it.read++
	return true
}

func (it *XORIterator) readDOD() (int64, bool) {
	// Count the leading 1-bits of the control prefix (max 4).
	var n uint8
	for n < 4 {
		b, err := it.r.readBit()
		if err != nil {
			it.err = err
			return 0, false
		}
		if b == zero {
			break
		}
		n++
	}
	var sz uint8
	switch n {
	case 0:
		return 0, true // dod == 0
	case 1:
		sz = 14
	case 2:
		sz = 17
	case 3:
		sz = 20
	case 4:
		sz = 64
	}
	raw, err := it.r.readBits(int(sz))
	if err != nil {
		it.err = err
		return 0, false
	}
	dod := int64(raw)
	if sz != 64 && raw > (1<<(sz-1)) { // sign-extend
		dod -= 1 << sz
	}
	return dod, true
}

func (it *XORIterator) readValue() bool {
	useful, err := it.r.readBit()
	if err != nil {
		it.err = err
		return false
	}
	if useful == zero {
		return true // value unchanged
	}
	newWindow, err := it.r.readBit()
	if err != nil {
		it.err = err
		return false
	}
	if newWindow == one {
		lead, err := it.r.readBits(5)
		if err != nil {
			it.err = err
			return false
		}
		sig, err := it.r.readBits(6)
		if err != nil {
			it.err = err
			return false
		}
		if sig == 0 {
			sig = 64
		}
		it.leading = uint8(lead)
		it.trailing = uint8(64 - lead - sig)
	}
	mbits := 64 - int(it.leading) - int(it.trailing)
	raw, err := it.r.readBits(mbits)
	if err != nil {
		it.err = err
		return false
	}
	vbits := math.Float64bits(it.v) ^ (raw << it.trailing)
	it.v = math.Float64frombits(vbits)
	return true
}

func (it *XORIterator) readUvarint() (uint64, error) {
	var x uint64
	var s uint
	for {
		b, err := it.r.readByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

func (it *XORIterator) readVarint() (int64, error) {
	ux, err := it.readUvarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}
