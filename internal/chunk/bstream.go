package chunk

import "io"

// bstream is a bit-addressable stream of bytes. It is the substrate for the
// Gorilla XOR encoding: the encoder needs to write individual bits (the
// delta-of-delta control bits, the XOR significant-bit windows) as well as
// whole bytes (the leading varints for the first sample). This is a trimmed,
// self-contained version of the Prometheus tsdb/chunkenc bit stream.
type bstream struct {
	stream []byte // the data, with the last byte partially filled
	count  uint8  // how many bits are still free in the last byte (0..8)
}

func (b *bstream) bytes() []byte { return b.stream }

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) - 1
	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}
	b.count--
}

// writeByte writes a full byte even when the stream is not byte-aligned, by
// splitting the byte across the current and next underlying byte.
func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) - 1
	// Fill the rest of the current byte with the high bits of byt.
	b.stream[i] |= byt >> (8 - b.count)
	b.stream = append(b.stream, 0)
	i++
	// Put the remaining low bits into the new byte.
	b.stream[i] = byt << b.count
}

// writeBits writes the low nbits bits of u, most-significant bit first.
func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= 64 - uint(nbits)
	for nbits >= 8 {
		b.writeByte(byte(u >> 56))
		u <<= 8
		nbits -= 8
	}
	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

type bstreamReader struct {
	stream []byte
	pos    int   // index of the next byte to load bits from
	buf    uint64 // bit buffer, valid bits live in the high end
	valid  uint8  // number of valid bits currently in buf
}

func newReader(b []byte) *bstreamReader { return &bstreamReader{stream: b} }

func (r *bstreamReader) readBit() (bit, error) {
	if r.valid == 0 {
		if !r.loadNextByte() {
			return zero, io.EOF
		}
	}
	r.valid--
	v := (r.buf >> r.valid) & 1
	return v == 1, nil
}

func (r *bstreamReader) readBits(nbits int) (uint64, error) {
	var out uint64
	for nbits > 0 {
		if r.valid == 0 {
			if !r.loadNextByte() {
				return 0, io.EOF
			}
		}
		take := nbits
		if uint8(take) > r.valid {
			take = int(r.valid)
		}
		r.valid -= uint8(take)
		bits := (r.buf >> r.valid) & ((1 << take) - 1)
		out = (out << take) | bits
		nbits -= take
	}
	return out, nil
}

func (r *bstreamReader) readByte() (byte, error) {
	v, err := r.readBits(8)
	return byte(v), err
}

func (r *bstreamReader) loadNextByte() bool {
	if r.pos >= len(r.stream) {
		return false
	}
	r.buf = uint64(r.stream[r.pos])
	r.valid = 8
	r.pos++
	return true
}
