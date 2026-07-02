// Package remotewrite implements just enough of the Prometheus remote-write
// protocol to ingest real Prometheus traffic: snappy-framed, protobuf-encoded
// WriteRequest messages.
//
// The on-wire schema (prometheus/prompb) we care about:
//
//	message WriteRequest { repeated TimeSeries timeseries = 1; }
//	message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//	message Label        { string name = 1; string value = 2; }
//	message Sample       { double value = 1; int64 timestamp = 2; }
//
// We decode the protobuf wire format by hand rather than pull in the protobuf
// runtime + prompb — the schema is tiny and frozen, and the wire format is
// worth knowing: each field is a varint tag of (field_number<<3 | wire_type),
// followed by a payload whose shape the wire_type selects.
package remotewrite

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Label struct{ Name, Value string }

type Sample struct {
	Value       float64
	TimestampMs int64
}

type TimeSeries struct {
	Labels  []Label
	Samples []Sample
}

// protobuf wire types we use.
const (
	wireVarint = 0 // ints, bools, enums
	wireI64    = 1 // fixed 64-bit (double)
	wireLen    = 2 // length-delimited (strings, sub-messages)
)

// Unmarshal decodes an (already snappy-decompressed) WriteRequest body.
func Unmarshal(b []byte) ([]TimeSeries, error) {
	var out []TimeSeries
	for len(b) > 0 {
		field, wt, rest, err := readTag(b)
		if err != nil {
			return nil, err
		}
		b = rest
		if field == 1 && wt == wireLen { // WriteRequest.timeseries
			msg, rest, err := readLen(b)
			if err != nil {
				return nil, err
			}
			b = rest
			ts, err := decodeTimeSeries(msg)
			if err != nil {
				return nil, err
			}
			out = append(out, ts)
		} else {
			b, err = skip(b, wt)
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func decodeTimeSeries(b []byte) (TimeSeries, error) {
	var ts TimeSeries
	for len(b) > 0 {
		field, wt, rest, err := readTag(b)
		if err != nil {
			return ts, err
		}
		b = rest
		switch {
		case field == 1 && wt == wireLen: // labels
			msg, rest, err := readLen(b)
			if err != nil {
				return ts, err
			}
			b = rest
			l, err := decodeLabel(msg)
			if err != nil {
				return ts, err
			}
			ts.Labels = append(ts.Labels, l)
		case field == 2 && wt == wireLen: // samples
			msg, rest, err := readLen(b)
			if err != nil {
				return ts, err
			}
			b = rest
			s, err := decodeSample(msg)
			if err != nil {
				return ts, err
			}
			ts.Samples = append(ts.Samples, s)
		default:
			b, err = skip(b, wt)
			if err != nil {
				return ts, err
			}
		}
	}
	return ts, nil
}

func decodeLabel(b []byte) (Label, error) {
	var l Label
	for len(b) > 0 {
		field, wt, rest, err := readTag(b)
		if err != nil {
			return l, err
		}
		b = rest
		if wt == wireLen {
			s, rest, err := readLen(b)
			if err != nil {
				return l, err
			}
			b = rest
			switch field {
			case 1:
				l.Name = string(s)
			case 2:
				l.Value = string(s)
			}
		} else {
			b, err = skip(b, wt)
			if err != nil {
				return l, err
			}
		}
	}
	return l, nil
}

func decodeSample(b []byte) (Sample, error) {
	var s Sample
	for len(b) > 0 {
		field, wt, rest, err := readTag(b)
		if err != nil {
			return s, err
		}
		b = rest
		switch {
		case field == 1 && wt == wireI64: // double value
			if len(b) < 8 {
				return s, fmt.Errorf("remotewrite: truncated double")
			}
			s.Value = math.Float64frombits(binary.LittleEndian.Uint64(b))
			b = b[8:]
		case field == 2 && wt == wireVarint: // timestamp (ms)
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return s, fmt.Errorf("remotewrite: bad timestamp varint")
			}
			s.TimestampMs = int64(v)
			b = b[n:]
		default:
			b, err = skip(b, wt)
			if err != nil {
				return s, err
			}
		}
	}
	return s, nil
}

func readTag(b []byte) (field int, wt int, rest []byte, err error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, nil, fmt.Errorf("remotewrite: bad tag varint")
	}
	return int(v >> 3), int(v & 0x7), b[n:], nil
}

func readLen(b []byte) (msg, rest []byte, err error) {
	l, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, nil, fmt.Errorf("remotewrite: bad length varint")
	}
	b = b[n:]
	if uint64(len(b)) < l {
		return nil, nil, fmt.Errorf("remotewrite: truncated length-delimited field")
	}
	return b[:l], b[l:], nil
}

func skip(b []byte, wt int) ([]byte, error) {
	switch wt {
	case wireVarint:
		_, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("remotewrite: bad varint to skip")
		}
		return b[n:], nil
	case wireI64:
		if len(b) < 8 {
			return nil, fmt.Errorf("remotewrite: truncated i64 to skip")
		}
		return b[8:], nil
	case wireLen:
		_, rest, err := readLen(b)
		return rest, err
	default:
		return nil, fmt.Errorf("remotewrite: unsupported wire type %d", wt)
	}
}

// Marshal encodes time series back into the WriteRequest wire format. Used by
// tests and the demo client to produce real remote-write payloads.
func Marshal(series []TimeSeries) []byte {
	var out []byte
	for _, ts := range series {
		out = appendTagLen(out, 1, encodeTimeSeries(ts)) // WriteRequest.timeseries
	}
	return out
}

func encodeTimeSeries(ts TimeSeries) []byte {
	var b []byte
	for _, l := range ts.Labels {
		b = appendTagLen(b, 1, encodeLabel(l))
	}
	for _, s := range ts.Samples {
		b = appendTagLen(b, 2, encodeSample(s))
	}
	return b
}

func encodeLabel(l Label) []byte {
	var b []byte
	b = appendTagLen(b, 1, []byte(l.Name))
	b = appendTagLen(b, 2, []byte(l.Value))
	return b
}

func encodeSample(s Sample) []byte {
	var b []byte
	b = appendTag(b, 1, wireI64)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(s.Value))
	b = appendTag(b, 2, wireVarint)
	b = binary.AppendUvarint(b, uint64(s.TimestampMs))
	return b
}

func appendTag(b []byte, field, wt int) []byte {
	return binary.AppendUvarint(b, uint64(field)<<3|uint64(wt))
}

func appendTagLen(b []byte, field int, payload []byte) []byte {
	b = appendTag(b, field, wireLen)
	b = binary.AppendUvarint(b, uint64(len(payload)))
	return append(b, payload...)
}
