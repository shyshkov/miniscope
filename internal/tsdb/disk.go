package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/vshyshkovskyi/miniscope/internal/chunk"
)

// On-disk block format (the stand-in for one Parquet file on object storage):
//
//	[4]  magic "MSB1"
//	[4]  index length (uint32 LE)
//	[..] index: JSON {manifest, series:[{labels, minT, maxT, count, off, len}]}
//	[..] data:  concatenated Gorilla chunk bytes, one run per series
//
// The index (manifest + per-series offsets) is small and read fully into RAM on
// open; the bulk — the compressed chunks — stays on disk and is read only for
// series that survive pruning. That separation is what lets memory stay flat
// while ingested volume grows without bound.

var blockMagic = [4]byte{'M', 'S', 'B', '1'}

type diskSeries struct {
	Labels Labels `json:"l"`
	MinT   int64  `json:"min"`
	MaxT   int64  `json:"max"`
	Count  int    `json:"n"`
	Offset int64  `json:"off"`
	Length int64  `json:"len"`
}

type blockIndex struct {
	Manifest Manifest     `json:"manifest"`
	Series   []diskSeries `json:"series"`
}

// writeBlockFile serializes a set of in-memory series to an immutable block file
// and returns the bytes written.
func writeBlockFile(path string, series map[string]*memSeries) (int64, error) {
	var data []byte
	entries := make([]diskSeries, 0, len(series))
	for _, s := range series {
		cb := s.chunk.Bytes()
		entries = append(entries, diskSeries{
			Labels: s.labels,
			MinT:   s.minT,
			MaxT:   s.maxT,
			Count:  s.chunk.NumSamples(),
			Offset: int64(len(data)),
			Length: int64(len(cb)),
		})
		data = append(data, cb...)
	}
	idxBytes, err := json.Marshal(blockIndex{Manifest: computeManifest(series), Series: entries})
	if err != nil {
		return 0, err
	}

	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var hdr [8]byte
	copy(hdr[:4], blockMagic[:])
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(idxBytes)))
	if _, err := f.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := f.Write(idxBytes); err != nil {
		return 0, err
	}
	if _, err := f.Write(data); err != nil {
		return 0, err
	}
	return int64(len(hdr)) + int64(len(idxBytes)) + int64(len(data)), nil
}

// DiskBlock is an opened block file. It holds only the index in memory; chunk
// bytes are read from disk on demand.
type DiskBlock struct {
	path      string
	f         *os.File
	idx       blockIndex
	dataStart int64
}

func openBlock(path string) (*DiskBlock, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	if [4]byte(hdr[:4]) != blockMagic {
		f.Close()
		return nil, fmt.Errorf("tsdb: %s: bad magic", path)
	}
	idxLen := binary.LittleEndian.Uint32(hdr[4:])
	idxBytes := make([]byte, idxLen)
	if _, err := io.ReadFull(f, idxBytes); err != nil {
		f.Close()
		return nil, err
	}
	var idx blockIndex
	if err := json.Unmarshal(idxBytes, &idx); err != nil {
		f.Close()
		return nil, err
	}
	return &DiskBlock{path: path, f: f, idx: idx, dataStart: int64(8 + idxLen)}, nil
}

func (d *DiskBlock) Close() error { return d.f.Close() }

// query prunes via the manifest, then reads chunk bytes only for surviving,
// matching series. scanned reports whether the block was read at all.
func (d *DiskBlock) query(sel Selector, mint, maxt int64) (res []SeriesResult, scanned bool) {
	if skip, _ := canSkip(d.idx.Manifest, sel, mint, maxt); skip {
		return nil, false
	}
	for _, e := range d.idx.Series {
		if e.MaxT < mint || e.MinT > maxt || !sel.Matches(e.Labels) {
			continue
		}
		buf := make([]byte, e.Length)
		if _, err := d.f.ReadAt(buf, d.dataStart+e.Offset); err != nil {
			continue
		}
		it := chunk.NewIterator(buf, e.Count)
		var samples []Sample
		for it.Next() {
			t, v := it.At()
			if t >= mint && t <= maxt {
				samples = append(samples, Sample{t, v})
			}
		}
		if len(samples) > 0 {
			res = append(res, SeriesResult{Labels: e.Labels, Samples: samples})
		}
	}
	return res, true
}
