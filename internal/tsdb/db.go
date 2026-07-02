package tsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/vshyshkovskyi/miniscope/internal/chunk"
)

// DB is the persistent engine: a single in-memory head holding the current time
// window, plus a list of immutable on-disk blocks for everything older. When the
// head's span reaches blockDur it is flushed to a block file and evicted from
// RAM, so resident memory tracks one window — not the total volume ingested.
// This is what lets ingestion run past the RAM ceiling, bounded only by disk.
type DB struct {
	mu       sync.Mutex
	dir      string
	blockDur int64 // window length in ms

	head        map[string]*memSeries
	headMinT    int64
	headHasData bool

	blocks         []*DiskBlock
	seq            int
	flushedSamples int64
	flushedBytes   int64
}

// Open creates/loads a DB rooted at dir. blockDur is the flush window in ms.
func Open(dir string, blockDur int64) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, blockDur: blockDur, head: make(map[string]*memSeries)}

	matches, _ := filepath.Glob(filepath.Join(dir, "block-*.msb"))
	sort.Strings(matches)
	for _, p := range matches {
		blk, err := openBlock(p)
		if err != nil {
			return nil, err
		}
		db.blocks = append(db.blocks, blk)
		db.seq++
	}
	return db, nil
}

// Append ingests one sample. When the sample crosses into the next window the
// current head is flushed and evicted before the sample lands.
func (db *DB) Append(labels Labels, t int64, v float64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.headHasData && t >= db.headMinT+db.blockDur {
		if err := db.flushLocked(); err != nil {
			return err
		}
	}
	id := labels.String()
	s, ok := db.head[id]
	if !ok {
		s = &memSeries{labels: labels, chunk: chunk.NewXORChunk(), minT: t}
		db.head[id] = s
	}
	s.chunk.Append(t, v)
	s.maxT = t
	if !db.headHasData {
		db.headMinT = t
		db.headHasData = true
	}
	return nil
}

// Flush forces the current head out to disk (e.g. on shutdown).
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if len(db.head) == 0 {
		return nil
	}
	path := filepath.Join(db.dir, fmt.Sprintf("block-%06d.msb", db.seq))
	n, err := writeBlockFile(path, db.head)
	if err != nil {
		return err
	}
	blk, err := openBlock(path)
	if err != nil {
		return err
	}
	for _, s := range db.head {
		db.flushedSamples += int64(s.chunk.NumSamples())
	}
	db.flushedBytes += n
	db.blocks = append(db.blocks, blk)
	db.seq++

	// Evict the window from RAM. The old map (and its chunks) become garbage;
	// only the small on-disk index for this block stays resident.
	db.head = make(map[string]*memSeries)
	db.headHasData = false
	return nil
}

// Query returns matching series across the head and every on-disk block,
// pruning blocks that cannot contain a match. blocksScanned/blocksPruned report
// how many on-disk blocks were actually read.
func (db *DB) Query(sel Selector, mint, maxt int64) (res []SeriesResult, blocksScanned, blocksPruned int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	res = append(res, scanSeries(db.head, sel, mint, maxt)...)
	for _, b := range db.blocks {
		r, scanned := b.query(sel, mint, maxt)
		if scanned {
			blocksScanned++
			res = append(res, r...)
		} else {
			blocksPruned++
		}
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Labels.String() < res[j].Labels.String() })
	return res, blocksScanned, blocksPruned
}

// Stats reports what is resident vs flushed.
func (db *DB) Stats() (headSamples, flushedSamples int64, numBlocks int, diskBytes int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, s := range db.head {
		headSamples += int64(s.chunk.NumSamples())
	}
	return headSamples, db.flushedSamples, len(db.blocks), db.flushedBytes
}

// Close releases all open block files.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, b := range db.blocks {
		b.Close()
	}
	return nil
}
