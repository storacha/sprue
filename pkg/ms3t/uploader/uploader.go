// Package uploader hands batches of IPLD blocks off to durable storage as
// CAR files. The interface separates submission (queueing a logical PUT's
// blocks) from flushing (forcing buffered work out), so a buffered
// implementation can amortize many small S3 ops into one larger upload
// without changing the caller's flow.
package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/storacha/sprue/pkg/ms3t/cars"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// Uploader is the seam between the bucket service and durable storage.
//
// Submit hands one logical batch of blocks (typically the result of a
// single S3 op) to the uploader along with the root CID(s) that
// summarize what was written. The implementation may flush immediately
// or buffer and flush later.
//
// Flush forces any buffered work out to durable storage. Callers use
// this for explicit boundaries (multipart Complete, shutdown) or
// recovery loops.
//
// Close flushes any remaining buffered work and releases resources
// (background goroutines, file handles, network clients).
type Uploader interface {
	Submit(ctx context.Context, roots []cid.Cid, blocks []block.Block) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

// === Disk ===

// Disk is a synchronous Uploader that writes one CAR file per Submit
// call into a directory. Useful for development, debugging, and as the
// inner sink of a Batched uploader.
type Disk struct {
	dir string

	mu    sync.Mutex
	count uint64 // unique suffix for files when collisions could occur
}

// NewDisk creates the target directory if needed and returns a Disk
// uploader.
func NewDisk(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("uploader: mkdir %s: %w", dir, err)
	}
	return &Disk{dir: dir}, nil
}

func (d *Disk) Submit(_ context.Context, roots []cid.Cid, blocks []block.Block) error {
	if len(roots) == 0 {
		return errors.New("uploader: at least one root required")
	}
	if len(blocks) == 0 {
		return nil
	}

	var carBuf bytes.Buffer
	if err := cars.Write(&carBuf, roots, blocks); err != nil {
		return fmt.Errorf("uploader: encode car: %w", err)
	}

	final := filepath.Join(d.dir, d.fileName(roots))
	tmp, err := os.CreateTemp(d.dir, ".tmp-*.car")
	if err != nil {
		return fmt.Errorf("uploader: tmpfile: %w", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(carBuf.Bytes()); err != nil {
		return fmt.Errorf("uploader: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("uploader: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("uploader: close: %w", err)
	}
	if err := os.Rename(tmpPath, final); err != nil {
		return fmt.Errorf("uploader: rename: %w", err)
	}
	committed = true
	return nil
}

func (d *Disk) Flush(context.Context) error { return nil }
func (d *Disk) Close(context.Context) error { return nil }

// fileName produces a human-recognizable filename for a CAR, derived
// from the first root and the total root count for multi-root batches.
func (d *Disk) fileName(roots []cid.Cid) string {
	first := roots[0].String()
	if len(roots) == 1 {
		return first + ".car"
	}
	d.mu.Lock()
	d.count++
	n := d.count
	d.mu.Unlock()
	return fmt.Sprintf("%s+%d-%d.car", first, len(roots)-1, n)
}

// === Noop ===

// Noop discards all submissions. Useful for tests/benchmarks.
type Noop struct{}

func (Noop) Submit(context.Context, []cid.Cid, []block.Block) error { return nil }
func (Noop) Flush(context.Context) error                            { return nil }
func (Noop) Close(context.Context) error                            { return nil }

// === Batched ===

// BatchedOptions configures a Batched uploader.
type BatchedOptions struct {
	// MaxBytes triggers a flush when the buffered block bytes exceed
	// this size. 0 → 64 MiB.
	MaxBytes int64
	// MaxAge triggers a flush when the time since the last submit
	// exceeds this duration. 0 → 5 seconds.
	MaxAge time.Duration
	// CheckInterval is how often the background loop wakes to evaluate
	// the time-based threshold. 0 → MaxAge / 4 (clamped to a minimum).
	CheckInterval time.Duration
}

func (o *BatchedOptions) defaults() {
	if o.MaxBytes <= 0 {
		o.MaxBytes = 64 << 20
	}
	if o.MaxAge <= 0 {
		o.MaxAge = 5 * time.Second
	}
	if o.CheckInterval <= 0 {
		o.CheckInterval = o.MaxAge / 4
		if o.CheckInterval < 100*time.Millisecond {
			o.CheckInterval = 100 * time.Millisecond
		}
	}
}

// Batched buffers Submit calls in memory and flushes them to an inner
// Uploader as one combined batch when a size or time threshold is hit.
// Multiple roots accumulate; the eventual CAR has all of them.
//
// Crash recovery: Batched does not persist its in-memory queue. If the
// process dies, blocks that were Submitted but not yet Flushed remain
// in the underlying blockstore (canonical) but were not shipped via
// the inner Uploader. Recovery is the responsibility of the caller —
// see bucket.Service.Recover.
type Batched struct {
	inner Uploader
	opts  BatchedOptions

	mu           sync.Mutex
	rootSet      map[cid.Cid]struct{}
	roots        []cid.Cid
	blockSet     map[cid.Cid]struct{}
	blocks       []block.Block
	pendingBytes int64
	lastSubmit   time.Time

	stop chan struct{}
	done chan struct{}
}

// NewBatched wraps inner with size+time-driven flushing.
func NewBatched(inner Uploader, opts BatchedOptions) *Batched {
	opts.defaults()
	b := &Batched{
		inner:    inner,
		opts:     opts,
		rootSet:  map[cid.Cid]struct{}{},
		blockSet: map[cid.Cid]struct{}{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go b.loop()
	return b
}

func (b *Batched) Submit(ctx context.Context, roots []cid.Cid, blocks []block.Block) error {
	if len(roots) == 0 {
		return errors.New("uploader: at least one root required")
	}

	b.mu.Lock()
	for _, r := range roots {
		if _, ok := b.rootSet[r]; !ok {
			b.rootSet[r] = struct{}{}
			b.roots = append(b.roots, r)
		}
	}
	for _, blk := range blocks {
		c := blk.Cid()
		if _, ok := b.blockSet[c]; !ok {
			b.blockSet[c] = struct{}{}
			b.blocks = append(b.blocks, blk)
			b.pendingBytes += int64(len(blk.RawData()))
		}
	}
	b.lastSubmit = time.Now()
	overSize := b.pendingBytes >= b.opts.MaxBytes
	b.mu.Unlock()

	if overSize {
		return b.Flush(ctx)
	}
	return nil
}

func (b *Batched) Flush(ctx context.Context) error {
	b.mu.Lock()
	if len(b.blocks) == 0 {
		b.mu.Unlock()
		return nil
	}
	roots := b.roots
	blocks := b.blocks
	b.roots = nil
	b.blocks = nil
	b.rootSet = map[cid.Cid]struct{}{}
	b.blockSet = map[cid.Cid]struct{}{}
	b.pendingBytes = 0
	b.mu.Unlock()

	return b.inner.Submit(ctx, roots, blocks)
}

func (b *Batched) Close(ctx context.Context) error {
	close(b.stop)
	<-b.done
	if err := b.Flush(ctx); err != nil {
		return err
	}
	return b.inner.Close(ctx)
}

func (b *Batched) loop() {
	defer close(b.done)
	ticker := time.NewTicker(b.opts.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.mu.Lock()
			shouldFlush := len(b.blocks) > 0 && !b.lastSubmit.IsZero() &&
				time.Since(b.lastSubmit) >= b.opts.MaxAge
			b.mu.Unlock()
			if shouldFlush {
				_ = b.Flush(context.Background())
			}
		}
	}
}

// === Compile-time assertions ===

var (
	_ Uploader = (*Disk)(nil)
	_ Uploader = Noop{}
	_ Uploader = (*Batched)(nil)
)
