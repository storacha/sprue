package blockstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/storacha/sprue/pkg/ms3t/uploader"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

// CARBuffer is a per-S3-op IpldBlockstore that captures every Put — body
// chunks, MST nodes, ObjectManifests — in memory. On Commit it submits
// the entire batch to the configured Uploader (which may flush
// immediately or buffer further) and then flushes the blocks to the
// underlying canonical store.
//
// Reads check the in-memory buffer first and fall through to the
// underlying store on miss. This lets the MST's GetPointer recompute
// path Put a node and immediately re-Read it during the same op.
//
// Single-shot per session: create a CARBuffer at the start of an S3 op,
// Put any number of blocks, then call Commit(root) exactly once on
// success or Discard on failure.
//
// Safe for concurrent reads but writes are serialized within one
// session — the bucket service holds a per-bucket mutex around the
// whole flow.
type CARBuffer struct {
	underlying cbor.IpldBlockstore
	uploader   uploader.Uploader

	mu     sync.RWMutex
	blocks map[cid.Cid]block.Block
	order  []cid.Cid
}

// NewCARBuffer constructs a per-op buffer backed by underlying for
// reads and Submitting to up at Commit.
func NewCARBuffer(underlying cbor.IpldBlockstore, up uploader.Uploader) *CARBuffer {
	return &CARBuffer{
		underlying: underlying,
		uploader:   up,
		blocks:     map[cid.Cid]block.Block{},
	}
}

func (b *CARBuffer) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	b.mu.RLock()
	blk, ok := b.blocks[c]
	b.mu.RUnlock()
	if ok {
		return blk, nil
	}
	return b.underlying.Get(ctx, c)
}

func (b *CARBuffer) Put(_ context.Context, blk block.Block) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.blocks[blk.Cid()]; !exists {
		b.blocks[blk.Cid()] = blk
		b.order = append(b.order, blk.Cid())
	}
	return nil
}

// Commit submits the buffered blocks to the Uploader rooted at root,
// then flushes them to the underlying canonical store. Empties the
// buffer on success.
func (b *CARBuffer) Commit(ctx context.Context, root cid.Cid) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.order) == 0 {
		return nil
	}

	blks := make([]block.Block, len(b.order))
	for i, c := range b.order {
		blks[i] = b.blocks[c]
	}

	if err := b.uploader.Submit(ctx, []cid.Cid{root}, blks); err != nil {
		return fmt.Errorf("carbuffer: submit: %w", err)
	}

	for _, blk := range blks {
		if err := b.underlying.Put(ctx, blk); err != nil {
			return fmt.Errorf("carbuffer: flush %s: %w", blk.Cid(), err)
		}
	}

	b.blocks = map[cid.Cid]block.Block{}
	b.order = nil
	return nil
}

// Discard drops any buffered blocks without submitting or flushing
// them. Use this when the surrounding op has failed and in-flight
// blocks should be abandoned.
func (b *CARBuffer) Discard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocks = map[cid.Cid]block.Block{}
	b.order = nil
}

var _ cbor.IpldBlockstore = (*CARBuffer)(nil)
