package uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/storacha/sprue/pkg/ms3t/cars"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	block "github.com/ipfs/go-block-format"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-ucanto/did"
	guppyclient "github.com/storacha/guppy/pkg/client"
)

// placeholderCID is the smallest legal raw-codec CID with an
// identity-hashed two-byte payload. It mirrors guppy's internal
// PlaceholderCID and is used as the "root" for the
// ShardedDagIndexView and the SpaceIndexAdd invocation: the index's
// Content() field and SpaceIndexAdd's rootCID parameter aren't
// load-bearing for inner-CID lookups (per guppy's own usage), so
// instead of inventing a synthetic root for each multi-root CAR we
// just pass this placeholder through.
var placeholderCID = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})

// Guppy is an Uploader that ships each Submit's CAR to Forge via the
// guppy client, then uploads a per-CAR index and registers it with
// the indexing-service so individual inner CIDs become resolvable.
//
// One Submit produces three Forge round trips:
//
//  1. SpaceBlobAdd of the CAR     (one piri blob, multihash-keyed)
//  2. SpaceBlobAdd of the index   (a small CAR encoding the inner
//                                  CID → byte-range mappings)
//  3. SpaceIndexAdd               (registers the index → placeholder
//                                  root association with the indexer)
//
// Multi-root CARs ride as one logical batch. The index covers every
// inner block from every root; SpaceIndexAdd is called once per
// CAR, not once per root, since the rootCID parameter is treated as
// a placeholder by the upstream pattern.
//
// Synchronous: Submit blocks until all three calls have returned.
// Wrap in uploader.Batched if you want size/time-driven batching of
// multiple S3 ops into one CAR before each Submit fires.
type Guppy struct {
	client   *guppyclient.Client
	spaceDID did.DID
}

// GuppyConfig wires a *guppyclient.Client (already configured with
// connection, principal, and delegation proofs) plus the destination
// space DID into a Guppy uploader.
type GuppyConfig struct {
	Client   *guppyclient.Client
	SpaceDID did.DID
}

// NewGuppy constructs a Guppy uploader from a configured client.
func NewGuppy(cfg GuppyConfig) (*Guppy, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("uploader: guppy client is required")
	}
	if cfg.SpaceDID == (did.DID{}) {
		return nil, fmt.Errorf("uploader: space DID is required")
	}
	return &Guppy{client: cfg.Client, spaceDID: cfg.SpaceDID}, nil
}

func (g *Guppy) Submit(ctx context.Context, roots []cid.Cid, blocks []block.Block) error {
	if len(roots) == 0 {
		return fmt.Errorf("uploader: at least one root required")
	}
	if len(blocks) == 0 {
		return nil
	}

	// 1. Encode CAR + record each inner block's byte position.
	var carBuf bytes.Buffer
	positions, err := cars.WriteWithPositions(&carBuf, roots, blocks)
	if err != nil {
		return fmt.Errorf("uploader: encode car: %w", err)
	}
	carBytes := carBuf.Bytes()

	carDigest, err := multihash.Sum(carBytes, multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("uploader: hash car: %w", err)
	}

	// 2. Upload the CAR as one piri blob.
	if _, err := g.client.SpaceBlobAdd(ctx,
		bytes.NewReader(carBytes), g.spaceDID,
		guppyclient.WithPrecomputedDigest(carDigest, uint64(len(carBytes))),
	); err != nil {
		return fmt.Errorf("uploader: SpaceBlobAdd(car): %w", err)
	}

	// 3. Build a ShardedDagIndexView that points every inner CID at
	//    its slice of the CAR. Single shard (the CAR we just uploaded),
	//    placeholder content (see comment above placeholderCID).
	view := blobindex.NewShardedDagIndexView(cidlink.Link{Cid: placeholderCID}, 1)
	for _, p := range positions {
		view.SetSlice(carDigest, p.CID.Hash(), blobindex.Position{
			Offset: p.Offset,
			Length: p.Length,
		})
	}
	archReader, err := view.Archive()
	if err != nil {
		return fmt.Errorf("uploader: archive index: %w", err)
	}
	indexBytes, err := io.ReadAll(archReader)
	if err != nil {
		return fmt.Errorf("uploader: read archived index: %w", err)
	}
	indexDigest, err := multihash.Sum(indexBytes, multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("uploader: hash index: %w", err)
	}

	// 4. Upload the index as its own piri blob.
	if _, err := g.client.SpaceBlobAdd(ctx,
		bytes.NewReader(indexBytes), g.spaceDID,
		guppyclient.WithPrecomputedDigest(indexDigest, uint64(len(indexBytes))),
	); err != nil {
		return fmt.Errorf("uploader: SpaceBlobAdd(index): %w", err)
	}

	// 5. Register the index with the indexing-service. The index CID
	//    uses the CAR multicodec, since the index is itself a CAR
	//    (matching how guppy frames its blobs).
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	if err := g.client.SpaceIndexAdd(ctx,
		indexCID, uint64(len(indexBytes)), placeholderCID, g.spaceDID,
	); err != nil {
		return fmt.Errorf("uploader: SpaceIndexAdd: %w", err)
	}
	return nil
}

func (g *Guppy) Flush(context.Context) error { return nil }
func (g *Guppy) Close(context.Context) error { return nil }

var _ Uploader = (*Guppy)(nil)
