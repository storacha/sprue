package blockstore

import (
	"bytes"
	"context"
	"fmt"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// WalkReachable returns every block reachable from root in the given
// blockstore via IPLD links. DAG-CBOR blocks are scanned for child CIDs;
// raw blocks are leaves.
//
// Cycles are detected and not revisited. Block order is BFS by
// discovery — useful for CAR-friendly streaming (root first, then its
// direct children, etc.).
//
// Used by recovery: walking from a bucket's HEAD collects every
// structural block plus every body chunk reachable from any current
// ObjectManifest, which is exactly the set we want to ship to Forge.
func WalkReachable(ctx context.Context, bs cbor.IpldBlockstore, root cid.Cid) ([]block.Block, error) {
	if !root.Defined() {
		return nil, nil
	}

	visited := map[cid.Cid]struct{}{}
	var out []block.Block
	queue := []cid.Cid{root}

	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		if _, seen := visited[c]; seen {
			continue
		}
		visited[c] = struct{}{}

		blk, err := bs.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", c, err)
		}
		out = append(out, blk)

		// Only DAG-CBOR blocks have IPLD links to follow. Raw blocks
		// (codec 0x55) are body chunks — leaves of the DAG.
		if c.Prefix().Codec != cid.DagCBOR {
			continue
		}
		err = cbg.ScanForLinks(bytes.NewReader(blk.RawData()), func(child cid.Cid) {
			if _, seen := visited[child]; !seen {
				queue = append(queue, child)
			}
		})
		if err != nil {
			return nil, fmt.Errorf("scan links %s: %w", c, err)
		}
	}
	return out, nil
}
