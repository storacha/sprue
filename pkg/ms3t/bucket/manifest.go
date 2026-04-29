package bucket

import "github.com/ipfs/go-cid"

// ObjectManifest is the per-object metadata record stored as a CBOR block
// in the IPLD blockstore. The MST leaf for an object key points at this
// record's CID. The body bytes themselves live as raw IPLD blocks (codec
// 0x55) addressed by sha256 multihash; this manifest holds the ordered
// list of chunk CIDs.
type ObjectManifest struct {
	Key         string `cborgen:"k"`
	ContentType string `cborgen:"ct"`
	Created     int64  `cborgen:"t"`
	Body        Body   `cborgen:"b"`
}

// Body describes how the object's bytes are split into content-addressed
// chunks. ChunkSize is fixed across the object's chunks; the last chunk
// may be shorter than ChunkSize. Range arithmetic is direct: byte N lives
// in chunk index N/ChunkSize at offset N%ChunkSize.
type Body struct {
	Size      int64     `cborgen:"s"`
	ChunkSize int64     `cborgen:"cs"`
	Chunks    []cid.Cid `cborgen:"c"`
	SHA256    []byte    `cborgen:"h"` // full-body sha256, for ETag
}
