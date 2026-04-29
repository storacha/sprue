package bucket

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
)

// DefaultChunkSize is the chunk size used when callers don't supply one.
// 1 MiB matches typical UnixFS chunking and balances per-blob piri
// overhead against range-read granularity.
const DefaultChunkSize int64 = 1 << 20

// rawBlockPrefix produces CIDs for body chunks: CIDv1, raw codec (0x55),
// sha256 multihash. Chunks are opaque bytes — no IPLD links — so the raw
// codec is the natural fit.
var rawBlockPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1,
}

// putBody reads body bytes from r, splits at chunkSize, writes each chunk
// as a raw IPLD block to bs, and returns a Body record. The body's full
// sha256 is computed once during chunking and stored on the Body for use
// as the ETag.
func putBody(ctx context.Context, bs cbor.IpldBlockstore, r io.Reader, chunkSize int64) (Body, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	buf := make([]byte, chunkSize)
	bodyHasher := sha256.New()
	var chunks []cid.Cid
	var total int64

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			bodyHasher.Write(chunk)
			c, perr := putRawBlock(ctx, bs, chunk)
			if perr != nil {
				return Body{}, fmt.Errorf("put chunk: %w", perr)
			}
			chunks = append(chunks, c)
			total += int64(n)
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		return Body{}, fmt.Errorf("read body: %w", err)
	}

	return Body{
		Size:      total,
		ChunkSize: chunkSize,
		Chunks:    chunks,
		SHA256:    bodyHasher.Sum(nil),
	}, nil
}

func putRawBlock(ctx context.Context, bs cbor.IpldBlockstore, data []byte) (cid.Cid, error) {
	c, err := rawBlockPrefix.Sum(data)
	if err != nil {
		return cid.Undef, err
	}
	blk, err := block.NewBlockWithCid(data, c)
	if err != nil {
		return cid.Undef, err
	}
	if err := bs.Put(ctx, blk); err != nil {
		return cid.Undef, err
	}
	return c, nil
}

// openBody returns a reader over the full body.
func openBody(ctx context.Context, bs cbor.IpldBlockstore, body Body) io.ReadCloser {
	return &bodyReader{ctx: ctx, bs: bs, body: body, end: body.Size - 1}
}

// openBodyRange returns a reader over [start, end] inclusive of the body.
// Caller must ensure 0 <= start <= end <= Size-1.
func openBodyRange(ctx context.Context, bs cbor.IpldBlockstore, body Body, start, end int64) io.ReadCloser {
	cs := body.ChunkSize
	startChunk := int(start / cs)
	startOffset := start % cs
	return &bodyReader{
		ctx:        ctx,
		bs:         bs,
		body:       body,
		nextChunk:  startChunk,
		startOff:   startOffset,
		pos:        start,
		end:        end,
		havePartial: true,
	}
}

// bodyReader streams chunks lazily. It supports both whole-body and ranged
// reads via the same loop — only the initial offset and the inclusive end
// position differ.
type bodyReader struct {
	ctx  context.Context
	bs   cbor.IpldBlockstore
	body Body

	nextChunk   int
	startOff    int64 // offset into the first chunk we read
	havePartial bool  // whether startOff still applies to the next chunk read

	cur    []byte // currently materialized chunk bytes
	curOff int    // read position within cur

	pos int64 // current absolute byte position (next byte to return)
	end int64 // last byte to return (inclusive)
	err error
}

func (br *bodyReader) Read(p []byte) (int, error) {
	if br.err != nil {
		return 0, br.err
	}
	if br.pos > br.end {
		br.err = io.EOF
		return 0, io.EOF
	}

	if br.cur == nil || br.curOff >= len(br.cur) {
		if br.nextChunk >= len(br.body.Chunks) {
			br.err = io.EOF
			return 0, io.EOF
		}
		blk, err := br.bs.Get(br.ctx, br.body.Chunks[br.nextChunk])
		if err != nil {
			br.err = fmt.Errorf("read chunk %d: %w", br.nextChunk, err)
			return 0, br.err
		}
		br.cur = blk.RawData()
		br.curOff = 0
		br.nextChunk++
		if br.havePartial {
			br.curOff = int(br.startOff)
			br.havePartial = false
		}
	}

	// Don't read past the inclusive end position.
	remaining := br.end - br.pos + 1
	available := int64(len(br.cur) - br.curOff)
	want := int64(len(p))
	if want > available {
		want = available
	}
	if want > remaining {
		want = remaining
	}

	n := copy(p[:want], br.cur[br.curOff:br.curOff+int(want)])
	br.curOff += n
	br.pos += int64(n)
	return n, nil
}

func (br *bodyReader) Close() error { return nil }
