// Package bucket implements S3-style CRUD operations on top of the forked
// MST (one tree per bucket) and an IPLD blockstore that holds both the
// structural blocks (MST nodes, ObjectManifests) and the raw body chunks.
package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/storacha/sprue/pkg/ms3t/blockstore"
	"github.com/storacha/sprue/pkg/ms3t/mst"
	"github.com/storacha/sprue/pkg/ms3t/registry"
	"github.com/storacha/sprue/pkg/ms3t/uploader"
)

// Service is the entry point for bucket operations.
type Service struct {
	bs       cbor.IpldBlockstore
	cst      cbor.IpldStore // long-lived, read-only over bs (for List/Head)
	reg      registry.Registry
	uploader uploader.Uploader

	chunkSize int64

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// Options configures a Service. Zero-valued options take sensible defaults.
type Options struct {
	// ChunkSize is the body chunk size for new objects. 0 → DefaultChunkSize.
	ChunkSize int64
	// Uploader receives a CAR per mutation (PutObject, DeleteObject)
	// containing the structural blocks created by that op. nil →
	// uploader.Noop (CARs are dropped on the floor).
	Uploader uploader.Uploader
}

// New wires the dependencies into a Service. The blockstore can be
// any cbor.IpldBlockstore implementation; sprue's fx wiring chooses
// either a SQLite-backed local store (default) or a Forge-backed
// pass-through store (no_cache mode).
func New(bs cbor.IpldBlockstore, reg registry.Registry, opt Options) *Service {
	cs := opt.ChunkSize
	if cs <= 0 {
		cs = DefaultChunkSize
	}
	up := opt.Uploader
	if up == nil {
		up = uploader.Noop{}
	}
	return &Service{
		bs:        bs,
		cst:       mst.CborStore(bs),
		reg:       reg,
		uploader:  up,
		chunkSize: cs,
		locks:     map[string]*sync.Mutex{},
	}
}

func (s *Service) bucketLock(name string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.locks[name]; ok {
		return m
	}
	m := &sync.Mutex{}
	s.locks[name] = m
	return m
}

var (
	ErrBucketExists   = registry.ErrExists
	ErrBucketNotFound = registry.ErrNotFound
	ErrObjectNotFound = errors.New("bucket: object not found")
	ErrInvalidKey     = errors.New("bucket: invalid object key")
	ErrInvalidBucket  = errors.New("bucket: invalid bucket name")
	ErrBucketNotEmpty = errors.New("bucket: bucket not empty")
	ErrInvalidRange   = errors.New("bucket: invalid range")
)

// === Bucket lifecycle ===

func (s *Service) CreateBucket(ctx context.Context, name string) error {
	if !validBucketName(name) {
		return ErrInvalidBucket
	}
	return s.reg.Create(ctx, name, time.Now().Unix())
}

func (s *Service) ListBuckets(ctx context.Context) ([]*registry.State, error) {
	return s.reg.List(ctx)
}

// === Forge sync (recovery + shutdown) ===

// Recover ensures every bucket's current root has been shipped to the
// Uploader. For each bucket, if the persisted ForgeRoot does not equal
// the current Root, walks the entire DAG reachable from Root, submits
// the blocks, flushes the Uploader, and advances ForgeRoot.
//
// Intended to be called once at startup, before the HTTP listener
// begins serving. Idempotent: if everything is already in sync, it's
// a fast scan and a no-op flush.
func (s *Service) Recover(ctx context.Context) error {
	states, err := s.reg.List(ctx)
	if err != nil {
		return fmt.Errorf("recover: list buckets: %w", err)
	}

	var dirty []*registry.State
	for _, st := range states {
		if !st.Root.Defined() {
			continue
		}
		if st.ForgeRoot.Defined() && st.ForgeRoot.Equals(st.Root) {
			continue
		}
		blocks, err := blockstore.WalkReachable(ctx, s.bs, st.Root)
		if err != nil {
			return fmt.Errorf("recover %q: walk: %w", st.Name, err)
		}
		if len(blocks) == 0 {
			continue
		}
		if err := s.uploader.Submit(ctx, []cid.Cid{st.Root}, blocks); err != nil {
			return fmt.Errorf("recover %q: submit: %w", st.Name, err)
		}
		dirty = append(dirty, st)
	}

	if len(dirty) == 0 {
		return nil
	}
	if err := s.uploader.Flush(ctx); err != nil {
		return fmt.Errorf("recover: flush: %w", err)
	}
	for _, st := range dirty {
		if err := s.reg.SetForgeRoot(ctx, st.Name, st.Root); err != nil {
			return fmt.Errorf("recover %q: set forge root: %w", st.Name, err)
		}
	}
	return nil
}

// Shutdown cleanly drains the Uploader and advances ForgeRoot to the
// current Root for every bucket. After Shutdown returns successfully,
// a subsequent Recover at the next startup is a no-op.
func (s *Service) Shutdown(ctx context.Context) error {
	if err := s.uploader.Close(ctx); err != nil {
		return fmt.Errorf("shutdown: close uploader: %w", err)
	}
	states, err := s.reg.List(ctx)
	if err != nil {
		return fmt.Errorf("shutdown: list buckets: %w", err)
	}
	for _, st := range states {
		if !st.Root.Defined() {
			continue
		}
		if st.ForgeRoot.Defined() && st.ForgeRoot.Equals(st.Root) {
			continue
		}
		if err := s.reg.SetForgeRoot(ctx, st.Name, st.Root); err != nil {
			return fmt.Errorf("shutdown %q: set forge root: %w", st.Name, err)
		}
	}
	return nil
}

func (s *Service) DeleteBucket(ctx context.Context, name string) error {
	lock := s.bucketLock(name)
	lock.Lock()
	defer lock.Unlock()

	st, err := s.reg.Get(ctx, name)
	if err != nil {
		return err
	}
	if st.Root.Defined() {
		t := mst.LoadMST(s.cst, st.Root)
		var seen bool
		walkErr := t.WalkLeavesFromNocache(ctx, "", func(string, cid.Cid) error {
			seen = true
			return mst.ErrStopWalk
		})
		if walkErr != nil {
			return fmt.Errorf("bucket: scan empty: %w", walkErr)
		}
		if seen {
			return ErrBucketNotEmpty
		}
	}
	return s.reg.Delete(ctx, name)
}

// === Object operations ===

// PutObject stores body under (bucket, key), creating or replacing as
// needed. Body bytes are chunked into raw IPLD blocks (written directly
// to the underlying blockstore). Structural blocks (manifest + mutated
// MST nodes) are captured into a per-op CARBuffer and emitted as a
// single CAR via the configured Uploader at commit time.
func (s *Service) PutObject(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*ObjectManifest, error) {
	if !mst.IsValidKey(key) {
		return nil, ErrInvalidKey
	}

	lock := s.bucketLock(bucket)
	lock.Lock()
	defer lock.Unlock()

	st, err := s.reg.Get(ctx, bucket)
	if err != nil {
		return nil, err
	}

	// All blocks for this PUT — body chunks, manifest, MST mutation path —
	// flow through the same per-op CARBuffer. The Uploader receives one
	// Submit per S3 op containing every block reachable from the new root,
	// individually addressable inside the resulting CAR via the indexer.
	buf := blockstore.NewCARBuffer(s.bs, s.uploader)

	bodyRec, err := putBody(ctx, buf, body, s.chunkSize)
	if err != nil {
		buf.Discard()
		return nil, fmt.Errorf("bucket: chunk body: %w", err)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	mf := &ObjectManifest{
		Key:         key,
		ContentType: contentType,
		Created:     time.Now().Unix(),
		Body:        bodyRec,
	}

	opCst := mst.CborStore(buf)

	mfCid, err := opCst.Put(ctx, mf)
	if err != nil {
		buf.Discard()
		return nil, fmt.Errorf("bucket: manifest put: %w", err)
	}

	t := loadOrEmpty(opCst, st.Root)
	t2, err := t.Add(ctx, key, mfCid, -1)
	if errors.Is(err, mst.ErrAlreadyExists) {
		t2, err = t.Update(ctx, key, mfCid)
	}
	if err != nil {
		buf.Discard()
		return nil, fmt.Errorf("bucket: mst write: %w", err)
	}

	newRoot, err := t2.GetPointer(ctx)
	if err != nil {
		buf.Discard()
		return nil, fmt.Errorf("bucket: mst pointer: %w", err)
	}

	if err := buf.Commit(ctx, newRoot); err != nil {
		return nil, fmt.Errorf("bucket: car commit: %w", err)
	}

	if err := s.reg.CASRoot(ctx, bucket, st.Root, newRoot); err != nil {
		return nil, fmt.Errorf("bucket: advance root: %w", err)
	}
	return mf, nil
}

// GetObject opens the body and returns the manifest. Caller must Close.
// If rng is non-nil, returns a reader over the requested byte range.
func (s *Service) GetObject(ctx context.Context, bucket, key string, rng *Range) (io.ReadCloser, *ObjectManifest, error) {
	mf, err := s.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	if rng == nil {
		return openBody(ctx, s.bs, mf.Body), mf, nil
	}
	if err := rng.resolve(mf.Body.Size); err != nil {
		return nil, mf, err
	}
	return openBodyRange(ctx, s.bs, mf.Body, rng.Start, rng.End), mf, nil
}

// HeadObject returns just the manifest.
func (s *Service) HeadObject(ctx context.Context, bucket, key string) (*ObjectManifest, error) {
	st, err := s.reg.Get(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !st.Root.Defined() {
		return nil, ErrObjectNotFound
	}
	t := mst.LoadMST(s.cst, st.Root)
	mfCid, err := t.Get(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("bucket: mst get: %w", err)
	}
	var mf ObjectManifest
	if err := s.cst.Get(ctx, mfCid, &mf); err != nil {
		return nil, fmt.Errorf("bucket: manifest get: %w", err)
	}
	return &mf, nil
}

// DeleteObject removes a key from the bucket. Missing keys return nil
// (matching S3's idempotent DELETE semantics). Body chunks are NOT
// deleted from the blockstore; GC is a future, separate pass over live
// manifests.
func (s *Service) DeleteObject(ctx context.Context, bucket, key string) error {
	lock := s.bucketLock(bucket)
	lock.Lock()
	defer lock.Unlock()

	st, err := s.reg.Get(ctx, bucket)
	if err != nil {
		return err
	}
	if !st.Root.Defined() {
		return nil
	}

	buf := blockstore.NewCARBuffer(s.bs, s.uploader)
	opCst := mst.CborStore(buf)

	t := mst.LoadMST(opCst, st.Root)
	t2, err := t.Delete(ctx, key)
	if errors.Is(err, mst.ErrNotFound) {
		buf.Discard()
		return nil
	}
	if err != nil {
		buf.Discard()
		return fmt.Errorf("bucket: mst delete: %w", err)
	}

	newRoot, err := t2.GetPointer(ctx)
	if err != nil {
		buf.Discard()
		return fmt.Errorf("bucket: mst pointer: %w", err)
	}
	if err := buf.Commit(ctx, newRoot); err != nil {
		return fmt.Errorf("bucket: car commit: %w", err)
	}
	if err := s.reg.CASRoot(ctx, bucket, st.Root, newRoot); err != nil {
		return fmt.Errorf("bucket: advance root: %w", err)
	}
	return nil
}

// === Range support ===

// Range describes an inclusive byte range, matching HTTP Range semantics.
//
// To support the open-ended ("bytes=N-") and suffix ("bytes=-N") forms
// without forcing the HTTP layer to do a separate HEAD before the GET,
// callers may set sentinel values:
//   - Start = -1 means "the last End bytes" (suffix form)
//   - End   = -1 means "from Start to the end of the object"
//
// resolve() is called by the service once the body size is known.
type Range struct {
	Start int64
	End   int64
}

// resolve fills in any sentinel values (-1) using size and validates the
// resulting range. Returns ErrInvalidRange if the range is unsatisfiable.
func (r *Range) resolve(size int64) error {
	if size <= 0 {
		return ErrInvalidRange
	}
	switch {
	case r.Start < 0 && r.End >= 0:
		// suffix: last End bytes
		if r.End == 0 {
			return ErrInvalidRange
		}
		if r.End > size {
			r.End = size
		}
		r.Start = size - r.End
		r.End = size - 1
	case r.Start >= 0 && r.End < 0:
		// open-ended
		r.End = size - 1
	}
	if r.Start < 0 || r.End < r.Start || r.End >= size {
		return ErrInvalidRange
	}
	return nil
}

// === Listing ===

type ListResult struct {
	Objects        []*ObjectManifest
	CommonPrefixes []string
	Truncated      bool
	NextToken      string
}

type ListOptions struct {
	Prefix     string
	Delimiter  string
	StartAfter string
	MaxKeys    int
}

const defaultMaxKeys = 1000

func (s *Service) List(ctx context.Context, bucket string, opt ListOptions) (*ListResult, error) {
	if opt.MaxKeys <= 0 {
		opt.MaxKeys = defaultMaxKeys
	}

	st, err := s.reg.Get(ctx, bucket)
	if err != nil {
		return nil, err
	}
	res := &ListResult{}
	if !st.Root.Defined() {
		return res, nil
	}

	t := mst.LoadMST(s.cst, st.Root)

	from := opt.Prefix
	if opt.StartAfter != "" && opt.StartAfter > from {
		from = opt.StartAfter + "\x01"
	}

	seenPrefix := map[string]struct{}{}
	walkErr := t.WalkLeavesFromNocache(ctx, from, func(k string, mfCid cid.Cid) error {
		if opt.Prefix != "" && !strings.HasPrefix(k, opt.Prefix) {
			return mst.ErrStopWalk
		}

		if opt.Delimiter != "" {
			tail := k[len(opt.Prefix):]
			if i := strings.Index(tail, opt.Delimiter); i >= 0 {
				cp := opt.Prefix + tail[:i+len(opt.Delimiter)]
				if _, dup := seenPrefix[cp]; !dup {
					seenPrefix[cp] = struct{}{}
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
					if len(res.Objects)+len(res.CommonPrefixes) >= opt.MaxKeys {
						res.Truncated = true
						res.NextToken = cp
						return mst.ErrStopWalk
					}
				}
				return nil
			}
		}

		var mf ObjectManifest
		if err := s.cst.Get(ctx, mfCid, &mf); err != nil {
			return fmt.Errorf("manifest get %s: %w", mfCid, err)
		}
		res.Objects = append(res.Objects, &mf)

		if len(res.Objects)+len(res.CommonPrefixes) >= opt.MaxKeys {
			res.Truncated = true
			res.NextToken = k
			return mst.ErrStopWalk
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("bucket: walk: %w", walkErr)
	}
	return res, nil
}

// === Internal helpers ===

func loadOrEmpty(cst cbor.IpldStore, root cid.Cid) *mst.MerkleSearchTree {
	if root.Defined() {
		return mst.LoadMST(cst, root)
	}
	return mst.NewEmptyMST(cst)
}

func validBucketName(s string) bool {
	if len(s) < 3 || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '.':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
