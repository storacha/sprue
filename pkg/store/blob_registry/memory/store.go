package memory

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/store"
	blobregistry "github.com/storacha/sprue/pkg/store/blob_registry"
)

type Store struct {
	mutex sync.RWMutex
	blobs map[did.DID][]blobregistry.Record
}

var _ blobregistry.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		blobs: map[did.DID][]blobregistry.Record{},
	}
}

func (s *Store) Get(ctx context.Context, space did.DID, digest multihash.Multihash) (blobregistry.Record, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for _, ent := range s.blobs[space] {
		if bytes.Equal(ent.Blob.Digest, digest) {
			return ent, nil
		}
	}
	return blobregistry.Record{}, blobregistry.ErrEntryNotFound
}

func (s *Store) Add(ctx context.Context, space did.DID, blob types.Blob, cause cid.Cid) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, ent := range s.blobs[space] {
		if bytes.Equal(ent.Blob.Digest, blob.Digest) {
			return blobregistry.ErrEntryExists
		}
	}

	s.blobs[space] = append(s.blobs[space], blobregistry.Record{
		Space:      space,
		Blob:       blob,
		Cause:      cause,
		InsertedAt: time.Now(),
	})
	return nil
}

func (s *Store) List(ctx context.Context, space did.DID, options ...blobregistry.ListOption) (store.Page[blobregistry.Record], error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cfg := blobregistry.ListConfig{}
	for _, opt := range options {
		opt(&cfg)
	}

	entries := s.blobs[space]

	if cfg.Cursor != nil {
		found := false
		for i, ent := range entries {
			if ent.Blob.Digest.HexString() == *cfg.Cursor {
				entries = entries[i+1:]
				found = true
				break
			}
		}
		if !found {
			return store.Page[blobregistry.Record]{}, nil
		}
	}

	var cursor *string
	if cfg.Limit != nil && len(entries) > *cfg.Limit {
		entries = entries[:*cfg.Limit]
		c := entries[len(entries)-1].Blob.Digest.HexString()
		cursor = &c
	}

	results := make([]blobregistry.Record, len(entries))
	copy(results, entries)
	return store.Page[blobregistry.Record]{Results: results, Cursor: cursor}, nil
}

func (s *Store) Remove(ctx context.Context, space did.DID, digest multihash.Multihash) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ents := []blobregistry.Record{}
	found := false
	for _, ent := range s.blobs[space] {
		if bytes.Equal(ent.Blob.Digest, digest) {
			found = true
		} else {
			ents = append(ents, ent)
		}
	}
	if !found {
		return blobregistry.ErrEntryNotFound
	}
	s.blobs[space] = ents
	return nil
}
