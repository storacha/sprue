package blobregistry

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/lib/errors"
	"github.com/storacha/sprue/pkg/store"
)

const (
	EntryNotFoundErrorName = "EntryNotFound"
	EntryExistsErrorName   = "EntryExists"
)

var (
	// ErrEntryNotFound indicates an entry was not found that matches the passed details.
	ErrEntryNotFound = errors.New(EntryNotFoundErrorName, "blob not found")
	// ErrEntryExists indicates an entry already exists that matches the passed details.
	ErrEntryExists = errors.New(EntryExistsErrorName, "blob already exists")
)

type (
	ListConfig = store.PaginationConfig
	ListOption func(cfg *ListConfig)
)

func WithListLimit(limit int) ListOption {
	return func(cfg *ListConfig) {
		cfg.Limit = &limit
	}
}

func WithListCursor(cursor string) ListOption {
	return func(cfg *ListConfig) {
		cfg.Cursor = &cursor
	}
}

type Record struct {
	Space      did.DID
	Blob       types.Blob
	Cause      cid.Cid
	InsertedAt time.Time
}

type Store interface {
	// Lookup an existing registration. May return [ErrEntryNotFound].
	Get(ctx context.Context, space did.DID, digest multihash.Multihash) (Record, error)
	// Add an item into the registry if it does not already exist. May return
	// [ErrEntryExists] if the blob is already registered in the space.
	Add(ctx context.Context, space did.DID, blob types.Blob, cause cid.Cid) error
	// List entries in the registry for a given space.
	List(ctx context.Context, space did.DID, options ...ListOption) (store.Page[Record], error)
	// Remove an item from the registry if it exists.
	Remove(ctx context.Context, space did.DID, digest multihash.Multihash) error
}
