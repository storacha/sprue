package storageprovider

import (
	"context"
	"net/url"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/store"
)

const (
	// StorageProviderNotFoundErrorName is the name given to an error where the
	// storage provider is not found in the store.
	StorageProviderNotFoundErrorName = "StorageProviderNotFound"
)

var (
	ErrStorageProviderNotFound = store.NewError(StorageProviderNotFoundErrorName, "storage provider not found")
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

type StorageProviderRecord struct {
	// DID of the stroage provider.
	Provider did.DID
	// Public URL that accepts UCAN invocations.
	Endpoint url.URL
	// Proof the upload service can invoke blob/allocate and blob/accept.
	Proof delegation.Delegation
	// Weight determines chance of selection for uploads relative to other
	// providers.
	Weight int
	// ReplicationWeight determines the chance of selection for replications
	// relative to other providers. Defaults to weight if not set.
	ReplicationWeight int
	// Date and time the record was created (ISO 8601).
	InsertedAt time.Time
	// Date and time the record was last updated (ISO 8601).
	UpdatedAt time.Time
}

type Store interface {
	Put(ctx context.Context, providerID did.DID, endpoint url.URL, proof delegation.Delegation, weight int, replicationWeight int) error
	// Get a storage provider record by provider DID. May return
	// [ErrStorageProviderNotFound].
	Get(ctx context.Context, providerID did.DID) (StorageProviderRecord, error)
	// Delete a storage provider record by provider DID. May return
	// [ErrStorageProviderNotFound] if the record does not exist.
	Delete(ctx context.Context, providerID did.DID) error
	List(ctx context.Context, options ...ListOption) (store.Page[StorageProviderRecord], error)
}
