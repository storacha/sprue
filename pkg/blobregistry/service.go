package blobregistry

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/did"
	blobregistry "github.com/storacha/sprue/pkg/store/blob_registry"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/metrics"
	spacediff "github.com/storacha/sprue/pkg/store/space_diff"

	"github.com/storacha/sprue/pkg/store"
)

// Re-export store types so consumers only need to import this package.
type (
	Record     = blobregistry.Record
	ListOption = blobregistry.ListOption
)

var (
	WithListLimit  = blobregistry.WithListLimit
	WithListCursor = blobregistry.WithListCursor
	ErrEntryNotFound = blobregistry.ErrEntryNotFound
	ErrEntryExists   = blobregistry.ErrEntryExists
)

// Service provides blob registry operations with business logic
// (consumer lookup, space diff tracking, metrics updates).
type Service interface {
	Get(ctx context.Context, space did.DID, digest multihash.Multihash) (Record, error)
	Register(ctx context.Context, space did.DID, blob types.Blob, cause cid.Cid) error
	List(ctx context.Context, space did.DID, options ...ListOption) (store.Page[Record], error)
	Deregister(ctx context.Context, space did.DID, digest multihash.Multihash, cause cid.Cid) error
}

func CollectConsumers(ctx context.Context, consumerStore consumer.Store, space did.DID) ([]consumer.Record, error) {
	results, err := store.Collect(ctx, func(ctx context.Context, options store.PaginationConfig) (store.Page[consumer.Record], error) {
		opts := []consumer.ListOption{}
		if options.Cursor != nil {
			opts = append(opts, consumer.WithListCursor(*options.Cursor))
		}
		return consumerStore.List(ctx, space, opts...)
	})
	if err != nil {
		return nil, fmt.Errorf("listing consumers: %w", err)
	}
	if len(results) == 0 {
		return nil, consumer.ErrConsumerNotFound
	}
	return results, nil
}

func RecordSpaceDiffs(ctx context.Context, spaceDiffStore spacediff.Store, consumers []consumer.Record, space did.DID, cause cid.Cid, delta int64) error {
	receiptAt := time.Now()
	for _, c := range consumers {
		if err := spaceDiffStore.Put(ctx, c.Provider, space, c.Subscription, cause, delta, receiptAt); err != nil {
			return fmt.Errorf("recording space diff: %w", err)
		}
	}
	return nil
}

func IncrementMetrics(ctx context.Context, spaceMetrics metrics.SpaceStore, adminMetrics metrics.Store, space did.DID, inc map[string]uint64) error {
	if err := spaceMetrics.IncrementTotals(ctx, space, inc); err != nil {
		return fmt.Errorf("incrementing space metrics: %w", err)
	}
	if err := adminMetrics.IncrementTotals(ctx, inc); err != nil {
		return fmt.Errorf("incrementing admin metrics: %w", err)
	}
	return nil
}
