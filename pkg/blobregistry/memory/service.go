package memory

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/blobregistry"
	"github.com/storacha/sprue/pkg/store"
	blobregistrystore "github.com/storacha/sprue/pkg/store/blob_registry"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/metrics"
	spacediff "github.com/storacha/sprue/pkg/store/space_diff"
)

type Service struct {
	store          blobregistrystore.Store
	consumerStore  consumer.Store
	spaceDiffStore spacediff.Store
	spaceMetrics   metrics.SpaceStore
	adminMetrics   metrics.Store
}

var _ blobregistry.Service = (*Service)(nil)

func NewService(
	store blobregistrystore.Store,
	consumerStore consumer.Store,
	spaceDiffStore spacediff.Store,
	spaceMetrics metrics.SpaceStore,
	adminMetrics metrics.Store,
) *Service {
	return &Service{
		store:          store,
		consumerStore:  consumerStore,
		spaceDiffStore: spaceDiffStore,
		spaceMetrics:   spaceMetrics,
		adminMetrics:   adminMetrics,
	}
}

func (s *Service) Get(ctx context.Context, space did.DID, digest multihash.Multihash) (blobregistry.Record, error) {
	return s.store.Get(ctx, space, digest)
}

func (s *Service) Register(ctx context.Context, space did.DID, blob types.Blob, cause cid.Cid) error {
	consumers, err := blobregistry.CollectConsumers(ctx, s.consumerStore, space)
	if err != nil {
		return fmt.Errorf("collecting consumers: %w", err)
	}

	if err := s.store.Add(ctx, space, blob, cause); err != nil {
		return err
	}

	inc := map[string]uint64{
		metrics.BlobAddTotalMetric:     1,
		metrics.BlobAddSizeTotalMetric: blob.Size,
	}
	if err := blobregistry.RecordSpaceDiffs(ctx, s.spaceDiffStore, consumers, space, cause, int64(blob.Size)); err != nil {
		return err
	}
	return blobregistry.IncrementMetrics(ctx, s.spaceMetrics, s.adminMetrics, space, inc)
}

func (s *Service) List(ctx context.Context, space did.DID, options ...blobregistry.ListOption) (store.Page[blobregistry.Record], error) {
	return s.store.List(ctx, space, options...)
}

func (s *Service) Deregister(ctx context.Context, space did.DID, digest multihash.Multihash, cause cid.Cid) error {
	existing, err := s.store.Get(ctx, space, digest)
	if err != nil {
		return err
	}

	consumers, err := blobregistry.CollectConsumers(ctx, s.consumerStore, space)
	if err != nil {
		return fmt.Errorf("collecting consumers: %w", err)
	}

	if err := s.store.Remove(ctx, space, digest); err != nil {
		return err
	}

	inc := map[string]uint64{
		metrics.BlobRemoveTotalMetric:     1,
		metrics.BlobRemoveSizeTotalMetric: existing.Blob.Size,
	}
	if err := blobregistry.RecordSpaceDiffs(ctx, s.spaceDiffStore, consumers, space, cause, -int64(existing.Blob.Size)); err != nil {
		return err
	}
	return blobregistry.IncrementMetrics(ctx, s.spaceMetrics, s.adminMetrics, space, inc)
}
