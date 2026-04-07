package aws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/blobregistry"
	"github.com/storacha/sprue/pkg/store"
	blobregistrystore "github.com/storacha/sprue/pkg/store/blob_registry"
	awsblobregistrystore "github.com/storacha/sprue/pkg/store/blob_registry/aws"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/metrics"
	metricsaws "github.com/storacha/sprue/pkg/store/metrics/aws"
	spacediffaws "github.com/storacha/sprue/pkg/store/space_diff/aws"
)

// Service is the AWS implementation of blobregistry.Service.
// It uses DynamoDB transactions to atomically write blob records,
// space diffs, and metrics.
type Service struct {
	dynamo        *dynamodb.Client
	store         *awsblobregistrystore.Store
	consumerStore consumer.Store
	spaceDiff     *spacediffaws.Store
	spaceMetrics  *metricsaws.SpaceStore
	adminMetrics  *metricsaws.Store
}

var _ blobregistry.Service = (*Service)(nil)

func NewService(
	dynamo *dynamodb.Client,
	store *awsblobregistrystore.Store,
	consumerStore consumer.Store,
	spaceDiff *spacediffaws.Store,
	spaceMetrics *metricsaws.SpaceStore,
	adminMetrics *metricsaws.Store,
) *Service {
	return &Service{
		dynamo:        dynamo,
		store:         store,
		consumerStore: consumerStore,
		spaceDiff:     spaceDiff,
		spaceMetrics:  spaceMetrics,
		adminMetrics:  adminMetrics,
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

	items := []dynamotypes.TransactWriteItem{
		s.store.TransactAdd(space, blob, cause),
	}

	receiptAt := time.Now()
	for _, c := range consumers {
		items = append(items, s.spaceDiff.TransactPut(ctx, c.Provider, space, c.Subscription, cause, int64(blob.Size), receiptAt))
	}

	inc := map[string]uint64{
		metrics.BlobAddTotalMetric:     1,
		metrics.BlobAddSizeTotalMetric: blob.Size,
	}
	items = append(items, s.spaceMetrics.TransactIncrementTotals(space, inc)...)
	items = append(items, s.adminMetrics.TransactIncrementTotals(inc)...)

	if _, err := s.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}); err != nil {
		var txErr *dynamotypes.TransactionCanceledException
		if errors.As(err, &txErr) {
			for _, reason := range txErr.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return blobregistry.ErrEntryExists
				}
			}
		}
		return fmt.Errorf("registering blob: %w", err)
	}
	return nil
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

	items := []dynamotypes.TransactWriteItem{
		s.store.TransactRemove(space, digest),
	}

	receiptAt := time.Now()
	for _, c := range consumers {
		items = append(items, s.spaceDiff.TransactPut(ctx, c.Provider, space, c.Subscription, cause, -int64(existing.Blob.Size), receiptAt))
	}

	inc := map[string]uint64{
		metrics.BlobRemoveTotalMetric:     1,
		metrics.BlobRemoveSizeTotalMetric: existing.Blob.Size,
	}
	items = append(items, s.spaceMetrics.TransactIncrementTotals(space, inc)...)
	items = append(items, s.adminMetrics.TransactIncrementTotals(inc)...)

	if _, err := s.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}); err != nil {
		var txErr *dynamotypes.TransactionCanceledException
		if errors.As(err, &txErr) {
			for _, reason := range txErr.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return blobregistrystore.ErrEntryNotFound
				}
			}
		}
		return fmt.Errorf("deregistering blob: %w", err)
	}
	return nil
}
