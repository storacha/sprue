package handlers

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/sprue/pkg/indexerclient"
)

// SpaceIndexAddService defines the interface for the space/index/add handler.
type SpaceIndexAddService interface {
	ID() principal.Signer
	IndexerClient() *indexerclient.Client
	Logger() *zap.Logger
}

// extractRetrievalAuth extracts the space/content/retrieve delegation from the
// invocation facts. Guppy includes this delegation so the indexer can fetch
// the index blob from storage providers that require UCAN authorization.
func extractRetrievalAuth(inv invocation.Invocation) (delegation.Delegation, error) {
	var authLink ipld.Link
	for _, fact := range inv.Facts() {
		if v, ok := fact["retrievalAuth"]; ok {
			if node, ok := v.(ipld.Node); ok {
				link, err := node.AsLink()
				if err == nil {
					authLink = link
					break
				}
			}
		}
	}
	if authLink == nil {
		return nil, fmt.Errorf("retrievalAuth fact not found in invocation")
	}

	// Build delegation from invocation blocks
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(inv.Blocks()))
	if err != nil {
		return nil, fmt.Errorf("creating block reader: %w", err)
	}
	dlg, err := delegation.NewDelegationView(authLink, bs)
	if err != nil {
		return nil, fmt.Errorf("creating delegation view: %w", err)
	}
	return dlg, nil
}

// WithSpaceIndexAddMethod registers the space/index/add handler.
// This handler publishes index claims to the indexer service.
func WithSpaceIndexAddMethod(s SpaceIndexAddService) server.Option {
	return server.WithServiceMethod(
		spaceindexcap.AddAbility,
		server.Provide(
			spaceindexcap.Add,
			func(ctx context.Context,
				cap ucan.Capability[spaceindexcap.AddCaveats],
				inv invocation.Invocation,
				iCtx server.InvocationContext,
			) (result.Result[spaceindexcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				logger := s.Logger()

				spaceDID := cap.With()
				index := cap.Nb().Index
				content := cap.Nb().Content

				logger.Debug("space/index/add",
					zap.String("space", spaceDID),
					zap.String("index", index.String()),
					zap.Any("content", content))

				indexerClient := s.IndexerClient()
				if indexerClient == nil {
					logger.Debug("space/index/add STUB: indexer not configured")
					return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](
						spaceindexcap.AddOk{},
					), nil, nil
				}

				// Extract retrievalAuth delegation from invocation facts
				// Guppy provides this so the indexer can fetch the index blob from piri
				retrievalAuth, err := extractRetrievalAuth(inv)
				if err != nil {
					logger.Debug("no retrievalAuth in invocation", zap.Error(err))
					// Continue without retrieval auth - indexer will try public retrieval
				} else {
					logger.Debug("extracted retrievalAuth delegation",
						zap.String("link", retrievalAuth.Link().String()))
				}

				// Publish to indexer with retrieval authorization
				if err := indexerClient.PublishIndexClaim(ctx, spaceDID, content, index, retrievalAuth); err != nil {
					logger.Error("indexer publish failed", zap.Error(err))
					// Return success anyway - don't block uploads for indexing failures
					// TODO: Consider returning error in production
				} else {
					logger.Debug("published to indexer")
				}

				return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](
					spaceindexcap.AddOk{},
				), nil, nil
			},
		),
	)
}
