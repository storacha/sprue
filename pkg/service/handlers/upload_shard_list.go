package handlers

import (
	"context"
	"fmt"

	"github.com/alanshaw/ucantone/did"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/capabilities/upload/shard"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/sprue/pkg/internal/ipldutil"
	"github.com/storacha/sprue/pkg/lib/errors"
	upload_store "github.com/storacha/sprue/pkg/store/upload"
	"go.uber.org/zap"
)

// WithUploadShardListMethod registers the upload/shard/list handler.
// This handler lists the shards of an upload.
func WithUploadShardListMethod(uploadStore upload_store.Store, logger *zap.Logger) server.Option {
	return server.WithServiceMethod(
		shard.ListAbility,
		server.Provide(shard.List, UploadShardListHandler(uploadStore, logger)),
	)
}

func UploadShardListHandler(uploadStore upload_store.Store, logger *zap.Logger) server.HandlerFunc[shard.ListCaveats, shard.ListOk, failure.IPLDBuilderFailure] {
	log := logger.With(zap.String("handler", shard.ListAbility))
	return server.HandlerFunc[shard.ListCaveats, shard.ListOk, failure.IPLDBuilderFailure](
		func(ctx context.Context,
			cap ucan.Capability[shard.ListCaveats],
			inv invocation.Invocation,
			iCtx server.InvocationContext,
		) (result.Result[shard.ListOk, failure.IPLDBuilderFailure], fx.Effects, error) {
			args := cap.Nb()
			log := log.With(zap.String("space", cap.With()), zap.Stringer("root", args.Root))

			var opts []upload_store.ListShardsOption
			if args.Size != nil {
				log = log.With(zap.Uint64("size", *args.Size))
				opts = append(opts, upload_store.WithListShardsLimit(int(*args.Size)))
			}
			if args.Cursor != nil {
				log = log.With(zap.String("cursor", *args.Cursor))
				opts = append(opts, upload_store.WithListShardsCursor(*args.Cursor))
			}
			log.Debug("listing upload shards")

			space, err := did.Parse(cap.With())
			if err != nil {
				return result.Error[shard.ListOk, failure.IPLDBuilderFailure](
					errors.New(InvalidSpaceErrorName, "invalid space DID: %v", err),
				), nil, nil
			}

			root, err := ipldutil.ToCID(args.Root)
			if err != nil {
				return nil, nil, err
			}

			page, err := uploadStore.ListShards(ctx, space, root, opts...)
			if err != nil {
				log.Error("failed to list upload shards", zap.Error(err))
				return nil, nil, fmt.Errorf("listing upload shards: %w", err)
			}

			results := make([]ipld.Link, 0, len(page.Results))
			for _, r := range page.Results {
				results = append(results, cidlink.Link{Cid: r})
			}

			return result.Ok[shard.ListOk, failure.IPLDBuilderFailure](shard.ListOk{
				Results: results,
				Cursor:  page.Cursor,
			}), nil, nil
		})
}
