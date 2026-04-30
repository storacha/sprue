package delegation

import (
	"context"

	"github.com/alanshaw/ucantone/did"
	"github.com/alanshaw/ucantone/ucan"
	"github.com/ipfs/go-cid"
	"github.com/storacha/sprue/pkg/store"
)

type (
	ListByAudienceConfig = store.PaginationConfig
	ListByAudienceOption func(cfg *ListByAudienceConfig)
)

func WithListByAudienceLimit(limit int) ListByAudienceOption {
	return func(cfg *ListByAudienceConfig) { cfg.Limit = &limit }
}

func WithListByAudienceCursor(cursor string) ListByAudienceOption {
	return func(cfg *ListByAudienceConfig) { cfg.Cursor = &cursor }
}

type Store interface {
	// Write several items into storage.
	//
	// Implementations MAY choose to avoid storing delegations as long as they can
	// reliably retrieve the invocation by CID when they need to return the given
	// delegations.
	PutMany(ctx context.Context, delegations []ucan.Delegation, cause cid.Cid) error
	ListByAudience(ctx context.Context, audience did.DID, options ...ListByAudienceOption) (store.Page[ucan.Delegation], error)
}
