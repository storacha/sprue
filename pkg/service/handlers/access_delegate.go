package handlers

import (
	"context"

	"go.uber.org/zap"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/sprue/pkg/state"
)

// AccessDelegateService defines the interface for the access/delegate handler.
type AccessDelegateService interface {
	ID() principal.Signer
	State() state.StateStore
	Logger() *zap.Logger
}

// WithAccessDelegateMethod registers the access/delegate handler.
// This handler stores delegations for later retrieval.
func WithAccessDelegateMethod(s AccessDelegateService) server.Option {
	return server.WithServiceMethod(
		access.DelegateAbility,
		server.Provide(
			access.Delegate,
			func(ctx context.Context,
				cap ucan.Capability[access.DelegateCaveats],
				inv invocation.Invocation,
				iCtx server.InvocationContext,
			) (result.Result[access.DelegateOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				logger := s.Logger()

				agentDID := inv.Issuer().DID().String()
				delegations := cap.Nb().Delegations
				logger.Debug("access/delegate",
					zap.String("agent", agentDID),
					zap.Int("delegations", len(delegations.Keys)))

				// For a mock service, we just acknowledge receipt of the delegations
				// In a real service, these would be stored for later retrieval
				for _, key := range delegations.Keys {
					link := delegations.Values[key]
					if link != nil {
						logger.Debug("stored delegation", zap.String("link", link.String()))
					}
				}

				return result.Ok[access.DelegateOk, failure.IPLDBuilderFailure](access.DelegateOk{}), nil, nil
			},
		),
	)
}
