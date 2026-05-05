package service

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/fil-forge/ucantone/server"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/service"
	"github.com/storacha/sprue/pkg/store/agent"
	"github.com/storacha/sprue/pkg/store/delegation"
)

// Module provides the UCAN service.
var Module = fx.Module("service",
	fx.Provide(NewService),
)

// ServiceParams groups dependencies for Service construction.
type ServiceParams struct {
	fx.In

	Identity        *identity.Identity
	AgentStore      agent.Store
	DelegationStore delegation.Store
	Logger          *zap.Logger
	Handlers        []service.Handler   `group:"ucan_handlers"`
	Options         []server.HTTPOption `group:"ucan_options"`
}

// NewService creates the UCAN service with all handlers registered.
func NewService(p ServiceParams) *service.Service {
	return service.New(p.Identity, p.AgentStore, p.DelegationStore, p.Handlers, p.Logger, p.Options...)
}
