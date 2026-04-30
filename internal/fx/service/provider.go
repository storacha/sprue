package service

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/alanshaw/ucantone/server"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
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
	IndexerClient   *indexerclient.Client `optional:"true"`
	Logger          *zap.Logger
	Options         []server.HTTPOption `group:"ucan_options"`
}

// NewService creates the UCAN service with all handlers registered.
func NewService(p ServiceParams) (*service.Service, error) {
	return service.New(p.Identity, p.AgentStore, p.DelegationStore, p.IndexerClient, p.Logger, p.Options...)
}
