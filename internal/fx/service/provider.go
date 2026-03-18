package service

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/service"
	"github.com/storacha/sprue/pkg/state"
	"github.com/storacha/sprue/pkg/store/agent"
)

// Module provides the UCAN service.
var Module = fx.Module("service",
	fx.Provide(NewService),
)

// ServiceParams groups dependencies for Service construction.
type ServiceParams struct {
	fx.In

	Config        *config.Config
	Identity      *identity.Identity
	Store         state.StateStore
	AgentStore    agent.Store
	IndexerClient *indexerclient.Client `optional:"true"`
	Logger        *zap.Logger
	Options       []server.Option `group:"ucan_options"`
}

// NewService creates the UCAN service with all handlers registered.
func NewService(p ServiceParams) (*service.Service, error) {
	return service.New(p.Config, p.Identity, p.Store, p.AgentStore, p.IndexerClient, p.Logger, p.Options...)
}
