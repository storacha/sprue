package handlers

import (
	"go.uber.org/fx"

	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/sprue/pkg/service/handlers"
)

var Module = fx.Module("service-handlers",
	fx.Provide(
		fx.Annotate(
			handlers.NewAccessAuthorizeHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAccessClaimHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAccessDelegateHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAdminProviderDeregisterHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAdminProviderListHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAdminProviderRegisterHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewAdminProviderWeightSetHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewProviderAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewBlobAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewBlobReplicateHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewIndexAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewUCANConcludeHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewUploadAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewUploadListHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewUploadShardListHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.NewHTTPPutConcludeHandler,
			fx.ResultTags(`group:"ucan_conclude_handlers"`),
		),
		fx.Annotate(
			handlers.NewBlobReplicaTransferConcludeHandler,
			fx.ResultTags(`group:"ucan_conclude_handlers"`),
		),
		NewConcludeHandlers,
	),
)

type ConcludeHandlersParams struct {
	fx.In
	Handlers []handlers.ConclusionHandler `group:"ucan_conclude_handlers"`
}

func NewConcludeHandlers(params ConcludeHandlersParams) map[ucan.Ability]handlers.ConclusionHandlerFunc {
	handlers := make(map[ucan.Ability]handlers.ConclusionHandlerFunc, len(params.Handlers))
	for _, h := range params.Handlers {
		handlers[h.Ability] = h.Handler
	}
	return handlers
}
