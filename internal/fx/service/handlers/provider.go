package handlers

import (
	"go.uber.org/fx"

	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/sprue/pkg/service/handlers"
)

var Module = fx.Module("service-handlers",
	fx.Provide(
		fx.Annotate(
			handlers.AccessAuthorizeHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AccessClaimHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AccessDelegateHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AdminProviderDeregisterHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AdminProviderListHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AdminProviderRegisterHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.AdminProviderWeightSetHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.ProviderAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.BlobAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.BlobReplicateHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.IndexAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.UCANConcludeHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.UploadAddHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.UploadListHandler,
			fx.ResultTags(`group:"ucan_handlers"`),
		),
		fx.Annotate(
			handlers.UploadShardListHandler,
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
