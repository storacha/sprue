package handlers

import (
	"go.uber.org/fx"

	"github.com/storacha/sprue/pkg/service/handlers"
)

var Module = fx.Module("service-handlers",
	fx.Provide(
		fx.Annotate(
			handlers.WithAccessAuthorizeMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithAccessClaimMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithAccessDelegateMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithFilecoinOfferMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithProviderAddMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithSpaceBlobAddMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithSpaceBlobReplicateMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithSpaceIndexAddMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithUCANConcludeMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
		fx.Annotate(
			handlers.WithUploadAddMethod,
			fx.ResultTags(`group:"ucan_options"`),
		),
	),
)
