package fx

import (
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/internal/fx/service"
	"github.com/storacha/sprue/internal/fx/service/handlers"
	"github.com/storacha/sprue/internal/fx/store/aws"
	"github.com/storacha/sprue/internal/fx/store/memory"
	"go.uber.org/fx"
)

// AppModule aggregates all application modules.
var AppModule = func(cfg *config.Config) fx.Option {
	opts := []fx.Option{
		fx.Supply(cfg),
		ConfigModule,
		LoggerModule,
		IdentityModule,
		ServicesModule,
		ClientsModule,
		service.Module,
		handlers.Module,
		ServerModule,
	}
	if cfg.Deployment.InMemoryStores {
		opts = append(opts, memory.Module)
	} else {
		opts = append(opts, aws.Module)
	}
	return fx.Options(opts...)
}
