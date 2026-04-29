package fx

import (
	"fmt"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/internal/fx/service"
	"github.com/storacha/sprue/internal/fx/service/handlers"
	"github.com/storacha/sprue/internal/fx/store/aws"
	"github.com/storacha/sprue/internal/fx/store/memory"
	"github.com/storacha/sprue/internal/fx/store/postgres"
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
		MS3TModule,
	}
	switch cfg.Storage.Type {
	case config.StorageTypeMemory:
		opts = append(opts, memory.Module)
	case config.StorageTypePostgres, "":
		// Empty Type is treated as the default backend (postgres) so callers
		// constructing a Config literal in tests don't have to set it.
		opts = append(opts, postgres.Module)
	case config.StorageTypeAWS:
		opts = append(opts, aws.Module)
	default:
		return fx.Error(fmt.Errorf("unknown storage.type %q (valid: memory, postgres, aws)", cfg.Storage.Type))
	}
	return fx.Options(opts...)
}
