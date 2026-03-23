package fx

import (
	"github.com/storacha/sprue/internal/fx/service"
	"github.com/storacha/sprue/internal/fx/service/handlers"
	"github.com/storacha/sprue/internal/fx/store/aws"
	"go.uber.org/fx"
)

// AppModule aggregates all application modules. It requires [config.Config] to
// be provided by the caller, typically via [ConfigModule].
var AppModule = fx.Options(
	LoggerModule,
	IdentityModule,
	StoreModule,
	MailerModule,
	aws.Module,
	ClientsModule,
	service.Module,
	handlers.Module,
	ServerModule,
)
