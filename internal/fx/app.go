package fx

import (
	"go.uber.org/fx"
)

// AppModule aggregates all application modules.
var AppModule = fx.Options(
	ConfigModule,
	LoggerModule,
	IdentityModule,
	StoreModule,
	ClientsModule,
	ServiceModule,
	ServerModule,
)
