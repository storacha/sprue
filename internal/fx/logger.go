package fx

import (
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
)

// LoggerModule provides both *zap.Logger and *otelzap.Logger. Code that has
// no request context injects *zap.Logger unchanged; code that has a context
// can inject *otelzap.Logger to get span-status updates and OTel log bridge
// emission via its Ctx / *Context methods.
var LoggerModule = fx.Module("logger",
	fx.Provide(NewZapLogger),
	fx.Provide(NewOtelLogger),
)

// NewZapLogger creates a zap logger based on the configured log level.
func NewZapLogger(cfg *config.Config) (*zap.Logger, error) {
	if cfg.Log.Level == "debug" || cfg.Deployment.Environment == "development" || cfg.Deployment.Environment == "test" {
		return zap.NewDevelopment()
	}
	return zap.NewProduction()
}

// NewOtelLogger wraps the base zap logger with otelzap so call sites with a
// context can use logger.Ctx(ctx) to annotate the active span and emit to
// the OTel log pipeline. Also registers otelzap globals so free-floating
// otelzap.Ctx(ctx) calls work anywhere in the program.
func NewOtelLogger(base *zap.Logger) *otelzap.Logger {
	l := otelzap.New(base,
		otelzap.WithMinLevel(zap.InfoLevel),
		otelzap.WithErrorStatusLevel(zap.ErrorLevel),
	)
	otelzap.ReplaceGlobals(l)
	return l
}
