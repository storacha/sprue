package fx

import (
	"go.uber.org/fx"

	"github.com/storacha/sprue/internal/config"
)

// ConfigParams holds parameters for config loading.
type ConfigParams struct {
	ConfigFile string
}

// ConfigModule provides configuration via viper.
var ConfigModule = fx.Module("config",
	fx.Provide(NewConfig),
)

// NewConfig creates the Config from viper, loading from the config file,
// environment variables, and defaults.
func NewConfig(params ConfigParams) (*config.Config, error) {
	return config.Load(params.ConfigFile)
}
