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
	fx.Provide(
		NewConfig,
		ProvideConfigs,
	),
)

// NewConfig creates the Config from viper, loading from the config file,
// environment variables, and defaults.
func NewConfig(params ConfigParams) (*config.Config, error) {
	return config.Load(params.ConfigFile)
}

type Configs struct {
	fx.Out
	Deployment config.DeploymentConfig
	Server     config.ServerConfig
	Identity   config.IdentityConfig
	Indexer    config.IndexerConfig
	DynamoDB   config.DynamoDBConfig
	S3         config.S3Config
	Log        config.LogConfig
	Mailer     config.MailerConfig
}

// ProvideConfigs provides the individual fields of the config.
func ProvideConfigs(cfg *config.Config) Configs {
	return Configs{
		Deployment: cfg.Deployment,
		Server:     cfg.Server,
		Identity:   cfg.Identity,
		Indexer:    cfg.Indexer,
		DynamoDB:   cfg.DynamoDB,
		S3:         cfg.S3,
		Log:        cfg.Log,
		Mailer:     cfg.Mailer,
	}
}
