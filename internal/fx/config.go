package fx

import (
	"go.uber.org/fx"

	"github.com/storacha/sprue/internal/config"
)

var ConfigModule = fx.Module("config",
	fx.Provide(ProvideConfigs),
)

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
