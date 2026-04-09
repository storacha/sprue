package lib

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/internal/fx"
	"github.com/storacha/sprue/pkg/client"
	"github.com/storacha/sprue/pkg/identity"
	"go.uber.org/zap"
)

func InitClient(cmd *cobra.Command) (*client.Client, *config.Config, *zap.Logger, *identity.Identity) {
	var configFile string
	configFlag := cmd.InheritedFlags().Lookup("config")
	if configFlag != nil {
		configFile = configFlag.Value.String()
	}
	cfg, err := config.Load(configFile)
	cobra.CheckErr(err)

	logger, err := fx.NewLogger(cfg)
	cobra.CheckErr(err)
	id, err := fx.NewIdentity(cfg, logger)
	cobra.CheckErr(err)

	c, err := client.New(
		id.Signer.DID(),
		client.WithServiceURL(fmt.Sprintf("http://%s:%d", cfg.Server.Host, cfg.Server.Port)),
	)
	cobra.CheckErr(err)
	return c, cfg, logger, id
}
