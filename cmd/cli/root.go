package cli

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/alanshaw/1up-service/cmd/cli/client"
	"github.com/alanshaw/1up-service/cmd/cli/serve"
	"github.com/alanshaw/1up-service/pkg/build"
)

func ExecuteContext(ctx context.Context) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}

var log = logging.Logger("cmd")

var configFilePath = path.Join("1up", "config.toml")

var (
	cfgFile  string
	logLevel string
	rootCmd  = &cobra.Command{
		Use:     "1up",
		Short:   "The Upload Service for the Storacha Network",
		Long:    "UCAN 1.0 version of the Upload Service for the Storacha Network",
		Version: build.Version,
	}
)

func init() {
	cobra.OnInitialize(initLogging, initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file path. Attempts to load from user config directory if not set e.g. ~/.config/"+configFilePath)

	rootCmd.PersistentFlags().String("data-dir", filepath.Join(lo.Must(os.UserHomeDir()), ".1up"), "Upload service data directory")
	cobra.CheckErr(viper.BindPFlag("repo.data_dir", rootCmd.PersistentFlags().Lookup("data-dir")))

	rootCmd.PersistentFlags().String("key-file", "", "Path to a PEM file containing ed25519 private key")
	cobra.CheckErr(rootCmd.MarkPersistentFlagFilename("key-file", "pem"))
	cobra.CheckErr(viper.BindPFlag("identity.key_file", rootCmd.PersistentFlags().Lookup("key-file")))

	// register all commands and their subcommands
	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(serve.Cmd)
}

func initConfig() {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("1UP")

	if cfgFile == "" {
		if configDir, err := os.UserConfigDir(); err == nil {
			defaultCfgFile := path.Join(configDir, configFilePath)
			if inf, err := os.Stat(defaultCfgFile); err == nil && !inf.IsDir() {
				log.Infof("loading config automatically from: %s", defaultCfgFile)
				cfgFile = defaultCfgFile
			} else {
				log.Warnw("failed to stat default config file", "error", err)
			}
		}
	}

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		cobra.CheckErr(viper.ReadInConfig())
	} else {
		// otherwise look for 1up-config.toml in current directory
		viper.SetConfigName("1up-config")
		viper.SetConfigType("toml")
		viper.AddConfigPath(".")
		// Don't error if config file is not found - it's optional
		_ = viper.ReadInConfig()
	}
}

func initLogging() {
	if logLevel != "" {
		ll, err := logging.LevelFromString(logLevel)
		cobra.CheckErr(err)
		logging.SetAllLoggers(ll)
	} else {
		// else set all loggers to warn level, then the ones we care most about to info.
		logging.SetAllLoggers(logging.LevelWarn)
		logging.SetLogLevel("cmd/serve", "info")
		logging.SetLogLevel("fx/upload/ucan", "info")
		logging.SetLogLevel("service/upload/ucan", "info")
		for _, cmd := range []string{
			"/debug/echo",
			"/provider/deregister",
			"/provider/register",
			"/provider/weight/set",
			"/space/blob/add",
		} {
			logging.SetLogLevel("service/upload/ucan"+cmd, "info")
		}
	}
}
