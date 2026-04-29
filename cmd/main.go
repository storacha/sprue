package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"

	"github.com/storacha/sprue/cmd/client"
	"github.com/storacha/sprue/cmd/identity"
	"github.com/storacha/sprue/internal/config"
	appfx "github.com/storacha/sprue/internal/fx"
)

var (
	cfgFile string
	loaded  *config.Config
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "sprue",
		Short: "Sprue upload service for Storacha local development",
		Long: `Sprue is the upload coordination service for Storacha local development.
Routes blob allocations to Piri nodes and tracks upload state in DynamoDB.`,
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the sprue service",
		RunE:  runServe,
	}

	// Hidden telemetry flags on `serve`. Registered with zero-value defaults;
	// actual binding into viper happens in PersistentPreRunE after cobra has
	// parsed argv, so pflag.Changed reflects only flags the user passed.
	config.RegisterTelemetryFlags(serveCmd)

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(identity.Cmd)

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path (default: looks for config.yaml in current dir)")

	// PersistentPreRunE only loads config for the `serve` subcommand — the
	// client / identity commands load their own state from elsewhere.
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if cmd != serveCmd {
			return nil
		}
		cfg, v, err := config.LoadWithViper(cfgFile)
		if err != nil {
			return err
		}
		if err := config.BindTelemetryFlags(v, cmd); err != nil {
			return err
		}
		// Re-unmarshal so that any flag values bound above now override yaml/env.
		cfg, err = config.Unmarshal(v)
		if err != nil {
			return err
		}
		loaded = cfg
		return nil
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	if loaded == nil {
		return fmt.Errorf("config not loaded")
	}

	app := fx.New(
		appfx.AppModule(loaded),
		// Suppress fx's default logging and use our own zap logger
		fx.WithLogger(func(log *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: log}
		}),
	)
	app.Run()

	return nil
}
