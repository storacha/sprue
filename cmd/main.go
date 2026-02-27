package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/fx"

	appfx "github.com/storacha/sprue/internal/fx"
)

var cfgFile string

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

	rootCmd.AddCommand(serveCmd)

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path (default: looks for config.yaml in current dir)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	app := fx.New(
		fx.Supply(appfx.ConfigParams{
			ConfigFile: cfgFile,
		}),
		appfx.AppModule,
		// Suppress fx's default logging, we use our own zap logger
		fx.NopLogger,
	)

	app.Run()
	return nil
}
