package serve

import (
	"context"
	"fmt"
	"time"

	"github.com/alanshaw/1up-service/pkg/build"
	"github.com/alanshaw/1up-service/pkg/config"
	"github.com/alanshaw/1up-service/pkg/fx/app"
	"github.com/alanshaw/1up-service/pkg/fx/receipt"
	"github.com/alanshaw/1up-service/pkg/fx/root"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("cmd/serve")

var Cmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the 1up server!",
	Args:  cobra.NoArgs,
	RunE:  doServe,
}

func init() {
	Cmd.PersistentFlags().String(
		"host",
		"localhost",
		"Host to listen on")
	cobra.CheckErr(viper.BindPFlag("server.host", Cmd.PersistentFlags().Lookup("host")))

	Cmd.PersistentFlags().UintP(
		"port",
		"p",
		3000,
		"Port to listen on",
	)
	cobra.CheckErr(viper.BindPFlag("server.port", Cmd.PersistentFlags().Lookup("port")))
}

func doServe(cmd *cobra.Command, _ []string) error {
	userCfg, err := config.Load[config.AppConfig]()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	appCfg, err := userCfg.ToAppConfig()
	if err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	app := fx.New(
		// if a panic occurs during operation, recover from it and exit (somewhat) gracefully.
		fx.RecoverFromPanics(),

		// provide fx with our logger for its events logged at debug level.
		// any fx errors will still be logged at the error level.
		fx.WithLogger(func() fxevent.Logger {
			el := &fxevent.ZapLogger{Logger: log.Desugar()}
			el.UseLogLevel(zapcore.DebugLevel)
			return el
		}),

		fx.StopTimeout(time.Minute),

		// common dependencies of the PDP and UCAN module:
		//   - identity
		//   - http server
		//   - databases & datastores
		app.CommonModules(appCfg),

		root.Module,
		receipt.Module,

		app.UploadModule,

		// Post-startup operations: print server info and record telemetry
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					cmd.Println("")
					cmd.Println("▗     ")
					cmd.Println("▜ ▌▌▛▌")
					cmd.Println("▟▖▙▌▙▌")
					cmd.Println("    ▌ ")
					cmd.Println("")
					cmd.Printf("🍄 1up-service %s\n", build.Version)
					cmd.Printf("🆔 %s\n", appCfg.Identity.Signer.DID())
					cmd.Printf("🚀 Ready! Server running on: http://%s:%d\n", appCfg.Server.Host, appCfg.Server.Port)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					log.Info("Shutting down server...")
					return nil
				},
			})
		}),
	)

	// valid the app was built successfully, an error here means a missing dep, i.e. a developer error (we never write errors...)
	if err := app.Err(); err != nil {
		return fmt.Errorf("building app: %w", err)
	}

	// run the app, when an interrupt signal is sent to the process, this method ends.
	// any errors encountered during shutdown will be exposed via logs
	app.Run()

	return nil
}
