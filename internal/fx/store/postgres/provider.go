// Package postgres wires the Postgres-backed store implementations into the
// application via uber-go/fx. It mirrors the layout of
// internal/fx/store/aws/provider.go.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/internal/migrations"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/storacha/sprue/pkg/store/agent"
	pgagent "github.com/storacha/sprue/pkg/store/agent/postgres"
	blobregistry "github.com/storacha/sprue/pkg/store/blob_registry"
	pgblobregistry "github.com/storacha/sprue/pkg/store/blob_registry/postgres"
	"github.com/storacha/sprue/pkg/store/consumer"
	pgconsumer "github.com/storacha/sprue/pkg/store/consumer/postgres"
	"github.com/storacha/sprue/pkg/store/customer"
	pgcustomer "github.com/storacha/sprue/pkg/store/customer/postgres"
	"github.com/storacha/sprue/pkg/store/delegation"
	pgdelegation "github.com/storacha/sprue/pkg/store/delegation/postgres"
	"github.com/storacha/sprue/pkg/store/metrics"
	pgmetrics "github.com/storacha/sprue/pkg/store/metrics/postgres"
	"github.com/storacha/sprue/pkg/store/replica"
	pgreplica "github.com/storacha/sprue/pkg/store/replica/postgres"
	"github.com/storacha/sprue/pkg/store/revocation"
	pgrevocation "github.com/storacha/sprue/pkg/store/revocation/postgres"
	spacediff "github.com/storacha/sprue/pkg/store/space_diff"
	pgspacediff "github.com/storacha/sprue/pkg/store/space_diff/postgres"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
	pgstorageprovider "github.com/storacha/sprue/pkg/store/storage_provider/postgres"
	"github.com/storacha/sprue/pkg/store/subscription"
	pgsubscription "github.com/storacha/sprue/pkg/store/subscription/postgres"
	"github.com/storacha/sprue/pkg/store/upload"
	pgupload "github.com/storacha/sprue/pkg/store/upload/postgres"

	// Reuse the AWS S3 client constructor for the three stores that keep an S3 half.
	awsstore "github.com/storacha/sprue/internal/fx/store/aws"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Module("postgres-store",
	fx.Provide(
		NewPostgresPool,
		// S3 client is still needed for the three stores (agent, delegation, upload)
		// that persist blob payloads outside of the database.
		awsstore.NewS3Client,

		fx.Annotate(NewAgentStore, fx.As(new(agent.Store))),
		fx.Annotate(NewBlobRegistryStore, fx.As(new(blobregistry.Store))),
		fx.Annotate(NewConsumerStore, fx.As(new(consumer.Store))),
		fx.Annotate(NewCustomerStore, fx.As(new(customer.Store))),
		fx.Annotate(NewDelegationStore, fx.As(new(delegation.Store))),
		fx.Annotate(NewSpaceMetricsStore, fx.As(fx.Self()), fx.As(new(metrics.SpaceStore))),
		fx.Annotate(NewAdminMetricsStore, fx.As(fx.Self()), fx.As(new(metrics.Store))),
		fx.Annotate(NewReplicaStore, fx.As(new(replica.Store))),
		fx.Annotate(NewRevocationStore, fx.As(new(revocation.Store))),
		fx.Annotate(NewSpaceDiffStore, fx.As(fx.Self()), fx.As(new(spacediff.Store))),
		fx.Annotate(NewStorageProviderStore, fx.As(new(storageprovider.Store))),
		fx.Annotate(NewSubscriptionStore, fx.As(new(subscription.Store))),
		fx.Annotate(NewUploadStore, fx.As(new(upload.Store))),
	),
	fx.Invoke(RunMigrations),
)

// NewPostgresPool creates a pgx connection pool and registers a lifecycle hook
// to close it at shutdown.
func NewPostgresPool(cfg config.PostgresConfig, lc fx.Lifecycle, logger *zap.Logger) (*pgxpool.Pool, error) {
	if cfg.DSN == "" {
		return nil, errors.New("postgres.dsn is required when store_backend is \"postgres\"")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres DSN: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pgx pool: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("pinging postgres: %w", err)
			}
			logger.Info("connected to postgres", zap.Int32("max_conns", poolCfg.MaxConns))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

// RunMigrations applies the embedded goose migrations at app start. It runs as
// an fx.Invoke so it executes before any store Initialize hooks. Migrations are
// skipped when storage.postgres.skip_migrations is true.
func RunMigrations(lc fx.Lifecycle, cfg config.PostgresConfig, pool *pgxpool.Pool, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if cfg.SkipMigrations {
				logger.Info("skipping postgres migrations (storage.postgres.skip_migrations=true)")
				return nil
			}
			logger.Info("running postgres migrations")
			return migrations.Up(ctx, pool, logger)
		},
	})
}

func NewAgentStore(lc fx.Lifecycle, pool *pgxpool.Pool, s3Cfg config.S3Config, s3Client *s3.Client) agent.Store {
	store := pgagent.New(pool, s3Client, s3Cfg.AgentMessageBucket)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return store.Shutdown(ctx)
		},
	})
	return store
}

func NewBlobRegistryStore(pool *pgxpool.Pool, consumerStore consumer.Store) blobregistry.Store {
	return pgblobregistry.New(pool, consumerStore)
}

func NewConsumerStore(pool *pgxpool.Pool) consumer.Store {
	return pgconsumer.New(pool)
}

func NewCustomerStore(pool *pgxpool.Pool) customer.Store {
	return pgcustomer.New(pool)
}

func NewDelegationStore(lc fx.Lifecycle, pool *pgxpool.Pool, s3Cfg config.S3Config, s3Client *s3.Client) delegation.Store {
	store := pgdelegation.New(pool, s3Client, s3Cfg.DelegationBucket)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewSpaceMetricsStore(pool *pgxpool.Pool) *pgmetrics.SpaceStore {
	return pgmetrics.NewSpaceStore(pool)
}

func NewAdminMetricsStore(pool *pgxpool.Pool) *pgmetrics.Store {
	return pgmetrics.New(pool)
}

func NewReplicaStore(pool *pgxpool.Pool) replica.Store {
	return pgreplica.New(pool)
}

func NewRevocationStore(pool *pgxpool.Pool) revocation.Store {
	return pgrevocation.New(pool)
}

func NewSpaceDiffStore(pool *pgxpool.Pool) *pgspacediff.Store {
	return pgspacediff.New(pool)
}

func NewStorageProviderStore(pool *pgxpool.Pool) storageprovider.Store {
	return pgstorageprovider.New(pool)
}

func NewSubscriptionStore(pool *pgxpool.Pool) subscription.Store {
	return pgsubscription.New(pool)
}

func NewUploadStore(pool *pgxpool.Pool) upload.Store {
	return pgupload.New(pool)
}
