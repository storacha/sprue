// Package ms3t exposes the embedded S3 listener as both a low-level
// Server type (see server.go) and an fx module (see Module).
//
// The S3 protocol layer is provided by github.com/versity/versitygw;
// the storage backend is the LSM-style log in pkg/ms3t/logstore in
// front of a Forge-backed read tier, with versitygw → logstore
// translation in pkg/ms3t/s3frontend.
//
// pkg/ms3t depends on a single external storage type for production
// wiring: *pgxpool.Pool. Callers are responsible for constructing
// the pool (typically via sprue's internal/fx/store/postgres). The
// module runs its own goose migrations (pkg/ms3t/migrations) against
// the pool at startup so the outer wiring does not need to know
// about ms3t's schema.
//
// When config.MS3T.Enabled is false the module is a no-op, so it is
// safe to always include it in the app graph.
package ms3t

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/storacha/go-ucanto/did"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/ms3t/blockstore"
	"github.com/storacha/sprue/pkg/ms3t/migrations"
	"github.com/storacha/sprue/pkg/ms3t/registry"
	"github.com/storacha/sprue/pkg/ms3t/uploader"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/routing"
)

// Module registers the embedded ms3t S3 listener. When
// config.MS3T.Enabled is false the module is a no-op, so it's safe
// to always include in the app graph.
var Module = fx.Module("ms3t",
	fx.Invoke(registerLifecycle),
)

// FxDeps bundles the sprue-internal services ms3t pulls in from the
// fx graph in production. Pool is the only storage dependency: ms3t
// owns its own schema (under the ms3t Postgres schema) and runs its
// own migrations.
//
// Pool is marked optional in the fx graph because storage backends
// other than postgres (memory, aws) do not provide one. ms3t is
// opt-in; when ms3t.enabled is true, registerLifecycle returns a
// fail-fast error if Pool is nil.
type FxDeps struct {
	fx.In

	Pool          *pgxpool.Pool `optional:"true"`
	Identity      *identity.Identity
	Router        *routing.Service
	PiriProvider  piriclient.Provider
	IndexerClient *indexerclient.Client `optional:"true"`
}

// registerLifecycle is the fx-only thin shim. It builds the
// production-only collaborators (Forge, Internal uploader, Postgres
// registry, migrations, space signer) and hands them to ms3t.New.
// Anything beyond that wiring lives in server.go and is reachable by
// tests without fx.
func registerLifecycle(
	lc fx.Lifecycle,
	cfg *config.Config,
	zlog *zap.Logger,
	deps FxDeps,
) error {
	mc := cfg.MS3T
	if !mc.Enabled {
		return nil
	}

	if deps.Pool == nil {
		return fmt.Errorf("ms3t: a *pgxpool.Pool must be provided in the fx graph when ms3t.enabled is true")
	}
	if deps.IndexerClient == nil {
		return fmt.Errorf("ms3t: indexer client is required (configure indexer.endpoint)")
	}

	if err := os.MkdirAll(mc.DataDir, 0o755); err != nil {
		return fmt.Errorf("ms3t: mkdir data dir: %w", err)
	}

	// Apply ms3t's own migrations against the caller-supplied pool.
	// Goose runs in the ms3t schema and tracks its version table at
	// ms3t.goose_db_version, so this never collides with any other
	// migrations on the same database.
	if err := migrations.Up(context.Background(), deps.Pool, zlog); err != nil {
		return fmt.Errorf("ms3t: migrations: %w", err)
	}

	// ms3t IS the space owner (root UCAN authority) so that
	// self-issued space/content/retrieve delegations validate down
	// the chain to piri's retrieval auth check. Key is generated on
	// first run and persisted under data_dir/space.key.
	keyPath := filepath.Join(mc.DataDir, "space.key")
	spaceSigner, err := LoadOrCreateSigner(keyPath)
	if err != nil {
		return fmt.Errorf("ms3t: space signer: %w", err)
	}
	zlog.Info("ms3t space loaded",
		zap.String("space_did", spaceSigner.DID().String()),
		zap.String("key_file", keyPath),
	)

	forgeReader, err := blockstore.NewForge(blockstore.ForgeConfig{
		IndexerEndpoint: cfg.Indexer.Endpoint,
		IndexerDID:      cfg.Indexer.DID,
		Spaces:          []did.DID{spaceSigner.DID()},
		Signer:          deps.Identity.Signer,
		SpaceSigner:     spaceSigner,
		Logger:          zlog,
	})
	if err != nil {
		return fmt.Errorf("ms3t: Reader blockstore: %w", err)
	}

	reg := registry.NewPostgres(deps.Pool)

	zlog.Info("ms3t internal uploader configured",
		zap.String("space_did", spaceSigner.DID().String()),
		zap.String("signer_did", deps.Identity.DID()),
	)
	up, err := uploader.NewForge(uploader.ForgeConfig{
		Router:        deps.Router,
		PiriProvider:  deps.PiriProvider,
		IndexerClient: deps.IndexerClient,
		Signer:        deps.Identity.Signer,
		SpaceSigner:   spaceSigner,
		Logger:        zlog,
	})
	if err != nil {
		return fmt.Errorf("ms3t: uploader: %w", err)
	}

	sealAge, err := time.ParseDuration(emptyDefault(mc.SealAge, "5s"))
	if err != nil {
		return fmt.Errorf("ms3t: parse seal_age %q: %w", mc.SealAge, err)
	}

	server, err := New(context.Background(),
		ServerConfig{
			Addr:       mc.Addr,
			DataDir:    mc.DataDir,
			Region:     mc.Region,
			RootAccess: mc.RootAccess,
			RootSecret: mc.RootSecret,
			ChunkSize:  mc.ChunkSize,
			SealBytes:  mc.SealBytes,
			SealAge:    sealAge,
			Retain:     mc.Retain,
		},
		ServerDeps{
			Logger:          zlog,
			BaseBlockReader: forgeReader,
			Uploader:        up,
			Registry:        reg,
			Meta:            reg,
		},
	)
	if err != nil {
		return err
	}

	lc.Append(fx.Hook{
		OnStart: server.Start,
		OnStop:  server.Stop,
	})
	return nil
}

// emptyDefault returns def when s is the empty string.
func emptyDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
