package fx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/ms3t/blockstore"
	"github.com/storacha/sprue/pkg/ms3t/bucket"
	"github.com/storacha/sprue/pkg/ms3t/registry"
	"github.com/storacha/sprue/pkg/ms3t/server"
	"github.com/storacha/sprue/pkg/ms3t/uploader"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/routing"

	_ "modernc.org/sqlite"
)

// MS3TModule registers the embedded ms3t S3 listener. When
// config.MS3T.Enabled is false the module is a no-op, so it's safe
// to always include in the app graph.
var MS3TModule = fx.Module("ms3t",
	fx.Invoke(RegisterMS3TLifecycle),
)

// MS3TDeps bundles the sprue-internal services ms3t pulls when
// config.MS3T.Forge.Enabled is true. Marked optional so disabled
// deployments don't fail to construct (e.g., the indexer client
// short-circuits to nil when the indexer endpoint isn't set).
type MS3TDeps struct {
	fx.In

	Identity      *identity.Identity
	Router        *routing.Service
	PiriProvider  piriclient.Provider
	IndexerClient *indexerclient.Client `optional:"true"`
}

// RegisterMS3TLifecycle wires ms3t's bucket service, HTTP handler,
// and listener into the fx lifecycle. Construction failures (bad
// config, missing service for forge mode) are returned synchronously
// so fx can abort startup before any other module initializes.
func RegisterMS3TLifecycle(
	lc fx.Lifecycle,
	cfg *config.Config,
	zlog *zap.Logger,
	deps MS3TDeps,
) error {
	mc := cfg.MS3T
	if !mc.Enabled {
		return nil
	}

	if err := os.MkdirAll(mc.DataDir, 0o755); err != nil {
		return fmt.Errorf("ms3t: mkdir data dir: %w", err)
	}

	noCache := mc.Forge.Enabled && mc.Forge.NoCache

	// When Forge is enabled, load or generate ms3t's own space
	// keypair. ms3t IS the space owner (root UCAN authority) so that
	// self-issued space/content/retrieve delegations validate down
	// the chain to piri's retrieval auth check.
	var spaceSigner principal.Signer
	if mc.Forge.Enabled {
		keyPath := mc.Forge.SpaceKeyFile
		if keyPath == "" {
			keyPath = filepath.Join(mc.DataDir, "space.key")
		}
		s, err := uploader.LoadOrCreateSigner(keyPath)
		if err != nil {
			return fmt.Errorf("ms3t: space signer: %w", err)
		}
		spaceSigner = s
		zlog.Info("ms3t space loaded",
			zap.String("space_did", spaceSigner.DID().String()),
			zap.String("key_file", keyPath),
		)
	}

	// Build the blockstore. In no_cache mode this is a Forge-backed
	// read-only store (every Get hits indexer + piri); SQLite is
	// skipped entirely. Otherwise we open a SQLite file under
	// data_dir.
	var bs cbor.IpldBlockstore
	var sqliteDB *sql.DB

	if noCache {
		fb, err := blockstore.NewForge(blockstore.ForgeConfig{
			IndexerEndpoint: cfg.Indexer.Endpoint,
			IndexerDID:      cfg.Indexer.DID,
			Spaces:          []did.DID{spaceSigner.DID()},
			Signer:          deps.Identity.Signer,
			SpaceSigner:     spaceSigner,
			Logger:          zlog,
		})
		if err != nil {
			return fmt.Errorf("ms3t: forge blockstore: %w", err)
		}
		bs = fb
	} else {
		dbPath := filepath.Join(mc.DataDir, "ms3t.db")
		db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)")
		if err != nil {
			return fmt.Errorf("ms3t: open sqlite: %w", err)
		}
		db.SetMaxOpenConns(1)
		sb, err := blockstore.New(db)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("ms3t: blockstore: %w", err)
		}
		bs = sb
		sqliteDB = db
	}

	// Registry always lives in SQLite; in no_cache mode it's the
	// only thing in the SQLite file. Open a (different) DB so
	// reusing one connection isn't a concern.
	regDBPath := filepath.Join(mc.DataDir, "ms3t-registry.db")
	if !noCache {
		// reuse the same db for registry when SQLite blockstore is in use
		regDBPath = ""
	}
	regDB, err := openRegistryDB(regDBPath, sqliteDB)
	if err != nil {
		if sqliteDB != nil {
			_ = sqliteDB.Close()
		}
		return fmt.Errorf("ms3t: registry db: %w", err)
	}
	reg, err := registry.NewSQL(regDB)
	if err != nil {
		_ = regDB.Close()
		if sqliteDB != nil && sqliteDB != regDB {
			_ = sqliteDB.Close()
		}
		return fmt.Errorf("ms3t: registry: %w", err)
	}

	carDir := filepath.Join(mc.DataDir, "cars")
	innerUp, err := buildMS3TInnerUploader(mc, carDir, deps, spaceSigner, zlog)
	if err != nil {
		_ = regDB.Close()
		if sqliteDB != nil && sqliteDB != regDB {
			_ = sqliteDB.Close()
		}
		return fmt.Errorf("ms3t: uploader: %w", err)
	}

	// Wrap in Batched unless no_cache is on. In no_cache mode every
	// PUT blocks on the full Forge round trip, closing the
	// read-after-write race.
	var up uploader.Uploader
	if noCache {
		up = innerUp
	} else {
		batchAge, err := parseDurationOr(mc.BatchAge, 5*time.Second)
		if err != nil {
			_ = regDB.Close()
			if sqliteDB != nil && sqliteDB != regDB {
				_ = sqliteDB.Close()
			}
			return fmt.Errorf("ms3t: batch_age: %w", err)
		}
		batchBytes := mc.BatchBytes
		if batchBytes <= 0 {
			batchBytes = 64 << 20
		}
		up = uploader.NewBatched(innerUp, uploader.BatchedOptions{
			MaxBytes: batchBytes,
			MaxAge:   batchAge,
		})
	}

	chunkSize := mc.ChunkSize
	if chunkSize <= 0 {
		chunkSize = bucket.DefaultChunkSize
	}
	svc := bucket.New(bs, reg, bucket.Options{
		ChunkSize: chunkSize,
		Uploader:  up,
	})

	// Adapt sprue's zap logger into ms3t's slog interface so log
	// output funnels through one pipeline.
	slogger := slog.New(zapslog.NewHandler(zlog.Core(), zapslog.WithName("ms3t")))
	httpHandler := server.New(svc, slogger)
	srv := &http.Server{Addr: mc.Addr, Handler: httpHandler}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := svc.Recover(ctx); err != nil {
				return fmt.Errorf("ms3t: recover: %w", err)
			}
			zlog.Info("starting ms3t S3 listener",
				zap.String("addr", mc.Addr),
				zap.String("data_dir", mc.DataDir),
				zap.Bool("forge", mc.Forge.Enabled),
				zap.Bool("no_cache", noCache),
				zap.Int64("chunk_size", chunkSize),
			)
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					zlog.Error("ms3t listener error", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			zlog.Info("shutting down ms3t S3 listener")
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			var errs []error
			if err := srv.Shutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Errorf("http shutdown: %w", err))
			}
			if err := svc.Shutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Errorf("service shutdown: %w", err))
			}
			if err := regDB.Close(); err != nil {
				errs = append(errs, fmt.Errorf("registry db close: %w", err))
			}
			if sqliteDB != nil && sqliteDB != regDB {
				if err := sqliteDB.Close(); err != nil {
					errs = append(errs, fmt.Errorf("blockstore db close: %w", err))
				}
			}
			if len(errs) > 0 {
				return fmt.Errorf("ms3t shutdown: %v", errs)
			}
			return nil
		},
	})
	return nil
}

// openRegistryDB returns a *sql.DB for the registry. If reuse is
// non-nil, returns it (registry shares the SQLite file with the
// blockstore). Otherwise opens a fresh sqlite db at path.
func openRegistryDB(path string, reuse *sql.DB) (*sql.DB, error) {
	if reuse != nil {
		return reuse, nil
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// buildMS3TInnerUploader returns the inner uploader. With
// ms3t.forge.enabled = false the inner is uploader.Disk (writes CARs
// to a local directory). With it set to true, it's uploader.Internal
// — sprue's piriclient and indexerclient with sprue's identity as
// the signer and ms3t's self-generated space keypair as the space
// owner.
func buildMS3TInnerUploader(
	mc config.MS3TConfig,
	carDir string,
	deps MS3TDeps,
	spaceSigner principal.Signer,
	zlog *zap.Logger,
) (uploader.Uploader, error) {
	if !mc.Forge.Enabled {
		return uploader.NewDisk(carDir)
	}
	if deps.IndexerClient == nil {
		return nil, fmt.Errorf("ms3t.forge requires the indexer client; configure indexer.endpoint")
	}
	if spaceSigner == nil {
		return nil, fmt.Errorf("ms3t.forge requires a space signer (internal error)")
	}

	zlog.Info("ms3t internal uploader configured",
		zap.String("space_did", spaceSigner.DID().String()),
		zap.String("signer_did", deps.Identity.DID()),
		zap.Bool("no_cache", mc.Forge.NoCache),
	)

	return uploader.NewInternal(uploader.InternalConfig{
		Router:        deps.Router,
		PiriProvider:  deps.PiriProvider,
		IndexerClient: deps.IndexerClient,
		Signer:        deps.Identity.Signer,
		SpaceSigner:   spaceSigner,
		Logger:        zlog,
	})
}

func parseDurationOr(s string, dflt time.Duration) (time.Duration, error) {
	if s == "" {
		return dflt, nil
	}
	return time.ParseDuration(s)
}
