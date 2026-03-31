package routing

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/sprue/internal/testutil"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
	spmemory "github.com/storacha/sprue/pkg/store/storage_provider/memory"
	"github.com/stretchr/testify/require"
)

func mustURL(t *testing.T, raw string) url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return *u
}

func addProvider(t *testing.T, store *spmemory.Store, weight int, replicationWeight *int) storageprovider.Record {
	t.Helper()
	ctx := context.Background()
	id := testutil.RandomSigner(t)
	endpoint := mustURL(t, "https://"+strings.TrimPrefix(id.DID().String(), "did:key:")+".example.com")
	err := store.Put(ctx, id.DID(), endpoint, nil, weight, replicationWeight)
	require.NoError(t, err)
	rec, err := store.Get(ctx, id.DID())
	require.NoError(t, err)
	return rec
}

func TestGetProviderInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("found", func(t *testing.T) {
		store := spmemory.New()
		rec := addProvider(t, store, 100, nil)
		svc := NewService(store)

		info, err := svc.GetProviderInfo(ctx, rec.Provider)
		require.NoError(t, err)
		require.Equal(t, rec.Provider, info.ID)
		require.Equal(t, rec.Endpoint, info.Endpoint)
	})

	t.Run("not found", func(t *testing.T) {
		store := spmemory.New()
		svc := NewService(store)
		unknown := testutil.RandomSigner(t)

		_, err := svc.GetProviderInfo(ctx, unknown)
		require.ErrorIs(t, err, storageprovider.ErrStorageProviderNotFound)
	})
}

func TestSelectStorageProvider(t *testing.T) {
	ctx := context.Background()
	blob := types.Blob{Size: 1024}

	t.Run("no providers", func(t *testing.T) {
		store := spmemory.New()
		svc := NewService(store)

		_, err := svc.SelectStorageProvider(ctx, blob)
		require.ErrorIs(t, err, ErrCandidateUnavailable)
	})

	t.Run("single provider", func(t *testing.T) {
		store := spmemory.New()
		rec := addProvider(t, store, 100, nil)
		svc := NewService(store)

		info, err := svc.SelectStorageProvider(ctx, blob)
		require.NoError(t, err)
		require.Equal(t, rec.Provider, info.ID)
	})

	t.Run("excludes zero weight providers", func(t *testing.T) {
		store := spmemory.New()
		addProvider(t, store, 0, nil)
		svc := NewService(store)

		_, err := svc.SelectStorageProvider(ctx, blob)
		require.ErrorIs(t, err, ErrCandidateUnavailable)
	})

	t.Run("with exclusions", func(t *testing.T) {
		store := spmemory.New()
		excluded := addProvider(t, store, 100, nil)
		kept := addProvider(t, store, 100, nil)
		svc := NewService(store)

		// Run multiple times to verify excluded provider is never selected
		for range 20 {
			info, err := svc.SelectStorageProvider(ctx, blob, WithExclusions(excluded.Provider))
			require.NoError(t, err)
			require.Equal(t, kept.Provider, info.ID)
		}
	})

	t.Run("all providers excluded", func(t *testing.T) {
		store := spmemory.New()
		p := addProvider(t, store, 100, nil)
		svc := NewService(store)

		_, err := svc.SelectStorageProvider(ctx, blob, WithExclusions(p.Provider))
		require.ErrorIs(t, err, ErrCandidateUnavailable)
	})

	t.Run("selects from multiple providers", func(t *testing.T) {
		store := spmemory.New()
		p1 := addProvider(t, store, 100, nil)
		p2 := addProvider(t, store, 100, nil)
		svc := NewService(store)

		seen := map[string]bool{}
		for range 100 {
			info, err := svc.SelectStorageProvider(ctx, blob)
			require.NoError(t, err)
			seen[info.ID.DID().String()] = true
		}
		// With equal weights over 100 iterations, both should be selected
		require.True(t, seen[p1.Provider.DID().String()])
		require.True(t, seen[p2.Provider.DID().String()])
	})
}

func TestSelectReplicationProvider(t *testing.T) {
	ctx := context.Background()
	blob := types.Blob{Size: 1024}

	t.Run("excludes primary", func(t *testing.T) {
		store := spmemory.New()
		primary := addProvider(t, store, 100, nil)
		secondary := addProvider(t, store, 100, nil)
		svc := NewService(store)

		for range 20 {
			info, err := svc.SelectReplicationProvider(ctx, primary.Provider, blob)
			require.NoError(t, err)
			require.Equal(t, secondary.Provider, info.ID)
		}
	})

	t.Run("no candidates after excluding primary", func(t *testing.T) {
		store := spmemory.New()
		primary := addProvider(t, store, 100, nil)
		svc := NewService(store)

		_, err := svc.SelectReplicationProvider(ctx, primary.Provider, blob)
		require.ErrorIs(t, err, ErrCandidateUnavailable)
	})

	t.Run("excludes zero replication weight", func(t *testing.T) {
		store := spmemory.New()
		primary := addProvider(t, store, 100, nil)
		zeroRW := 0
		addProvider(t, store, 100, &zeroRW)
		good := addProvider(t, store, 100, nil)
		svc := NewService(store)

		for range 20 {
			info, err := svc.SelectReplicationProvider(ctx, primary.Provider, blob)
			require.NoError(t, err)
			require.Equal(t, good.Provider, info.ID)
		}
	})

	t.Run("uses replication weight when set", func(t *testing.T) {
		store := spmemory.New()
		primary := addProvider(t, store, 100, nil)
		rw := 50
		replica := addProvider(t, store, 100, &rw)
		svc := NewService(store)

		info, err := svc.SelectReplicationProvider(ctx, primary.Provider, blob)
		require.NoError(t, err)
		require.Equal(t, replica.Provider, info.ID)
	})

	t.Run("combines primary exclusion with additional exclusions", func(t *testing.T) {
		store := spmemory.New()
		primary := addProvider(t, store, 100, nil)
		excluded := addProvider(t, store, 100, nil)
		keeper := addProvider(t, store, 100, nil)
		svc := NewService(store)

		for range 20 {
			info, err := svc.SelectReplicationProvider(ctx, primary.Provider, blob, WithExclusions(excluded.Provider))
			require.NoError(t, err)
			require.Equal(t, keeper.Provider, info.ID)
		}
	})
}
