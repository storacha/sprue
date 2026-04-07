package memory_test

import (
	"testing"

	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/sprue/internal/testutil"
	svcmemory "github.com/storacha/sprue/pkg/blobregistry/memory"
	storememory "github.com/storacha/sprue/pkg/store/blob_registry/memory"
	consumermemory "github.com/storacha/sprue/pkg/store/consumer/memory"
	"github.com/storacha/sprue/pkg/store/metrics"
	metricsmemory "github.com/storacha/sprue/pkg/store/metrics/memory"
	spacediffmemory "github.com/storacha/sprue/pkg/store/space_diff/memory"
	"github.com/stretchr/testify/require"
)

func newTestService() (*svcmemory.Service, *consumermemory.Store, *metricsmemory.SpaceStore, *metricsmemory.Store) {
	consumerStore := consumermemory.New()
	spaceMetrics := metricsmemory.NewSpaceStore()
	adminMetrics := metricsmemory.New()
	store := storememory.New()
	svc := svcmemory.NewService(store, consumerStore, spacediffmemory.New(), spaceMetrics, adminMetrics)
	return svc, consumerStore, spaceMetrics, adminMetrics
}

func TestServiceRegister(t *testing.T) {
	ctx := t.Context()

	t.Run("updates metrics on register", func(t *testing.T) {
		svc, consumerStore, spaceMetrics, adminMetrics := newTestService()

		space := testutil.RandomDID(t)
		provider := testutil.RandomDID(t)
		customer := testutil.RandomDID(t)
		cause := testutil.RandomCID(t)
		require.NoError(t, consumerStore.Add(ctx, provider, space, customer, "sub1", cause))

		bl := captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 8192}
		require.NoError(t, svc.Register(ctx, space, bl, cause))

		spaceM, err := spaceMetrics.Get(ctx, space)
		require.NoError(t, err)
		require.Equal(t, uint64(1), spaceM[metrics.BlobAddTotalMetric])
		require.Equal(t, uint64(8192), spaceM[metrics.BlobAddSizeTotalMetric])

		adminM, err := adminMetrics.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), adminM[metrics.BlobAddTotalMetric])
		require.Equal(t, uint64(8192), adminM[metrics.BlobAddSizeTotalMetric])
	})

	t.Run("fails without consumer", func(t *testing.T) {
		svc, _, _, _ := newTestService()

		space := testutil.RandomDID(t)
		bl := captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 1024}
		err := svc.Register(ctx, space, bl, testutil.RandomCID(t))
		require.Error(t, err)
	})
}

func TestServiceDeregister(t *testing.T) {
	ctx := t.Context()

	t.Run("updates metrics on deregister", func(t *testing.T) {
		svc, consumerStore, spaceMetrics, adminMetrics := newTestService()

		space := testutil.RandomDID(t)
		provider := testutil.RandomDID(t)
		customer := testutil.RandomDID(t)
		cause := testutil.RandomCID(t)
		require.NoError(t, consumerStore.Add(ctx, provider, space, customer, "sub1", cause))

		bl := captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 4096}
		require.NoError(t, svc.Register(ctx, space, bl, cause))
		require.NoError(t, svc.Deregister(ctx, space, bl.Digest, cause))

		spaceM, err := spaceMetrics.Get(ctx, space)
		require.NoError(t, err)
		require.Equal(t, uint64(1), spaceM[metrics.BlobRemoveTotalMetric])
		require.Equal(t, uint64(4096), spaceM[metrics.BlobRemoveSizeTotalMetric])

		adminM, err := adminMetrics.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), adminM[metrics.BlobRemoveTotalMetric])
		require.Equal(t, uint64(4096), adminM[metrics.BlobRemoveSizeTotalMetric])
	})
}

func TestServiceGetAndList(t *testing.T) {
	ctx := t.Context()

	t.Run("delegates to store", func(t *testing.T) {
		svc, consumerStore, _, _ := newTestService()

		space := testutil.RandomDID(t)
		provider := testutil.RandomDID(t)
		customer := testutil.RandomDID(t)
		cause := testutil.RandomCID(t)
		require.NoError(t, consumerStore.Add(ctx, provider, space, customer, "sub1", cause))

		bl := captypes.Blob{Digest: testutil.RandomMultihash(t), Size: 512}
		require.NoError(t, svc.Register(ctx, space, bl, cause))

		rec, err := svc.Get(ctx, space, bl.Digest)
		require.NoError(t, err)
		require.Equal(t, bl.Digest, rec.Blob.Digest)

		page, err := svc.List(ctx, space)
		require.NoError(t, err)
		require.Len(t, page.Results, 1)
	})
}
