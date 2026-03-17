package storageprovider_test

import (
	"context"
	"net/url"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/sprue/pkg/internal/testutil"
	"github.com/storacha/sprue/pkg/store"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
	storageprovideraws "github.com/storacha/sprue/pkg/store/storage_provider/aws"
	"github.com/storacha/sprue/pkg/store/storage_provider/memory"
	"github.com/stretchr/testify/require"
)

type StoreKind string

const (
	Memory StoreKind = "memory"
	AWS    StoreKind = "aws"
)

var storeKinds = []StoreKind{Memory, AWS}

func makeStore(t *testing.T, k StoreKind) storageprovider.Store {
	switch k {
	case Memory:
		return memory.New()
	case AWS:
		return createAWSStore(t)
	}
	panic("unknown store kind")
}

func createAWSStore(t *testing.T) storageprovider.Store {
	// This test expects docker to be running in linux CI environments and fails if it's not
	if testutil.IsRunningInCI(t) && runtime.GOOS == "linux" {
		if !testutil.IsDockerAvailable(t) {
			t.Fatalf("docker is expected in CI linux testing environments, but wasn't found")
		}
	}
	// otherwise this test is running locally, skip it if docker isn't available
	if !testutil.IsDockerAvailable(t) {
		t.SkipNow()
	}

	dynamoEndpoint := testutil.CreateDynamo(t)
	dynamo := testutil.NewDynamoClient(t, dynamoEndpoint)

	s := storageprovideraws.New(dynamo, "storage-provider-"+uuid.NewString())
	require.NoError(t, s.Initialize(t.Context()))
	return s
}

// randomEndpoint returns a random HTTPS endpoint URL.
func randomEndpoint(t *testing.T) url.URL {
	t.Helper()
	u, err := url.Parse("https://" + uuid.NewString() + ".example.com")
	require.NoError(t, err)
	return *u
}

// makeProof creates a delegation from Alice to a random audience.
func makeProof(t *testing.T) delegation.Delegation {
	t.Helper()
	audience := testutil.RandomSigner(t)
	dlg, err := delegation.Delegate(
		testutil.Alice,
		audience,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("blob/allocate", testutil.Alice.DID().String(), ucan.NoCaveats{}),
		},
		delegation.WithNonce(uuid.NewString()),
	)
	require.NoError(t, err)
	return dlg
}

func TestStorageProviderStore(t *testing.T) {
	for _, k := range storeKinds {
		t.Run(string(k), func(t *testing.T) {
			s := makeStore(t, k)

			t.Run("puts and gets a provider", func(t *testing.T) {
				provider := testutil.RandomDID(t)
				endpoint := randomEndpoint(t)
				proof := makeProof(t)

				require.NoError(t, s.Put(t.Context(), provider, endpoint, proof, 10, 5))

				rec, err := s.Get(t.Context(), provider)
				require.NoError(t, err)
				require.Equal(t, provider, rec.Provider)
				require.Equal(t, endpoint, rec.Endpoint)
				require.Equal(t, proof.Root().Link(), rec.Proof.Root().Link())
				require.Equal(t, 10, rec.Weight)
				require.Equal(t, 5, rec.ReplicationWeight)
				require.False(t, rec.InsertedAt.IsZero())
			})

			t.Run("put updates an existing provider", func(t *testing.T) {
				provider := testutil.RandomDID(t)
				endpoint1 := randomEndpoint(t)
				endpoint2 := randomEndpoint(t)
				proof1 := makeProof(t)
				proof2 := makeProof(t)

				require.NoError(t, s.Put(t.Context(), provider, endpoint1, proof1, 10, 5))
				require.NoError(t, s.Put(t.Context(), provider, endpoint2, proof2, 20, 15))

				rec, err := s.Get(t.Context(), provider)
				require.NoError(t, err)
				require.Equal(t, endpoint2, rec.Endpoint)
				require.Equal(t, proof2.Root().Link(), rec.Proof.Root().Link())
				require.Equal(t, 20, rec.Weight)
				require.Equal(t, 15, rec.ReplicationWeight)
			})

			t.Run("Get returns ErrStorageProviderNotFound for unknown provider", func(t *testing.T) {
				provider := testutil.RandomDID(t)

				_, err := s.Get(t.Context(), provider)
				require.ErrorIs(t, err, storageprovider.ErrStorageProviderNotFound)
			})

			t.Run("deletes a provider", func(t *testing.T) {
				provider := testutil.RandomDID(t)
				endpoint := randomEndpoint(t)
				proof := makeProof(t)

				require.NoError(t, s.Put(t.Context(), provider, endpoint, proof, 10, 5))
				require.NoError(t, s.Delete(t.Context(), provider))

				_, err := s.Get(t.Context(), provider)
				require.ErrorIs(t, err, storageprovider.ErrStorageProviderNotFound)
			})

			t.Run("Delete returns ErrStorageProviderNotFound for unknown provider", func(t *testing.T) {
				provider := testutil.RandomDID(t)

				err := s.Delete(t.Context(), provider)
				require.ErrorIs(t, err, storageprovider.ErrStorageProviderNotFound)
			})

			t.Run("List includes added providers", func(t *testing.T) {
				provider1 := testutil.RandomDID(t)
				provider2 := testutil.RandomDID(t)
				endpoint := randomEndpoint(t)
				proof := makeProof(t)

				require.NoError(t, s.Put(t.Context(), provider1, endpoint, proof, 10, 5))
				require.NoError(t, s.Put(t.Context(), provider2, endpoint, proof, 10, 5))

				all, err := store.Collect(t.Context(), func(ctx context.Context, opts store.PaginationConfig) (store.Page[storageprovider.StorageProviderRecord], error) {
					var listOpts []storageprovider.ListOption
					if opts.Cursor != nil {
						listOpts = append(listOpts, storageprovider.WithListCursor(*opts.Cursor))
					}
					return s.List(ctx, listOpts...)
				})
				require.NoError(t, err)

				ids := make([]string, 0, len(all))
				for _, r := range all {
					ids = append(ids, r.Provider.String())
				}
				require.Contains(t, ids, provider1.String())
				require.Contains(t, ids, provider2.String())
			})

			t.Run("List paginates results", func(t *testing.T) {
				endpoint := randomEndpoint(t)
				proof := makeProof(t)
				for range 5 {
					require.NoError(t, s.Put(t.Context(), testutil.RandomDID(t), endpoint, proof, 10, 5))
				}

				all, err := store.Collect(t.Context(), func(ctx context.Context, opts store.PaginationConfig) (store.Page[storageprovider.StorageProviderRecord], error) {
					listOpts := []storageprovider.ListOption{storageprovider.WithListLimit(2)}
					if opts.Cursor != nil {
						listOpts = append(listOpts, storageprovider.WithListCursor(*opts.Cursor))
					}
					return s.List(ctx, listOpts...)
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(all), 5)
			})

			t.Run("deleted provider does not appear in List", func(t *testing.T) {
				provider := testutil.RandomDID(t)
				endpoint := randomEndpoint(t)
				proof := makeProof(t)

				require.NoError(t, s.Put(t.Context(), provider, endpoint, proof, 10, 5))
				require.NoError(t, s.Delete(t.Context(), provider))

				all, err := store.Collect(t.Context(), func(ctx context.Context, opts store.PaginationConfig) (store.Page[storageprovider.StorageProviderRecord], error) {
					var listOpts []storageprovider.ListOption
					if opts.Cursor != nil {
						listOpts = append(listOpts, storageprovider.WithListCursor(*opts.Cursor))
					}
					return s.List(ctx, listOpts...)
				})
				require.NoError(t, err)

				for _, r := range all {
					require.NotEqual(t, provider, r.Provider)
				}
			})
		})
	}
}
