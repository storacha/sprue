package handlers_test

import (
	"net/url"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/internal/testutil"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/lib/didmailto"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/routing"
	"github.com/storacha/sprue/pkg/service/handlers"
	agent_store "github.com/storacha/sprue/pkg/store/agent/memory"
	replica_store "github.com/storacha/sprue/pkg/store/replica/memory"
	storage_provider_store "github.com/storacha/sprue/pkg/store/storage_provider/memory"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestSpaceBlobReplicateHandler(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := t.Context()

	alice := testutil.Alice
	aliceAccount := testutil.Must(didmailto.Parse("did:mailto:example.com:alice"))(t)
	uploadService := testutil.WebService

	defaultCfg := config.DeploymentConfig{MaxReplicas: 3}

	t.Run("invalid space DID", func(t *testing.T) {
		spStore := storage_provider_store.New()
		router := routing.NewService(spStore, logger)
		blobReg, _ := newBlobRegistry()
		replicaStore := replica_store.New()
		agentStore := agent_store.New()
		nodeProvider := piriclient.NewProvider(uploadService, logger)

		handler := handlers.SpaceBlobReplicateHandler(
			defaultCfg, &identity.Identity{Signer: uploadService},
			router, blobReg, replicaStore, agentStore, nodeProvider, logger,
		)

		digest := testutil.RandomMultihash(t)
		blob := types.Blob{Digest: digest, Size: 1024}
		site := cidlink.Link{Cid: testutil.RandomCID(t)}

		cap := ucan.NewCapability(
			spaceblobcap.ReplicateAbility,
			"not-a-did",
			spaceblobcap.ReplicateCaveats{Blob: blob, Replicas: 2, Site: site},
		)

		inv, err := invocation.Invoke(alice, uploadService, cap)
		require.NoError(t, err)

		res, _, err := handler(ctx, cap, inv, nil)
		require.NoError(t, err)

		_, fail := result.Unwrap(res)
		require.NotNil(t, fail)

		model := datamodel.Bind(testutil.Must(fail.ToIPLD())(t))
		require.NotNil(t, model.Name)
		require.Equal(t, handlers.InvalidSpaceErrorName, *model.Name)
	})

	t.Run("replicas exceeds max", func(t *testing.T) {
		spStore := storage_provider_store.New()
		router := routing.NewService(spStore, logger)
		blobReg, _ := newBlobRegistry()
		replicaStore := replica_store.New()
		agentStore := agent_store.New()
		nodeProvider := piriclient.NewProvider(uploadService, logger)

		cfg := config.DeploymentConfig{MaxReplicas: 2}
		handler := handlers.SpaceBlobReplicateHandler(
			cfg, &identity.Identity{Signer: uploadService},
			router, blobReg, replicaStore, agentStore, nodeProvider, logger,
		)

		space := testutil.RandomSigner(t)
		digest := testutil.RandomMultihash(t)
		blob := types.Blob{Digest: digest, Size: 1024}
		site := cidlink.Link{Cid: testutil.RandomCID(t)}

		cap := ucan.NewCapability(
			spaceblobcap.ReplicateAbility,
			space.DID().String(),
			spaceblobcap.ReplicateCaveats{Blob: blob, Replicas: 3, Site: site},
		)

		inv, err := invocation.Invoke(alice, uploadService, cap)
		require.NoError(t, err)

		res, _, err := handler(ctx, cap, inv, nil)
		require.NoError(t, err)

		_, fail := result.Unwrap(res)
		require.NotNil(t, fail)

		model := datamodel.Bind(testutil.Must(fail.ToIPLD())(t))
		require.NotNil(t, model.Name)
		require.Equal(t, handlers.ReplicationCountRangeErrorName, *model.Name)
	})

	t.Run("blob not found in space", func(t *testing.T) {
		spStore := storage_provider_store.New()
		router := routing.NewService(spStore, logger)
		blobReg, _ := newBlobRegistry()
		replicaStore := replica_store.New()
		agentStore := agent_store.New()
		nodeProvider := piriclient.NewProvider(uploadService, logger)

		handler := handlers.SpaceBlobReplicateHandler(
			defaultCfg, &identity.Identity{Signer: uploadService},
			router, blobReg, replicaStore, agentStore, nodeProvider, logger,
		)

		space := testutil.RandomSigner(t)
		digest := testutil.RandomMultihash(t)
		blob := types.Blob{Digest: digest, Size: 1024}
		site := cidlink.Link{Cid: testutil.RandomCID(t)}

		cap := ucan.NewCapability(
			spaceblobcap.ReplicateAbility,
			space.DID().String(),
			spaceblobcap.ReplicateCaveats{Blob: blob, Replicas: 2, Site: site},
		)

		inv, err := invocation.Invoke(alice, uploadService, cap)
		require.NoError(t, err)

		res, _, err := handler(ctx, cap, inv, nil)
		require.NoError(t, err)

		_, fail := result.Unwrap(res)
		require.NotNil(t, fail)

		model := datamodel.Bind(testutil.Must(fail.ToIPLD())(t))
		require.NotNil(t, model.Name)
		require.Equal(t, handlers.ReplicationSourceNotFoundErrorName, *model.Name)
	})

	t.Run("invalid location commitment", func(t *testing.T) {
		spStore := storage_provider_store.New()
		router := routing.NewService(spStore, logger)
		blobReg, consumerStore := newBlobRegistry()
		replicaStore := replica_store.New()
		agentStore := agent_store.New()
		nodeProvider := piriclient.NewProvider(uploadService, logger)

		handler := handlers.SpaceBlobReplicateHandler(
			defaultCfg, &identity.Identity{Signer: uploadService},
			router, blobReg, replicaStore, agentStore, nodeProvider, logger,
		)

		space := testutil.RandomSigner(t)

		// provision the space
		err := consumerStore.Add(ctx, uploadService.DID(), space.DID(), aliceAccount, "", testutil.RandomCID(t))
		require.NoError(t, err)

		digest := testutil.RandomMultihash(t)
		blob := types.Blob{Digest: digest, Size: 1024}
		// random CID is not a valid location commitment delegation
		site := cidlink.Link{Cid: testutil.RandomCID(t)}

		// register the blob in the space
		err = blobReg.Register(ctx, space.DID(), blob, testutil.RandomCID(t))
		require.NoError(t, err)

		cap := ucan.NewCapability(
			spaceblobcap.ReplicateAbility,
			space.DID().String(),
			spaceblobcap.ReplicateCaveats{Blob: blob, Replicas: 2, Site: site},
		)

		inv, err := invocation.Invoke(alice, uploadService, cap)
		require.NoError(t, err)

		res, _, err := handler(ctx, cap, inv, nil)
		require.NoError(t, err)

		_, fail := result.Unwrap(res)
		require.NotNil(t, fail)

		model := datamodel.Bind(testutil.Must(fail.ToIPLD())(t))
		require.NotNil(t, model.Name)
		require.Equal(t, handlers.InvalidReplicationSiteErrorName, *model.Name)
	})

	t.Run("already fully replicated returns success", func(t *testing.T) {
		storageProvider := testutil.RandomSigner(t)
		storageProviderURL := testutil.Must(url.Parse("https://piri.example.com"))(t)
		storageProviderProof := delegateStorageProviderProof(t, storageProvider, uploadService)

		spStore := storage_provider_store.New()
		err := spStore.Put(ctx, *storageProviderURL, storageProviderProof, 100, nil)
		require.NoError(t, err)

		router := routing.NewService(spStore, logger)
		blobReg, consumerStore := newBlobRegistry()
		replicaStore := replica_store.New()
		agentStore := agent_store.New()
		nodeProvider := piriclient.NewProvider(uploadService, logger)

		handler := handlers.SpaceBlobReplicateHandler(
			defaultCfg, &identity.Identity{Signer: uploadService},
			router, blobReg, replicaStore, agentStore, nodeProvider, logger,
		)

		space := testutil.RandomSigner(t)

		// provision the space
		err = consumerStore.Add(ctx, uploadService.DID(), space.DID(), aliceAccount, "", testutil.RandomCID(t))
		require.NoError(t, err)

		digest := testutil.RandomMultihash(t)
		blob := types.Blob{Digest: digest, Size: 1024}
		site := cidlink.Link{Cid: testutil.RandomCID(t)}

		// register the blob in the space
		err = blobReg.Register(ctx, space.DID(), blob, testutil.RandomCID(t))
		require.NoError(t, err)

		// requesting 1 replica means source + 1 = 2 copies total
		// with 0 active replicas, newReplicasCount = 1 - (0 + 1) = 0
		// so no new allocations needed => success with no effects
		cap := ucan.NewCapability(
			spaceblobcap.ReplicateAbility,
			space.DID().String(),
			spaceblobcap.ReplicateCaveats{Blob: blob, Replicas: 1, Site: site},
		)

		inv, err := invocation.Invoke(alice, uploadService, cap)
		require.NoError(t, err)

		res, _, err := handler(ctx, cap, inv, nil)
		require.NoError(t, err)

		ok, fail := result.Unwrap(res)
		require.Nil(t, fail)
		require.NotNil(t, ok)
	})
}
