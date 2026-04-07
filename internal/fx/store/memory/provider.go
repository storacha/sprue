package memory

import (
	"github.com/storacha/sprue/pkg/blobregistry"
	memblobregistrysvc "github.com/storacha/sprue/pkg/blobregistry/memory"
	"github.com/storacha/sprue/pkg/store/agent"
	memagent "github.com/storacha/sprue/pkg/store/agent/memory"
	blobregistrystore "github.com/storacha/sprue/pkg/store/blob_registry"
	memblobregistry "github.com/storacha/sprue/pkg/store/blob_registry/memory"
	"github.com/storacha/sprue/pkg/store/consumer"
	memconsumer "github.com/storacha/sprue/pkg/store/consumer/memory"
	"github.com/storacha/sprue/pkg/store/customer"
	memcustomer "github.com/storacha/sprue/pkg/store/customer/memory"
	"github.com/storacha/sprue/pkg/store/delegation"
	memdelegation "github.com/storacha/sprue/pkg/store/delegation/memory"
	"github.com/storacha/sprue/pkg/store/metrics"
	memmetrics "github.com/storacha/sprue/pkg/store/metrics/memory"
	"github.com/storacha/sprue/pkg/store/replica"
	memreplica "github.com/storacha/sprue/pkg/store/replica/memory"
	"github.com/storacha/sprue/pkg/store/revocation"
	memrevocation "github.com/storacha/sprue/pkg/store/revocation/memory"
	spacediff "github.com/storacha/sprue/pkg/store/space_diff"
	memspacediff "github.com/storacha/sprue/pkg/store/space_diff/memory"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
	memstorageprovider "github.com/storacha/sprue/pkg/store/storage_provider/memory"
	"github.com/storacha/sprue/pkg/store/subscription"
	memsubscription "github.com/storacha/sprue/pkg/store/subscription/memory"
	"github.com/storacha/sprue/pkg/store/upload"
	memupload "github.com/storacha/sprue/pkg/store/upload/memory"
	"go.uber.org/fx"
)

var Module = fx.Module("memory-store",
	fx.Provide(
		NewAgentStore,
		NewBlobRegistry,
		fx.Annotate(
			NewBlobRegistryService,
			fx.As(new(blobregistry.Service)),
		),
		NewConsumerStore,
		NewCustomerStore,
		NewDelegationStore,
		NewSpaceMetricsStore,
		NewAdminMetricsStore,
		NewReplicaStore,
		NewRevocationStore,
		NewSpaceDiffStore,
		NewStorageProviderStore,
		NewSubscriptionStore,
		NewUploadStore,
	),
)

func NewAgentStore() agent.Store {
	return memagent.New()
}

func NewBlobRegistry() blobregistrystore.Store {
	return memblobregistry.New()
}

func NewBlobRegistryService(store blobregistrystore.Store, consumerStore consumer.Store, spaceDiffStore spacediff.Store, spaceMetrics metrics.SpaceStore, adminMetrics metrics.Store) *memblobregistrysvc.Service {
	return memblobregistrysvc.NewService(store, consumerStore, spaceDiffStore, spaceMetrics, adminMetrics)
}

func NewConsumerStore() consumer.Store {
	return memconsumer.New()
}

func NewCustomerStore() customer.Store {
	return memcustomer.New()
}

func NewDelegationStore() delegation.Store {
	return memdelegation.New()
}

func NewSpaceMetricsStore() metrics.SpaceStore {
	return memmetrics.NewSpaceStore()
}

func NewAdminMetricsStore() metrics.Store {
	return memmetrics.New()
}

func NewReplicaStore() replica.Store {
	return memreplica.New()
}

func NewRevocationStore() revocation.Store {
	return memrevocation.New()
}

func NewSpaceDiffStore() spacediff.Store {
	return memspacediff.New()
}

func NewStorageProviderStore() storageprovider.Store {
	return memstorageprovider.New()
}

func NewSubscriptionStore() subscription.Store {
	return memsubscription.New()
}

func NewUploadStore() upload.Store {
	return memupload.New()
}
