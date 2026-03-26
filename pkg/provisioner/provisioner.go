package provisioner

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/ipld/codec/cbor"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/lib/errors"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/customer"
	"github.com/storacha/sprue/pkg/store/subscription"
)

type (
	SpaceDID       = did.DID
	AccountDID     = did.DID
	ServiceDID     = did.DID
	SubscriptionID = string
)

const ProviderNotAllowedErrorName = "ProviderNotAllowed"

// ErrProviderNotAllowed is returned when a provider is not in the allowed list.
var ErrProviderNotAllowed = errors.New(ProviderNotAllowedErrorName, "provider not allowed")

type ProvisioningService struct {
	providers         []ServiceDID
	customerStore     customer.Store
	consumerStore     consumer.Store
	subscriptionStore subscription.Store
}

func New(providers []ServiceDID, customerStore customer.Store, consumerStore consumer.Store, subscriptionStore subscription.Store) *ProvisioningService {
	return &ProvisioningService{
		providers:         providers,
		customerStore:     customerStore,
		consumerStore:     consumerStore,
		subscriptionStore: subscriptionStore,
	}
}

// GetConsumer returns the consumer record for a given service provider and
// consumer (space). It may return [consumer.ErrConsumerNotFound].
func (s *ProvisioningService) GetConsumer(ctx context.Context, provider ServiceDID, space SpaceDID) (consumer.ConsumerRecord, error) {
	return s.consumerStore.Get(ctx, provider, space)
}

// GetCustomer returns the customer (account) that owns the consumer (space)
// for a given service provider. It may return [customer.ErrCustomerNotFound].
func (s *ProvisioningService) GetCustomer(ctx context.Context, provider ServiceDID, account AccountDID) (customer.CustomerRecord, error) {
	return s.customerStore.Get(ctx, account)
}

// GetSubscription returns the subscription record for a given service
// provider. It may return [subscription.ErrSubscriptionNotFound].
func (s *ProvisioningService) GetSubscription(ctx context.Context, provider ServiceDID, subscription SubscriptionID) (subscription.SubscriptionRecord, error) {
	return s.subscriptionStore.Get(ctx, provider, subscription)
}

// ListServiceProviders returns a list of services that have been provisioned
// for the given consumer (space).
func (s *ProvisioningService) ListServiceProviders(ctx context.Context, space SpaceDID) ([]ServiceDID, error) {
	var providers []ServiceDID
	page, err := s.consumerStore.List(ctx, space)
	if err != nil {
		return nil, err
	}
	for {
		for _, rec := range page.Results {
			providers = append(providers, rec.Provider)
		}
		if page.Cursor == nil {
			break
		}
		page, err = s.consumerStore.List(ctx, space, consumer.WithListCursor(*page.Cursor))
		if err != nil {
			return nil, err
		}
	}
	return providers, nil
}

// Provision provisions a service provider for a consumer (space) on behalf of
// a customer (account). It may return [customer.ErrCustomerNotFound]
// if the customer does not exist and [consumer.ErrConsumerExists] if the
// consumer is already provisioned for the provider.
func (s *ProvisioningService) Provision(ctx context.Context, account AccountDID, space SpaceDID, provider ServiceDID, cause cid.Cid) error {
	// Ensure the provider is allowed.
	if !slices.ContainsFunc(s.providers, func(p ServiceDID) bool {
		return p.String() == provider.String()
	}) {
		return ErrProviderNotAllowed
	}

	// Ensure the customer exists.
	_, err := s.customerStore.Get(ctx, account)
	if err != nil {
		return fmt.Errorf("getting customer: %w", err)
	}

	subscriptionID, err := NewSubscriptionID(space)
	if err != nil {
		return fmt.Errorf("creating subscription ID: %w", err)
	}

	if err := s.subscriptionStore.Add(ctx, provider, subscriptionID, account, cause); err != nil {
		if !errors.Is(err, subscription.ErrSubscriptionExists) {
			return fmt.Errorf("adding subscription: %w", err)
		}
	}

	return s.consumerStore.Add(ctx, provider, space, account, subscriptionID, cause)
}

func NewSubscriptionID(consumer SpaceDID) (string, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(1)
	if err != nil {
		return "", err
	}
	na, err := ma.AssembleEntry("consumer")
	if err != nil {
		return "", err
	}
	if err := na.AssignString(consumer.String()); err != nil {
		return "", err
	}
	if err := ma.Finish(); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return "", err
	}
	c, err := cid.Prefix{
		Version:  1,
		Codec:    cbor.Code,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(buf.Bytes())
	if err != nil {
		return "", err
	}
	return c.String(), nil
}
