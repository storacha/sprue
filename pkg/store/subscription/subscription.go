package subscription

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/lib/errors"
	"github.com/storacha/sprue/pkg/store"
)

const (
	// SubscriptionExistsErrorName is the name given to an error where the subscription
	// already exists in the store.
	SubscriptionExistsErrorName = "SubscriptionExists"
	// SubscriptionNotFoundErrorName is the name given to an error where the subscription
	// is not found in the store.
	SubscriptionNotFoundErrorName = "SubscriptionNotFound"
)

var (
	ErrSubscriptionExists   = errors.New(SubscriptionExistsErrorName, "subscription already exists")
	ErrSubscriptionNotFound = errors.New(SubscriptionNotFoundErrorName, "subscription not found")
)

type (
	ListByProviderAndCustomerConfig = store.PaginationConfig
	ListByProviderAndCustomerOption func(cfg *ListByProviderAndCustomerConfig)
)

func WithListByProviderAndCustomerLimit(limit int) ListByProviderAndCustomerOption {
	return func(cfg *ListByProviderAndCustomerConfig) {
		cfg.Limit = &limit
	}
}

func WithListByProviderAndCustomerCursor(cursor string) ListByProviderAndCustomerOption {
	return func(cfg *ListByProviderAndCustomerConfig) {
		cfg.Cursor = &cursor
	}
}

type SubscriptionRecord struct {
	// DID of the provider who services this subscription
	Provider did.DID
	// ID of this subscription - should be unique per-provider
	Subscription string
	// DID of the customer who maintains this subscription
	Customer did.DID
	// CID of the invocation that created this subscription
	Cause cid.Cid
	// Timestamp of when this subscription was created
	InsertedAt time.Time
}

type Store interface {
	// Get a subscription by provider DID and subscription ID. May return
	// [ErrSubscriptionNotFound].
	Get(ctx context.Context, provider did.DID, subscription string) (SubscriptionRecord, error)
	// Add a subscription - a relationship between a customer and a provider that
	// will allow for provisioning of consumers. May return [ErrSubscriptionExists]
	// if the subscription already exists.
	Add(ctx context.Context, provider did.DID, subscription string, customer did.DID, cause cid.Cid) error
	// A list of the subscriptions a customer has with a provider.
	ListByProviderAndCustomer(ctx context.Context, provider did.DID, customer did.DID, options ...ListByProviderAndCustomerOption) (store.Page[SubscriptionRecord], error)
}
