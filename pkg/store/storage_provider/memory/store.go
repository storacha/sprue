package memory

import (
	"context"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/store"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
)

type Store struct {
	mutex     sync.RWMutex
	providers map[did.DID]storageprovider.StorageProviderRecord
}

var _ storageprovider.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		providers: map[did.DID]storageprovider.StorageProviderRecord{},
	}
}

func (s *Store) Delete(ctx context.Context, providerID did.DID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.providers[providerID]; !ok {
		return storageprovider.ErrStorageProviderNotFound
	}
	delete(s.providers, providerID)
	return nil
}

func (s *Store) Get(ctx context.Context, providerID did.DID) (storageprovider.StorageProviderRecord, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if sp, ok := s.providers[providerID]; ok {
		return sp, nil
	}
	return storageprovider.StorageProviderRecord{}, storageprovider.ErrStorageProviderNotFound
}

func (s *Store) List(ctx context.Context, options ...storageprovider.ListOption) (store.Page[storageprovider.StorageProviderRecord], error) {
	cfg := storageprovider.ListConfig{}
	for _, opt := range options {
		opt(&cfg)
	}

	limit := 1000
	if cfg.Limit != nil {
		limit = *cfg.Limit
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	records := slices.Collect(maps.Values(s.providers))
	slices.SortFunc(records, func(a, b storageprovider.StorageProviderRecord) int {
		return strings.Compare(a.Provider.String(), b.Provider.String())
	})

	if cfg.Cursor != nil {
		for i, r := range records {
			if r.Provider.String() == *cfg.Cursor {
				records = records[i+1:]
				break
			}
		}
	}

	var cursor *string
	if len(records) > limit {
		records = records[:limit]
		c := records[len(records)-1].Provider.String()
		cursor = &c
	}

	return store.Page[storageprovider.StorageProviderRecord]{Results: records, Cursor: cursor}, nil
}

func (s *Store) Put(ctx context.Context, providerID did.DID, endpoint url.URL, proof delegation.Delegation, weight int, replicationWeight int) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if sp, ok := s.providers[providerID]; ok {
		sp.Endpoint = endpoint
		sp.Proof = proof
		sp.Weight = weight
		sp.ReplicationWeight = replicationWeight
		sp.UpdatedAt = time.Now()
		s.providers[providerID] = sp
		return nil
	}
	s.providers[providerID] = storageprovider.StorageProviderRecord{
		Provider:          providerID,
		Endpoint:          endpoint,
		Proof:             proof,
		Weight:            weight,
		ReplicationWeight: replicationWeight,
		InsertedAt:        time.Now(),
	}
	return nil
}
