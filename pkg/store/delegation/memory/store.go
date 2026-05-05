package memory

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	cid "github.com/ipfs/go-cid"
	"github.com/storacha/sprue/pkg/store"
	dlgstore "github.com/storacha/sprue/pkg/store/delegation"
)

type Store struct {
	mutex       sync.RWMutex
	delegations map[did.DID][]ucan.Delegation
}

var _ dlgstore.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		delegations: map[did.DID][]ucan.Delegation{},
	}
}

func (s *Store) ListByAudience(ctx context.Context, audience did.DID, options ...dlgstore.ListByAudienceOption) (store.Page[ucan.Delegation], error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	limit := 1000
	cfg := dlgstore.ListByAudienceConfig{Limit: &limit}
	for _, opt := range options {
		opt(&cfg)
	}
	delegations := slices.Clone(s.delegations[audience])
	if cfg.Cursor != nil {
		for i, d := range delegations {
			if d.Link().String() == *cfg.Cursor {
				if i+1 < len(delegations) {
					delegations = delegations[i+1:]
				}
				break
			}
		}
	}
	var cursor *string
	if cfg.Limit != nil && len(delegations) > *cfg.Limit {
		delegations = delegations[:*cfg.Limit]
		last := delegations[len(delegations)-1].Link().String()
		cursor = &last
	}
	return store.Page[ucan.Delegation]{
		Cursor:  cursor,
		Results: delegations,
	}, nil

}

func (s *Store) PutMany(ctx context.Context, delegations []ucan.Delegation, cause cid.Cid) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, d := range delegations {
		aud := d.Audience().DID()
		s.delegations[aud] = append(s.delegations[aud], d)
		slices.SortFunc(s.delegations[aud], func(a, b ucan.Delegation) int {
			return bytes.Compare(a.Link().Bytes(), b.Link().Bytes())
		})
	}
	return nil
}
