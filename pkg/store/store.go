package store

import (
	"context"
	"fmt"
)

// Page is a generic type representing a paginated response from the store.
type Page[T any] struct {
	Cursor  *string
	Results []T
}

type PaginationConfig struct {
	// Cursor is an optional string that indicates where to start the page. This
	// is typically the ID of the last item from the previous page.
	Cursor *string
	// Limit is an optional integer that specifies the maximum number of items to
	// return in the page. If not provided, a default limit may be applied by the
	// implementation.
	Limit *int
}

type GetPageFunc[T any] func(ctx context.Context, options PaginationConfig) (Page[T], error)

func Collect[T any](ctx context.Context, getPage GetPageFunc[T]) ([]T, error) {
	var items []T
	paginationOptions := PaginationConfig{}
	i := 0
	for {
		page, err := getPage(ctx, paginationOptions)
		if err != nil {
			return nil, fmt.Errorf("getting page %d: %w", i, err)
		}
		items = append(items, page.Results...)
		if page.Cursor == nil || len(page.Results) == 0 {
			break
		}
		paginationOptions.Cursor = page.Cursor
		i++
	}
	return items, nil
}
