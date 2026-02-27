package fx

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/dynamo"
	"github.com/storacha/sprue/pkg/state"
)

// StoreModule provides the DynamoDB state store.
var StoreModule = fx.Module("store",
	fx.Provide(NewStateStore),
)

// NewStateStore creates a DynamoDB-backed state store.
func NewStateStore(cfg *config.Config, logger *zap.Logger) (state.StateStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := dynamo.New(ctx, dynamo.Config{
		Endpoint:           cfg.DynamoDB.Endpoint,
		Region:             cfg.DynamoDB.Region,
		ProviderInfoTable:  cfg.DynamoDB.ProviderTable,
		AllocationsTable:   cfg.DynamoDB.AllocationsTable,
		ReceiptsTable:      cfg.DynamoDB.ReceiptsTable,
		AuthRequestsTable:  cfg.DynamoDB.AuthRequestsTable,
		ProvisioningsTable: cfg.DynamoDB.ProvisioningsTable,
		UploadsTable:       cfg.DynamoDB.UploadsTable,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("creating DynamoDB store: %w", err)
	}

	logger.Info("connected to DynamoDB store",
		zap.String("endpoint", cfg.DynamoDB.Endpoint),
		zap.String("allocations_table", cfg.DynamoDB.AllocationsTable),
		zap.String("receipts_table", cfg.DynamoDB.ReceiptsTable),
	)

	return store, nil
}
