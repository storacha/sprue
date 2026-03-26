package fx_test

import (
	"runtime"
	"testing"

	"github.com/google/uuid"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/sprue/internal/config"
	appfx "github.com/storacha/sprue/internal/fx"
	"github.com/storacha/sprue/internal/testutil"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// Test that the app can be wired with all modules and dependencies.
func TestWireApp(t *testing.T) {
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
	s3Endpoint := testutil.CreateS3(t)

	appID := uuid.NewString()
	conf := config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: 0,
		},
		Identity: config.IdentityConfig{
			PrivateKey: testutil.Must(ed25519.Format(testutil.WebService))(t),
			ServiceDID: testutil.WebService.DID().String(),
		},
		Indexer: config.IndexerConfig{
			Endpoint: "http://localhost:3000",
		},
		DynamoDB: config.DynamoDBConfig{
			Region:             "us-east-1",
			Endpoint:           dynamoEndpoint.String(),
			ProviderTable:      "provider-" + appID,
			AllocationsTable:   "allocations-" + appID,
			ReceiptsTable:      "receipts-" + appID,
			AuthRequestsTable:  "auth-requests-" + appID,
			ProvisioningsTable: "provisionings-" + appID,
			UploadsTable:       "uploads-" + appID,
			// w3infra tables
			AgentIndexTable:      "agent-index-" + appID,
			BlobRegistryTable:    "blob-registry-" + appID,
			ConsumerTable:        "consumer-" + appID,
			CustomerTable:        "customer-" + appID,
			DelegationTable:      "delegation-" + appID,
			SpaceMetricsTable:    "space-metrics-" + appID,
			AdminMetricsTable:    "admin-metrics-" + appID,
			ReplicaTable:         "replica-" + appID,
			RevocationTable:      "revocation-" + appID,
			StorageProviderTable: "storage-provider-" + appID,
			SubscriptionTable:    "subscription-" + appID,
			SpaceDiffTable:       "space-diff-" + appID,
			UploadTable:          "upload-" + appID,
		},
		S3: config.S3Config{
			Region:             "us-east-1",
			Endpoint:           s3Endpoint.String(),
			AgentMessageBucket: "agent-message-" + appID,
			DelegationBucket:   "delegation-" + appID,
			UploadShardsBucket: "upload-shards-" + appID,
		},
		Mailer: config.MailerConfig{
			Type: "nop",
		},
		Log: config.LogConfig{
			Level: "debug",
		},
	}

	app := fxtest.New(
		t,
		fx.Supply(&conf),
		appfx.AppModule,
		fx.NopLogger,
	)

	app.RequireStart()
	app.RequireStop()
}
