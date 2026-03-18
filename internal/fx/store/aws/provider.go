package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/store/agent"
	awsagent "github.com/storacha/sprue/pkg/store/agent/aws"
	blobregistry "github.com/storacha/sprue/pkg/store/blob_registry"
	awsblobregistry "github.com/storacha/sprue/pkg/store/blob_registry/aws"
	"github.com/storacha/sprue/pkg/store/consumer"
	awsconsumer "github.com/storacha/sprue/pkg/store/consumer/aws"
	"github.com/storacha/sprue/pkg/store/customer"
	awscustomer "github.com/storacha/sprue/pkg/store/customer/aws"
	"github.com/storacha/sprue/pkg/store/delegation"
	awsdelegation "github.com/storacha/sprue/pkg/store/delegation/aws"
	"github.com/storacha/sprue/pkg/store/metrics"
	awsmetrics "github.com/storacha/sprue/pkg/store/metrics/aws"
	"github.com/storacha/sprue/pkg/store/replica"
	awsreplica "github.com/storacha/sprue/pkg/store/replica/aws"
	"github.com/storacha/sprue/pkg/store/revocation"
	awsrevocation "github.com/storacha/sprue/pkg/store/revocation/aws"
	spacediff "github.com/storacha/sprue/pkg/store/space_diff"
	awsspacediff "github.com/storacha/sprue/pkg/store/space_diff/aws"
	storageprovider "github.com/storacha/sprue/pkg/store/storage_provider"
	awsstorageprovider "github.com/storacha/sprue/pkg/store/storage_provider/aws"
	"github.com/storacha/sprue/pkg/store/subscription"
	awssubscription "github.com/storacha/sprue/pkg/store/subscription/aws"
	"github.com/storacha/sprue/pkg/store/upload"
	awsupload "github.com/storacha/sprue/pkg/store/upload/aws"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Module("aws-store",
	fx.Provide(
		NewDynamoDBClient,
		NewS3Client,
		fx.Annotate(
			NewAgentStore,
			fx.As(new(agent.Store)),
		),
		fx.Annotate(
			NewBlobRegistry,
			fx.As(new(blobregistry.Store)),
		),
		fx.Annotate(
			NewConsumerStore,
			fx.As(fx.Self()),
			fx.As(new(consumer.Store)),
		),
		fx.Annotate(
			NewCustomerStore,
			fx.As(new(customer.Store)),
		),
		fx.Annotate(
			NewDelegationStore,
			fx.As(new(delegation.Store)),
		),
		fx.Annotate(
			NewSpaceMetricsStore,
			fx.As(fx.Self()),
			fx.As(new(metrics.SpaceStore)),
		),
		fx.Annotate(
			NewAdminMetricsStore,
			fx.As(fx.Self()),
			fx.As(new(metrics.Store)),
		),
		fx.Annotate(
			NewReplicaStore,
			fx.As(new(replica.Store)),
		),
		fx.Annotate(
			NewRevocationStore,
			fx.As(new(revocation.Store)),
		),
		fx.Annotate(
			NewSpaceDiffStore,
			fx.As(fx.Self()),
			fx.As(new(spacediff.Store)),
		),
		fx.Annotate(
			NewStorageProviderStore,
			fx.As(new(storageprovider.Store)),
		),
		fx.Annotate(
			NewSubscriptionStore,
			fx.As(new(subscription.Store)),
		),
		fx.Annotate(
			NewUploadStore,
			fx.As(new(upload.Store)),
		),
	),
)

func NewDynamoDBClient(cfg *config.Config, logger *zap.Logger) (*dynamodb.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.DynamoDB.Region),
	}

	if cfg.DynamoDB.Endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(cfg.DynamoDB.Endpoint))
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "dummy",
				SecretAccessKey: "dummy",
			},
		}))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(awsCfg)
	logger.Info("initialized DynamoDB client",
		zap.String("endpoint", cfg.DynamoDB.Endpoint),
		zap.String("region", cfg.DynamoDB.Region),
	)
	return client, nil
}

func NewS3Client(cfg *config.Config, logger *zap.Logger) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.S3.Region),
	}

	if cfg.S3.Endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(cfg.S3.Endpoint))
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
			},
		}))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3.Endpoint != "" {
			o.UsePathStyle = true
		}
	})
	logger.Info("initialized S3 client",
		zap.String("endpoint", cfg.S3.Endpoint),
		zap.String("region", cfg.S3.Region),
	)
	return client, nil
}

func NewAgentStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client, s3 *s3.Client) *awsagent.Store {
	store := awsagent.New(dynamo, cfg.DynamoDB.AgentIndexTable, s3, cfg.S3.AgentMessageBucket)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return store.Shutdown(ctx)
		},
	})
	return store
}

func NewBlobRegistry(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client, consumerStore consumer.Store, spaceDiff *awsspacediff.Store, spaceMetrics *awsmetrics.SpaceStore, adminMetrics *awsmetrics.Store) *awsblobregistry.Store {
	store := awsblobregistry.New(dynamo, cfg.DynamoDB.BlobRegistryTable, consumerStore, spaceDiff, spaceMetrics, adminMetrics)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewConsumerStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsconsumer.Store {
	store := awsconsumer.New(dynamo, cfg.DynamoDB.ConsumerTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewCustomerStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awscustomer.Store {
	store := awscustomer.New(dynamo, cfg.DynamoDB.CustomerTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewDelegationStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client, s3Client *s3.Client) *awsdelegation.Store {
	store := awsdelegation.New(dynamo, cfg.DynamoDB.DelegationTable, s3Client, cfg.S3.DelegationBucket)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewSpaceMetricsStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsmetrics.SpaceStore {
	store := awsmetrics.NewSpaceStore(dynamo, cfg.DynamoDB.SpaceMetricsTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewAdminMetricsStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsmetrics.Store {
	store := awsmetrics.New(dynamo, cfg.DynamoDB.AdminMetricsTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewReplicaStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsreplica.Store {
	store := awsreplica.New(dynamo, cfg.DynamoDB.ReplicaTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewRevocationStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsrevocation.Store {
	store := awsrevocation.New(dynamo, cfg.DynamoDB.RevocationTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewSpaceDiffStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsspacediff.Store {
	store := awsspacediff.New(dynamo, cfg.DynamoDB.SpaceDiffTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewStorageProviderStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awsstorageprovider.Store {
	store := awsstorageprovider.New(dynamo, cfg.DynamoDB.StorageProviderTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewSubscriptionStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client) *awssubscription.Store {
	store := awssubscription.New(dynamo, cfg.DynamoDB.SubscriptionTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}

func NewUploadStore(lc fx.Lifecycle, cfg *config.Config, dynamo *dynamodb.Client, s3Client *s3.Client) *awsupload.Store {
	store := awsupload.New(dynamo, cfg.DynamoDB.UploadTable, s3Client, cfg.S3.UploadShardsBucket)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return store.Initialize(ctx)
		},
	})
	return store
}
