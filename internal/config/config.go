package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds the sprue service configuration.
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Identity IdentityConfig `mapstructure:"identity"`
	Indexer  IndexerConfig  `mapstructure:"indexer"`
	DynamoDB DynamoDBConfig `mapstructure:"dynamodb"`
	S3       S3Config       `mapstructure:"s3"`
	Log      LogConfig      `mapstructure:"log"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// IdentityConfig holds service identity settings.
type IdentityConfig struct {
	// KeyFile is the path to an Ed25519 PEM key file for service identity.
	// Takes precedence over PrivateKey if set.
	KeyFile string `mapstructure:"key_file"`

	// PrivateKey is the base64-encoded ed25519 private key for service identity.
	// If empty and KeyFile is not set, a key will be generated at startup.
	PrivateKey string `mapstructure:"private_key"`

	// ServiceDID is the did:web identity for this service (e.g., did:web:upload).
	// When set with KeyFile, the service will wrap its did:key signer with this did:web
	// identity, allowing it to accept UCANs addressed to the did:web.
	ServiceDID string `mapstructure:"service_did"`
}

// IndexerConfig holds indexer service settings.
type IndexerConfig struct {
	// Endpoint is the URL of the indexing service.
	Endpoint string `mapstructure:"endpoint"`

	// DID is the DID of the indexer service (optional, can be derived from endpoint).
	DID string `mapstructure:"did"`
}

// DynamoDBConfig holds DynamoDB settings.
type DynamoDBConfig struct {
	// Endpoint is the DynamoDB endpoint (for local development).
	Endpoint string `mapstructure:"endpoint"`

	// Region is the AWS region for DynamoDB.
	Region string `mapstructure:"region"`

	// ProviderTable is the table name for provider info.
	ProviderTable string `mapstructure:"provider_table"`

	// AllocationsTable is the table name for blob allocations.
	AllocationsTable string `mapstructure:"allocations_table"`

	// ReceiptsTable is the table name for UCAN receipts.
	ReceiptsTable string `mapstructure:"receipts_table"`

	// AuthRequestsTable is the table name for auth requests.
	AuthRequestsTable string `mapstructure:"auth_requests_table"`

	// ProvisioningsTable is the table name for space provisionings.
	ProvisioningsTable string `mapstructure:"provisionings_table"`

	// UploadsTable is the table name for uploads.
	UploadsTable string `mapstructure:"uploads_table"`

	AgentIndexTable      string `mapstructure:"agent_index_table"`
	BlobRegistryTable    string `mapstructure:"blob_registry_table"`
	ConsumerTable        string `mapstructure:"consumer_table"`
	CustomerTable        string `mapstructure:"customer_table"`
	DelegationTable      string `mapstructure:"delegation_table"`
	SpaceMetricsTable    string `mapstructure:"space_metrics_table"`
	AdminMetricsTable    string `mapstructure:"admin_metrics_table"`
	ReplicaTable         string `mapstructure:"replica_table"`
	RevocationTable      string `mapstructure:"revocation_table"`
	StorageProviderTable string `mapstructure:"storage_provider_table"`
	SubscriptionTable    string `mapstructure:"subscription_table"`
	SpaceDiffTable       string `mapstructure:"space_diff_table"`
	UploadTable          string `mapstructure:"upload_table"`
}

// S3Config holds S3 settings.
type S3Config struct {
	// Endpoint is the S3 endpoint (for local development).
	Endpoint string `mapstructure:"endpoint"`

	// Region is the AWS region for S3.
	Region string `mapstructure:"region"`

	AgentMessageBucket string `mapstructure:"agent_message_bucket"`
	DelegationBucket   string `mapstructure:"delegation_bucket"`
	UploadShardsBucket string `mapstructure:"upload_shards_bucket"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	// Level controls logging verbosity (debug, info, warn, error).
	Level string `mapstructure:"level"`
}

// SetDefaults configures default values for viper.
func SetDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)

	// Indexer defaults (port 80 for did:web resolution in Docker)
	v.SetDefault("indexer.endpoint", "http://indexer:80")

	// DynamoDB defaults
	v.SetDefault("dynamodb.endpoint", "http://dynamodb-local:8000")
	v.SetDefault("dynamodb.region", "us-west-1")
	v.SetDefault("dynamodb.provider_table", "delegator-provider-info")
	v.SetDefault("dynamodb.allocations_table", "upload-allocations")
	v.SetDefault("dynamodb.receipts_table", "upload-receipts")
	v.SetDefault("dynamodb.auth_requests_table", "upload-auth-requests")
	v.SetDefault("dynamodb.provisionings_table", "upload-provisionings")
	v.SetDefault("dynamodb.uploads_table", "upload-uploads")

	v.SetDefault("dynamodb.agent_index_table", "agent-index")
	v.SetDefault("dynamodb.blob_registry_table", "blob-registry")
	v.SetDefault("dynamodb.consumer_table", "consumer")
	v.SetDefault("dynamodb.customer_table", "customer")
	v.SetDefault("dynamodb.delegation_table", "delegation")
	v.SetDefault("dynamodb.space_metrics_table", "space-metrics")
	v.SetDefault("dynamodb.admin_metrics_table", "admin-metrics")
	v.SetDefault("dynamodb.replica_table", "replica")
	v.SetDefault("dynamodb.revocation_table", "revocation")
	v.SetDefault("dynamodb.storage_provider_table", "storage-provider")
	v.SetDefault("dynamodb.subscription_table", "subscription")
	v.SetDefault("dynamodb.space_diff_table", "space-diff")
	v.SetDefault("dynamodb.upload_table", "upload")

	// S3 defaults
	v.SetDefault("s3.endpoint", "http://minio:9000")
	v.SetDefault("s3.region", "us-west-1")
	v.SetDefault("s3.agent_message_bucket", "agent-message")
	v.SetDefault("s3.delegation_bucket", "delegation")
	v.SetDefault("s3.upload_shards_bucket", "upload-shards")

	// Log defaults
	v.SetDefault("log.level", "info")
}

// BindEnvVars sets up environment variable binding with SPRUE_ prefix.
func BindEnvVars(v *viper.Viper) {
	v.SetEnvPrefix("SPRUE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
}

// Load creates a viper instance and loads configuration from the given config file
// (if provided), environment variables, and defaults.
func Load(configFile string) (*Config, error) {
	v := viper.New()

	SetDefaults(v)
	BindEnvVars(v)

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
	} else {
		// Look for config in standard locations
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/sprue/")
		// Ignore error if no config file found - use defaults and env vars
		_ = v.ReadInConfig()
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	return &cfg, nil
}

// LoadWithViper creates a viper instance and loads configuration, returning both
// the Config struct and the viper instance for flag binding.
func LoadWithViper(configFile string) (*Config, *viper.Viper, error) {
	v := viper.New()

	SetDefaults(v)
	BindEnvVars(v)

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, nil, fmt.Errorf("reading config file: %w", err)
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/sprue/")
		_ = v.ReadInConfig()
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	return &cfg, v, nil
}
