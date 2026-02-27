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

	// Log defaults
	v.SetDefault("log.level", "info")
}

// BindEnvVars sets up environment variable binding with UPLOAD_ prefix.
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
