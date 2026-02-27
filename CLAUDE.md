# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Build the binary
go build -o sprue ./cmd/main.go

# Run the service
./sprue serve
./sprue serve -c config.yaml

# Run tests
go test ./...

# Run a specific test
go test ./pkg/service/... -run TestName -v

# Build Docker image
docker build -t sprue .
```

## Architecture Overview

Sprue is the upload coordination service for Storacha local development. It routes blob allocations to Piri storage nodes and tracks upload state in DynamoDB.

### Core Components

**Dependency Injection (internal/fx/)**
- Uses uber-go/fx for dependency injection
- `AppModule` in `app.go` aggregates all modules: Config, Logger, Identity, Store, Clients, Service, Server
- Each module (e.g., `ConfigModule`, `ServerModule`) provides its dependencies via `fx.Provide` and hooks via `fx.Invoke`

**UCAN RPC Service (pkg/service/)**
- `Service` struct wraps a go-ucanto server that handles UCAN RPC requests
- Handlers in `pkg/service/handlers/` implement UCAP capabilities (e.g., `space/blob/add`, `upload/add`)
- Each handler follows the pattern: `With<Capability>Method(s ServiceInterface) server.Option`
- Handler service interfaces define the dependencies each handler needs (e.g., `SpaceBlobAddService`)

**State Management (pkg/state/)**
- `StateStore` interface defines all storage operations (allocations, receipts, auth requests, etc.)
- DynamoDB implementation in `pkg/dynamo/store.go`
- Key types: `Allocation`, `Upload`, `StoredReceipt`, `Provider`, `AuthRequest`, `Provisioning`

**External Clients (pkg/)**
- `piriclient`: Communicates with Piri storage nodes for blob allocation/acceptance
- `indexerclient`: Communicates with the indexing service

### HTTP Endpoints (internal/fx/server.go)

- `GET /` - Service info (DID, version)
- `GET /health` - Health check
- `GET /.well-known/did.json` - DID document for did:web resolution
- `POST /` - UCAN RPC endpoint
- `GET /receipt/:cid` - Receipt retrieval

### Configuration

Configuration via YAML file or environment variables with `UPLOAD_` prefix:
- `UPLOAD_SERVER_HOST`, `UPLOAD_SERVER_PORT`
- `UPLOAD_IDENTITY_KEY_FILE`, `UPLOAD_IDENTITY_PRIVATE_KEY`, `UPLOAD_IDENTITY_SERVICE_DID`
- `UPLOAD_PIRI_ENDPOINT`, `UPLOAD_INDEXER_ENDPOINT`
- `UPLOAD_DYNAMODB_*` for DynamoDB settings

Legacy env vars without prefix (e.g., `HOST`, `PORT`, `KEY_FILE`) also supported.

### Key Dependencies

- **go-ucanto**: UCAN RPC framework for capability-based authorization
- **go-libstoracha**: Storacha capability definitions (blob, space, upload, etc.)
- **echo/v4**: HTTP server framework
- **aws-sdk-go-v2**: DynamoDB client
- **viper/cobra**: Configuration and CLI
