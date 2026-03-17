package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/server"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/service/handlers"
	"github.com/storacha/sprue/pkg/state"
	"github.com/storacha/sprue/pkg/store/agent"
)

// Service implements the sprue upload service logic.
type Service struct {
	cfg           *config.Config
	identity      *identity.Identity
	state         state.StateStore
	agentStore    agent.Store
	indexerClient *indexerclient.Client
	logger        *zap.Logger
	ucanServer    server.ServerView[server.Service]
}

// New creates a new Service instance.
func New(cfg *config.Config, id *identity.Identity, store state.StateStore, agentStore agent.Store, indexerClient *indexerclient.Client, logger *zap.Logger) (*Service, error) {
	svc := &Service{
		cfg:           cfg,
		identity:      id,
		state:         store,
		agentStore:    agentStore,
		indexerClient: indexerClient,
		logger:        logger,
	}

	// Create UCAN server with handlers
	ucanSrv, err := svc.createUCANServer()
	if err != nil {
		return nil, fmt.Errorf("failed to create UCAN server: %w", err)
	}
	svc.ucanServer = ucanSrv

	return svc, nil
}

// createUCANServer creates the UCAN RPC server with registered handlers.
func (s *Service) createUCANServer() (server.ServerView[server.Service], error) {
	// For local development, allow any principal to invoke any capability.
	// This bypasses strict authorization checks that require proper delegation chains.
	permissiveCanIssue := func(cap ucan.Capability[any], issuer did.DID) bool {
		s.logger.Debug("permissive auth check",
			zap.String("capability", cap.Can()),
			zap.String("resource", cap.With()),
			zap.String("issuer", issuer.String()),
		)
		return true // Allow all invocations
	}

	options := []server.Option{
		server.WithCanIssue(permissiveCanIssue),
		handlers.WithAccessAuthorizeMethod(s),
		handlers.WithAccessClaimMethod(s),
		handlers.WithAccessDelegateMethod(s),
		handlers.WithFilecoinOfferMethod(s),
		handlers.WithProviderAddMethod(s),
		handlers.WithSpaceBlobAddMethod(s),
		handlers.WithSpaceBlobReplicateMethod(s),
		handlers.WithSpaceIndexAddMethod(s),
		handlers.WithUploadAddMethod(s),
		handlers.WithUCANConcludeMethod(s),
	}

	return server.NewServer(s.identity.Signer, options...)
}

// HandleUCANRequest handles incoming UCAN RPC requests.
func (s *Service) HandleUCANRequest(c echo.Context) error {
	r := c.Request()

	inBytes, inMsg, inIdx, err := decodeAndIndex(r.Body)
	if err != nil {
		return fmt.Errorf("decoding and indexing incoming agent message: %w", err)
	}
	r.Body.Close()

	err = s.agentStore.Write(r.Context(), inMsg, inIdx, inBytes)
	if err != nil {
		return fmt.Errorf("writing incoming agent message to agent store: %w", err)
	}

	res, err := s.ucanServer.Request(r.Context(), ucanhttp.NewRequest(bytes.NewReader(inBytes), r.Header))
	if err != nil {
		s.logger.Error("UCAN request error", zap.Error(err))
		return fmt.Errorf("handling UCAN request: %w", err)
	}

	outBytes, outMsg, outIdx, err := decodeAndIndex(res.Body())
	if err != nil {
		return fmt.Errorf("decoding and indexing outgoing agent message: %w", err)
	}
	res.Body().Close()

	err = s.agentStore.Write(r.Context(), outMsg, outIdx, outBytes)
	if err != nil {
		return fmt.Errorf("writing outgoing agent message to agent store: %w", err)
	}

	// Copy response headers
	for key, vals := range res.Headers() {
		for _, v := range vals {
			c.Response().Header().Add(key, v)
		}
	}

	return c.Stream(res.Status(), "", bytes.NewReader(outBytes))
}

func decodeAndIndex(r io.Reader) ([]byte, message.AgentMessage, []agent.IndexEntry, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reading request body: %w", err)
	}
	roots, blocks, err := car.Decode(bytes.NewReader(body))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decoding CAR: %w", err)
	}
	if len(roots) != 1 {
		return nil, nil, nil, fmt.Errorf("expected exactly one root in CAR, got %d", len(roots))
	}
	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(blocks))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating block reader: %w", err)
	}
	msg, err := message.NewMessage(roots[0], br)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating agent message: %w", err)
	}
	var entries []agent.IndexEntry
	for ent, err := range agent.Index(msg) {
		if err != nil {
			return nil, nil, nil, fmt.Errorf("indexing agent message: %w", err)
		}
		entries = append(entries, ent)
	}
	return body, msg, entries, nil
}

// HandleReceiptRequest handles receipt retrieval requests.
func (s *Service) HandleReceiptRequest(c echo.Context) error {
	task, err := cid.Parse(c.Param("cid"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("invalid task CID: %v", err),
		})
	}

	s.logger.Debug("receipt request", zap.String("task", task.String()))
	rcpt, err := s.agentStore.GetReceipt(c.Request().Context(), task)
	if err != nil {
		if errors.Is(err, agent.ErrReceiptNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "receipt not found",
			})
		}
		return fmt.Errorf("getting receipt: %w", err)
	}

	// Build an agent message containing the receipt
	msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
	if err != nil {
		s.logger.Error("failed to build message", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to build message",
		})
	}

	reader := car.Encode([]ipld.Link{msg.Root().Link()}, msg.Blocks())
	return c.Stream(http.StatusOK, car.ContentType, reader)
}

// Identity returns the service identity.
func (s *Service) Identity() *identity.Identity {
	return s.identity
}

// State returns the state store.
func (s *Service) State() state.StateStore {
	return s.state
}

// Config returns the service configuration.
func (s *Service) Config() *config.Config {
	return s.cfg
}

// Logger returns the logger.
func (s *Service) Logger() *zap.Logger {
	return s.logger
}

// ID returns the service's signer (implements handler interfaces).
func (s *Service) ID() principal.Signer {
	return s.identity.Signer
}

// IndexerClient returns the indexer client (may be nil if not configured).
func (s *Service) IndexerClient() *indexerclient.Client {
	return s.indexerClient
}

// PiriClient queries the provider table and creates a piri client on-demand.
// This ensures we always use the most up-to-date provider information.
func (s *Service) PiriClient(ctx context.Context) (*piriclient.Client, error) {
	// Query provider from DynamoDB (delegator-provider-info table)
	provider, err := s.state.GetFirstProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get provider from state: %w", err)
	}

	if provider == nil || provider.DID == (did.DID{}) || provider.Endpoint == nil {
		s.logger.Debug("no storage provider registered")
		return nil, nil
	}

	// Create a new piri client for this provider
	client, err := piriclient.New(provider.Endpoint, provider.DID, s.identity.Signer, s.state, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create piri client: %w", err)
	}

	s.logger.Debug("created piri client from provider info",
		zap.String("did", provider.DID.String()),
		zap.String("endpoint", provider.Endpoint.String()),
	)

	return client, nil
}
