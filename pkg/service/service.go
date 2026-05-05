package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/fil-forge/libforge/didmailto"
	"github.com/fil-forge/ucantone/ipld"
	"github.com/fil-forge/ucantone/result"
	"github.com/fil-forge/ucantone/server"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/validator"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/lib/ucan_server"
	"github.com/storacha/sprue/pkg/service/handlers"
	"github.com/storacha/sprue/pkg/service/ui"
	"github.com/storacha/sprue/pkg/store/agent"
	delegation_store "github.com/storacha/sprue/pkg/store/delegation"
	"go.uber.org/zap"
)

// Service implements the sprue upload service logic.
type Service struct {
	identity        *identity.Identity
	agentStore      agent.Store
	delegationStore delegation_store.Store
	indexerClient   *indexerclient.Client
	logger          *zap.Logger
	ucanServer      *server.HTTPServer
	options         []server.HTTPOption
}

// New creates a new Service instance.
func New(id *identity.Identity, agentStore agent.Store, delegationStore delegation_store.Store, indexerClient *indexerclient.Client, logger *zap.Logger, options ...server.HTTPOption) (*Service, error) {
	svc := &Service{
		identity:        id,
		agentStore:      agentStore,
		delegationStore: delegationStore,
		indexerClient:   indexerClient,
		logger:          logger,
		options:         options,
	}

	// Create UCAN server with handlers
	svc.ucanServer = svc.createUCANServer()

	return svc, nil
}

// createUCANServer creates the UCAN RPC server with registered handlers.
func (s *Service) createUCANServer() *server.HTTPServer {
	options := append(
		slices.Clone(s.options),
		server.WithReceiptTimestamps(true),
		server.WithEventListener(ucan_server.AgentMessageLogger{Logger: s.logger, AgentStore: s.agentStore}),
		server.WithEventListener(ucan_server.ErrorHandler{Logger: s.logger}),
		server.WithValidationOptions(
			validator.WithPrincipalParser(ucan_server.PrincipalParser),
			validator.WithNonStandardSignatureVerifier(
				ucan_server.NewAttestationVerifier(s.identity.Signer.Verifier()),
			),
		),
	)
	return server.NewHTTP(s.identity.Signer, options...)
}

// HandleUCANRequest handles incoming UCAN RPC requests.
func (s *Service) HandleUCANRequest(c echo.Context) error {
	s.ucanServer.ServeHTTP(c.Response(), c.Request())
	return nil
}

func (s *Service) HandleValidateEmailRequest(c echo.Context) error {
	if c.QueryParam("ucan") == "" {
		r, err := ui.ErrorPage("missing ucan query parameter")
		if err != nil {
			return fmt.Errorf("failed to render error page: %w", err)
		}
		return c.Stream(http.StatusBadRequest, "text/html", r)
	}

	switch c.Request().Method {
	case http.MethodGet:
		r, err := ui.PendingValidateEmailPage(true)
		if err != nil {
			return fmt.Errorf("failed to render validation page: %w", err)
		}
		return c.Stream(http.StatusOK, "text/html", r)
	case http.MethodPost:
		res, err := s.authorize(c.Request().Context(), c.QueryParam("ucan"))
		if err != nil {
			s.logger.Error("authorization error", zap.Error(err))
			r, err := ui.ErrorPage(fmt.Sprintf("Oops, something went wrong: %s", err.Error()))
			if err != nil {
				return fmt.Errorf("failed to render error page: %w", err)
			}
			return c.Stream(http.StatusInternalServerError, "text/html", r)
		}
		r, err := ui.ValidateEmailPage(res.UCAN, res.Email, res.Audience)
		if err != nil {
			return fmt.Errorf("failed to render validation page: %w", err)
		}
		return c.Stream(http.StatusOK, "text/html", r)
	default:
		return c.String(http.StatusMethodNotAllowed, "method not allowed")
	}
}

type authorizationResult struct {
	Email    string
	Audience string
	UCAN     string
	Facts    []ucan.Fact
}

func (s *Service) authorize(ctx context.Context, ucan string) (authorizationResult, error) {
	dlgs, err := ucans.ParseDelegations(ucan)
	if err != nil {
		return authorizationResult{}, fmt.Errorf("parsing delegations: %w", err)
	}
	if len(dlgs) != 1 {
		return authorizationResult{}, fmt.Errorf("unexpected number of delegations found in UCAN")
	}
	confirmation := dlgs[0]

	confirm := server.Provide(
		access.Confirm,
		handlers.AccessConfirmHandler(s.identity, s.delegationStore, s.logger),
	)
	txn, err := confirm(ctx, confirmation, s.ucanServer.Context())
	if err != nil {
		return authorizationResult{}, fmt.Errorf("executing access/confirm handler: %w", err)
	}
	o, x := result.Unwrap(txn.Out())
	if x != nil {
		return authorizationResult{}, fmt.Errorf("access/confirm invocation failure: %w", x)
	}

	// Extract the email and audience from the confirmation invocation.
	// This should match since we just successfully invoked the handler.
	match, err := access.Confirm.Match(validator.NewSource(confirmation.Capabilities()[0], confirmation))
	if err != nil {
		return authorizationResult{}, fmt.Errorf("matching access/confirm capability: %w", err)
	}
	email, err := didmailto.Email(match.Value().Nb().Iss)
	if err != nil {
		return authorizationResult{}, fmt.Errorf("parsing account DID: %w", err)
	}

	var confirmDlgs []delegation.Delegation
	for _, bytes := range o.Delegations.Values {
		dlgs, err := ucans.ExtractDelegations(bytes)
		if err != nil {
			return authorizationResult{}, fmt.Errorf("extracting delegations from confirmation result: %w", err)
		}
		if len(dlgs) != 1 {
			return authorizationResult{}, fmt.Errorf("unexpected number of delegations found in confirmation result")
		}
		confirmDlgs = append(confirmDlgs, dlgs[0])
	}

	ucan, err = ucans.FormatDelegations(confirmDlgs...)
	if err != nil {
		return authorizationResult{}, fmt.Errorf("formatting delegations: %w", err)
	}

	return authorizationResult{
		Email:    email,
		Audience: match.Value().Nb().Aud.String(),
		UCAN:     ucan,
		Facts:    confirmation.Facts(),
	}, nil
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
