package indexerclient

import (
	"context"
	"fmt"
	"net/url"
	"slices"

	assertcap "github.com/fil-forge/libforge/capabilities/assert"
	contentcap "github.com/fil-forge/libforge/capabilities/content"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/client"
	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
	"github.com/storacha/sprue/pkg/lib/ucan_client"
	"go.uber.org/zap"
)

// Client is a UCAN client for communicating with the indexer service.
type Client struct {
	endpoint   *url.URL
	indexerDID did.DID
	signer     ucan.Signer
	client     *client.HTTPClient
	logger     *zap.Logger
}

// New creates a new indexer client.
func New(endpoint *url.URL, indexerDID did.DID, signer ucan.Signer, logger *zap.Logger) (*Client, error) {
	client, err := client.NewHTTP(endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	return &Client{
		endpoint:   endpoint,
		indexerDID: indexerDID,
		signer:     signer,
		client:     client,
		logger:     logger,
	}, nil
}

// PublishIndexClaim sends an /assert/index claim to the indexer.
//
// The retrievalAuth parameter is a delegation chain authorizing the upload
// service to retrieve the index blob via `/content/retrieve` command.
func (c *Client) PublishIndexClaim(ctx context.Context, space did.DID, content, index cid.Cid, retrievalAuth []ucan.Delegation, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Receipt, error) {
	// Create a content retrieval delegation from upload service to indexer
	indexerDelegation, err := contentcap.Retrieve.Delegate(c.signer, c.indexerDID, space)
	if err != nil {
		return nil, fmt.Errorf("creating indexer delegation: %w", err)
	}
	retrievalAuth = slices.Clone(retrievalAuth)
	retrievalAuth = append(retrievalAuth, indexerDelegation)

	inv, err := assertcap.Index.Invoke(
		c.signer,
		c.signer,
		&assertcap.IndexArguments{
			Content: content,
			Index:   index,
		},
		invocation.WithAudience(c.indexerDID),
		invocation.WithMetadata(
			datamodel.Map{"retrievalAuth": indexerDelegation.Link()},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	_, rcpt, err := ucan_client.Execute[*assertcap.IndexOK](ctx, c.client, c.logger, inv, execution.WithDelegations(retrievalAuth...))
	if err != nil {
		return nil, fmt.Errorf("executing assert index invocation: %w", err)
	}
	return rcpt, nil
}
