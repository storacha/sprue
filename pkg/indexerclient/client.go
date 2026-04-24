package indexerclient

import (
	"context"
	"fmt"
	"net/url"
	"slices"

	assertcap "github.com/alanshaw/libracha/capabilities/assert"
	contentcap "github.com/alanshaw/libracha/capabilities/content"
	ucanlib "github.com/alanshaw/libracha/ucan"
	"github.com/alanshaw/ucantone/client"
	"github.com/alanshaw/ucantone/did"
	edm "github.com/alanshaw/ucantone/errors/datamodel"
	"github.com/alanshaw/ucantone/execution"
	"github.com/alanshaw/ucantone/ipld"
	"github.com/alanshaw/ucantone/ipld/datamodel"
	"github.com/alanshaw/ucantone/result"
	"github.com/alanshaw/ucantone/ucan"
	"github.com/alanshaw/ucantone/ucan/invocation"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
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
func (c *Client) PublishIndexClaim(ctx context.Context, space did.DID, content, index cid.Cid, retrievalAuth []ucan.Delegation, matcher ucanlib.DelegationMatcher, options ...invocation.Option) error {
	// Create a content retrieval delegation from upload service to indexer
	indexerDelegation, err := contentcap.Retrieve.Delegate(c.signer, c.indexerDID, space)
	if err != nil {
		return fmt.Errorf("creating indexer delegation: %w", err)
	}
	retrievalAuth = slices.Clone(retrievalAuth)
	retrievalAuth = append(retrievalAuth, indexerDelegation)

	// TODO: use assertcap.Index
	inv, err := invocation.Invoke(
		c.signer,
		c.signer,
		"/assert/index",
		datamodel.Map{
			"content": content,
			"index":   index,
		},
		invocation.WithAudience(c.indexerDID),
		invocation.WithMetadata(
			datamodel.Map{"retrievalAuth": indexerDelegation.Link()},
		),
	)
	if err != nil {
		return fmt.Errorf("creating invocation: %w", err)
	}

	xreq := execution.NewRequest(
		ctx,
		inv,
		// send the retrieval auth chain as referenced by the index assertion meta
		execution.WithDelegations(retrievalAuth...),
	)
	resp, err := c.client.Execute(xreq)
	if err != nil {
		return fmt.Errorf("executing assert index invocation: %w", err)
	}

	rcpt := resp.Receipt()
	allocOK, err := result.MatchResultR2(
		rcpt.Out(),
		func(o ipld.Any) (*assertcap.IndexOK, error) {
			var allocOK assertcap.IndexOK
			err := datamodel.Rebind(datamodel.NewAny(o), &allocOK)
			if err != nil {
				return nil, fmt.Errorf("binding accept response: %w", err)
			}
			return &allocOK, nil
		},
		func(x ipld.Any) (*assertcap.IndexOK, error) {
			var model edm.ErrorModel
			err := datamodel.Rebind(datamodel.NewAny(x), &model)
			if err != nil {
				c.logger.Error("failed to accept blob", zap.Any("error", x))
				return nil, fmt.Errorf("accepting blob: %v", x)
			}
			c.logger.Error("failed to accept blob", zap.String("name", model.ErrorName), zap.Error(model))
			return nil, fmt.Errorf("accepting blob: %w", model)
		},
	)
	if err != nil {
		return err
	}

	return allocOK, inv, rcpt, nil
}

// CacheLocationClaim sends a claim/cache invocation to cache a location claim with the indexer.
// This tells the indexer where content is stored (provider address).
func (c *Client) CacheLocationClaim(ctx context.Context, claim delegation.Delegation, providerAddrs []multiaddr.Multiaddr) error {
	c.logger.Debug("CacheLocationClaim",
		zap.String("claim", claim.Link().String()),
		zap.String("issuer", c.signer.DID().String()),
		zap.String("audience", c.indexerDID.String()),
		zap.Int("providerAddrs", len(providerAddrs)))

	inv, err := claimcap.Cache.Invoke(
		c.signer,
		c.indexerDID,
		c.signer.DID().String(),
		claimcap.CacheCaveats{
			Claim: claim.Link(),
			Provider: claimcap.Provider{
				Addresses: providerAddrs,
			},
		},
		delegation.WithProof(delegation.FromDelegation(claim)),
	)
	if err != nil {
		return fmt.Errorf("creating claim/cache invocation: %w", err)
	}
	c.logger.Debug("created invocation", zap.String("link", inv.Link().String()))

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.connection)
	if err != nil {
		c.logger.Error("execute error", zap.Error(err))
		return fmt.Errorf("executing claim/cache: %w", err)
	}
	c.logger.Debug("execute succeeded")

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		c.logger.Debug("no receipt in response")
		return fmt.Errorf("receipt not found for invocation")
	}
	c.logger.Debug("got receipt", zap.String("link", rcptLink.String()))

	// Read receipt and check for errors
	anyReader := receipt.NewAnyReceiptReader(captypes.Converters...)
	anyRcpt, err := anyReader.Read(rcptLink, resp.Blocks())
	if err != nil {
		c.logger.Error("reading receipt error", zap.Error(err))
		return fmt.Errorf("reading receipt: %w", err)
	}

	okNode, errNode := result.Unwrap(anyRcpt.Out())
	c.logger.Debug("receipt result", zap.Bool("ok", okNode != nil), zap.Bool("err", errNode != nil))
	if errNode != nil {
		// Extract error details for better debugging
		var errDetails string
		if msgNode, lookupErr := errNode.LookupByString("message"); lookupErr == nil {
			if msg, asErr := msgNode.AsString(); asErr == nil {
				errDetails = msg
			}
		}
		if errDetails == "" {
			if nameNode, lookupErr := errNode.LookupByString("name"); lookupErr == nil {
				if name, asErr := nameNode.AsString(); asErr == nil {
					errDetails = name
				}
			}
		}
		if errDetails == "" {
			errDetails = "unknown error"
		}
		return fmt.Errorf("claim/cache failed: %s", errDetails)
	}

	return nil
}
