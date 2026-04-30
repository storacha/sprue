package piriclient

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"time"

	blobcap "github.com/alanshaw/libracha/capabilities/blob"
	blobreplicacap "github.com/alanshaw/libracha/capabilities/blob/replica"
	ucanlib "github.com/alanshaw/libracha/ucan"
	"github.com/alanshaw/ucantone/client"
	"github.com/alanshaw/ucantone/did"
	"github.com/alanshaw/ucantone/execution"
	"github.com/alanshaw/ucantone/ucan"
	"github.com/alanshaw/ucantone/ucan/invocation"
	"github.com/alanshaw/ucantone/ucan/promise"
	"github.com/ipfs/go-cid"
	"github.com/storacha/sprue/pkg/lib/ucanclient"
	"go.uber.org/zap"
)

// Replication invocation timeout.
//
// Note: we set a reasonably large expiration as replication nodes use the
// invocation as proof for obtaining a retrieval delegation, and we want to
// allow for retries and/or job queue delays.
const replicaAllocationTTL = time.Hour * 24

// Client is a UCAN client for communicating with Piri nodes.
type Client struct {
	piriDID did.DID
	signer  ucan.Signer
	client  *client.HTTPClient
	logger  *zap.Logger
}

// New creates a new Piri client.
// The delegationFetcher is used to fetch delegation proofs on-demand for each request.
func New(endpoint *url.URL, piriDID did.DID, signer ucan.Signer, logger *zap.Logger) (*Client, error) {
	client, err := client.NewHTTP(endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	return NewWithClient(piriDID, signer, client, logger), nil
}

func NewWithClient(piriDID did.DID, signer ucan.Signer, client *client.HTTPClient, logger *zap.Logger) *Client {
	return &Client{
		piriDID: piriDID,
		signer:  signer,
		client:  client,
		logger:  logger,
	}
}

// AllocateRequest contains the parameters for a /blob/allocate invocation.
type AllocateRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Cause  cid.Cid
}

// Allocate sends a /blob/allocate invocation to the piri node.
// Returns the response data, the invocation that was sent, and the receipt from piri.
func (c *Client) Allocate(ctx context.Context, req *AllocateRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (*blobcap.AllocateOK, ucan.Invocation, ucan.Receipt, error) {
	inv, prfs, err := c.AllocateInvocation(ctx, req, matcher, options...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating allocate invocation: %w", err)
	}

	c.logger.Debug("ALLOCATE invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(prfs)))

	allocOK, rcpt, err := ucanclient.Execute[*blobcap.AllocateOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, nil, nil, err
	}
	return allocOK, inv, rcpt, nil
}

// AllocateInvocation returns the invocation for the allocate request (for use in effects).
func (c *Client) AllocateInvocation(ctx context.Context, req *AllocateRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Invocation, []ucan.Delegation, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.piriDID, blobcap.AllocateCommand, req.Space)
	if err != nil {
		return nil, nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.piriDID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := blobcap.Allocate.Invoke(
		c.signer,
		req.Space,
		&blobcap.AllocateArguments{
			Blob:  blobcap.Blob{Digest: req.Digest, Size: req.Size},
			Cause: req.Cause,
		},
		options...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating allocate invocation: %w", err)
	}

	return inv, prfs, nil
}

// PiriDID returns the DID of the piri node.
func (c *Client) PiriDID() did.DID {
	return c.piriDID
}

// AcceptRequest contains the parameters for a /blob/accept invocation.
type AcceptRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Put    cid.Cid // Link to the /http/put task that uploaded the blob
}

// Accept sends a /blob/accept invocation to the piri node.
func (c *Client) Accept(ctx context.Context, req *AcceptRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (*blobcap.AcceptOK, ucan.Invocation, ucan.Receipt, error) {
	inv, prfs, err := c.AcceptInvocation(ctx, req, matcher, options...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating accept invocation: %w", err)
	}

	c.logger.Debug("ACCEPT invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(prfs)))

	acceptOK, rcpt, err := ucanclient.Execute[*blobcap.AcceptOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, nil, nil, err
	}
	return acceptOK, inv, rcpt, nil
}

// AcceptInvocation returns the invocation for the accept request (for use in effects).
func (c *Client) AcceptInvocation(ctx context.Context, req *AcceptRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Invocation, []ucan.Delegation, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.piriDID, blobcap.AllocateCommand, req.Space)
	if err != nil {
		return nil, nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.piriDID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := blobcap.Accept.Invoke(
		c.signer,
		req.Space,
		&blobcap.AcceptArguments{
			Blob: blobcap.Blob{Digest: req.Digest, Size: req.Size},
			Put:  promise.AwaitOK{Task: req.Put},
		},
		options...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating accept invocation: %w", err)
	}

	return inv, prfs, nil
}

// ReplicaAllocateRequest contains the parameters for a /blob/replica/allocate invocation.
type ReplicaAllocateRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Site   ucan.Invocation // Location commitment
	Cause  cid.Cid
}

// ReplicaAllocate sends a /blob/replica/allocate invocation to the piri node.
// Returns the response data, the invocation that was sent, and the receipt from
// piri. It returns an error if the receipt contains a failure result.
func (c *Client) ReplicaAllocate(ctx context.Context, req *ReplicaAllocateRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (*blobreplicacap.AllocateOK, ucan.Invocation, ucan.Receipt, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.piriDID, blobcap.AllocateCommand, req.Space)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.piriDID),
		invocation.WithProofs(prfLinks...),
		// We set a reasonably large expiration as replication nodes use the
		// invocation as proof for obtaining a retrieval delegation, and we want to
		// allow for retries and/or job queue delays.
		invocation.WithExpiration(uint64(time.Now().Add(replicaAllocationTTL).Unix())),
	)

	inv, err := blobreplicacap.Allocate.Invoke(
		c.signer,
		req.Space,
		&blobreplicacap.AllocateArguments{
			Blob:  blobreplicacap.Blob{Digest: req.Digest, Size: req.Size},
			Site:  req.Site.Link(),
			Cause: req.Cause,
		},
		options...,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating replica allocate invocation: %w", err)
	}

	c.logger.Debug("REPLICA ALLOCATE invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(inv.Proofs())))

	allocOK, rcpt, err := ucanclient.Execute[*blobreplicacap.AllocateOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...), execution.WithInvocations(req.Site))
	if err != nil {
		return nil, nil, nil, err
	}
	return allocOK, inv, rcpt, nil
}
