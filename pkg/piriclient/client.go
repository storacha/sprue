package piriclient

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/alanshaw/ucantone/client"
	"github.com/alanshaw/ucantone/did"
	edm "github.com/alanshaw/ucantone/errors/datamodel"
	"github.com/alanshaw/ucantone/execution"
	"github.com/alanshaw/ucantone/ipld"
	"github.com/alanshaw/ucantone/ipld/datamodel"
	"github.com/alanshaw/ucantone/result"
	"github.com/alanshaw/ucantone/ucan"
	"github.com/alanshaw/ucantone/ucan/invocation"
	"github.com/alanshaw/ucantone/ucan/promise"
	"github.com/ipfs/go-cid"

	blobcap "github.com/alanshaw/libracha/capabilities/blob"
	ucanlib "github.com/alanshaw/libracha/ucan"
	blobreplicacap "github.com/storacha/go-libstoracha/capabilities/blob/replica"
	"github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
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

// AllocateRequest contains the parameters for a blob/allocate invocation.
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

	resp, err := c.client.Execute(execution.NewRequest(ctx, inv, execution.WithProofs(prfs...)))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("executing allocate invocation: %w", err)
	}

	rcpt := resp.Receipt()
	allocOK, err := result.MatchResultR2(
		rcpt.Out(),
		func(o ipld.Any) (*blobcap.AllocateOK, error) {
			var allocOK blobcap.AllocateOK
			err := datamodel.Rebind(datamodel.NewAny(o), &allocOK)
			if err != nil {
				return nil, fmt.Errorf("binding allocate response: %w", err)
			}
			return &allocOK, nil
		},
		func(x ipld.Any) (*blobcap.AllocateOK, error) {
			var model edm.ErrorModel
			err := datamodel.Rebind(datamodel.NewAny(x), &model)
			if err != nil {
				c.logger.Error("failed to allocate blob", zap.Any("error", x))
				return nil, fmt.Errorf("allocating blob: %v", x)
			}
			c.logger.Error("failed to allocate blob", zap.String("name", model.ErrorName), zap.Error(model))
			return nil, fmt.Errorf("allocating blob: %w", model)
		},
	)
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

// AcceptRequest contains the parameters for a blob/accept invocation.
type AcceptRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Put    cid.Cid // Link to the /http/put task that uploaded the blob
}

// Accept sends a blob/accept invocation to the piri node.
func (c *Client) Accept(ctx context.Context, req *AcceptRequest, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (*blobcap.AcceptOK, ucan.Invocation, ucan.Receipt, error) {
	inv, prfs, err := c.AcceptInvocation(ctx, req, matcher, options...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating accept invocation: %w", err)
	}

	c.logger.Debug("ACCEPT invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(prfs)))

	resp, err := c.client.Execute(execution.NewRequest(ctx, inv, execution.WithProofs(prfs...)))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("executing allocate invocation: %w", err)
	}

	rcpt := resp.Receipt()
	acceptOK, err := result.MatchResultR2(
		rcpt.Out(),
		func(o ipld.Any) (*blobcap.AcceptOK, error) {
			var acceptOK blobcap.AcceptOK
			err := datamodel.Rebind(datamodel.NewAny(o), &acceptOK)
			if err != nil {
				return nil, fmt.Errorf("binding accept response: %w", err)
			}
			return &acceptOK, nil
		},
		func(x ipld.Any) (*blobcap.AcceptOK, error) {
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
	Site   ucan.Delegation // Location commitment
	Cause  cid.Cid
}

// ReplicaAllocateResponse contains the response from a blob/replica/allocate invocation.
type ReplicaAllocateResponse struct {
	// Size is the number of bytes allocated for the Blob.
	Size uint64
	// Site resolves to an additional location for the blob.
	// The selector MUST be ".out.ok.site" i.e. [AllocateSiteSelector] and it
	// links to a receipt of a "blob/replica/transfer" task.
	Site types.Promise
	// Transfer is the invocation referenced in the promise, which is included in
	// the allocation response.
	Transfer invocation.Invocation
}

// ReplicaAllocate sends a blob/replica/allocate invocation to the piri node.
// Returns the response data, the invocation that was sent, and the receipt from
// piri. It returns an error if the receipt contains a failure result.
func (c *Client) ReplicaAllocate(ctx context.Context, req *ReplicaAllocateRequest, fetcher DelegationFetcher) (*ReplicaAllocateResponse, invocation.Invocation, receipt.AnyReceipt, error) {
	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, nil, nil, err
	}

	// We set a reasonably large expiration as replication nodes use the
	// invocation as proof for obtaining a retrieval delegation, and we want to
	// allow for retries and/or job queue delays.
	exp := time.Now().Add(replicaAllocationTTL).Unix()
	opts = append(opts, delegation.WithExpiration(int(exp)))

	inv, err := blobreplicacap.Allocate.Invoke(
		c.signer,
		c.piriDID,
		c.piriDID.String(), // resource is the piri DID
		blobreplicacap.AllocateCaveats{
			Space: req.Space,
			Blob: types.Blob{
				Digest: req.Digest,
				Size:   req.Size,
			},
			Site:  req.Site.Link(),
			Cause: req.Cause,
		},
		opts...,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating replica allocate invocation: %w", err)
	}

	// attach the location commitment to the allocation invocation
	for b, err := range req.Site.Blocks() {
		if err != nil {
			return nil, nil, nil, fmt.Errorf("iterating location commitment blocks: %w", err)
		}
		if err := inv.Attach(b); err != nil {
			return nil, nil, nil, fmt.Errorf("attaching location commitment block: %w", err)
		}
	}

	c.logger.Debug("REPLICA ALLOCATE invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(inv.Proofs())))

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.connection)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("executing replica allocate invocation: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, nil, fmt.Errorf("receipt not found for invocation")
	}

	reader := receipt.NewAnyReceiptReader(types.Converters...)
	rcpt, err := reader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reading receipt: %w", err)
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		return nil, nil, nil, fmt.Errorf("allocate failed: %s", fdm.Bind(x).Message)
	}

	allocateOk, err := ipld.Rebind[blobreplicacap.AllocateOk](o, blobreplicacap.AllocateOkType(), types.Converters...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rebinding receipt: %w", err)
	}

	if allocateOk.Site.UcanAwait.Selector != blobreplicacap.AllocateSiteSelector {
		return nil, nil, nil, fmt.Errorf("unexpected site selector: %s", allocateOk.Site.UcanAwait.Selector)
	}

	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(rcpt.Blocks()))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating block reader: %w", err)
	}
	transfer, err := invocation.NewInvocationView(allocateOk.Site.UcanAwait.Link, br)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating transfer invocation view: %w", err)
	}

	return &ReplicaAllocateResponse{
		Size:     allocateOk.Size,
		Site:     allocateOk.Site,
		Transfer: transfer,
	}, inv, rcpt, nil
}
