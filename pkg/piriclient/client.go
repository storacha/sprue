package piriclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	blobreplicacap "github.com/storacha/go-libstoracha/capabilities/blob/replica"
	"github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/did"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Replication invocation timeout.
//
// Note: we set a reasonably large expiration as replication nodes use the
// invocation as proof for obtaining a retrieval delegation, and we want to
// allow for retries and/or job queue delays.
const replicaAllocationTTL = time.Hour * 24

// DelegationFetcher provides an interface for fetching delegation proofs on-demand.
type DelegationFetcher interface {
	// GetDelegation fetches the delegation proof for the given audience.
	// Returns nil if no delegation is available (not an error condition).
	GetDelegation(ctx context.Context, audience ucan.Principal) (delegation.Delegation, error)
}

// Client is a UCAN client for communicating with Piri nodes.
type Client struct {
	piriDID    did.DID
	signer     ucan.Signer
	connection uclient.Connection
	logger     *otelzap.Logger
}

// New creates a new Piri client.
// The delegationFetcher is used to fetch delegation proofs on-demand for each request.
func New(endpoint *url.URL, piriDID did.DID, signer ucan.Signer, logger *zap.Logger) (*Client, error) {
	// otelhttp wraps the default transport so every outbound UCAN request
	// becomes a child span of whatever span is active on the caller's ctx
	// (typically piriclient.Allocate / .Accept / .ReplicaAllocate). This is
	// also what propagates the W3C traceparent header downstream so piri
	// nodes can continue the trace.
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	channel := ucanhttp.NewChannel(endpoint, ucanhttp.WithClient(httpClient))
	conn, err := uclient.NewConnection(piriDID, channel)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	return NewWithConnection(piriDID, signer, conn, logger), nil
}

func NewWithConnection(piriDID did.DID, signer ucan.Signer, conn uclient.Connection, logger *zap.Logger) *Client {
	return &Client{
		piriDID:    piriDID,
		signer:     signer,
		connection: conn,
		logger:     otelzap.New(logger),
	}
}

// AllocateRequest contains the parameters for a blob/allocate invocation.
type AllocateRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Cause  ipld.Link
}

// AllocateResponse contains the response from a blob/allocate invocation.
type AllocateResponse struct {
	Size    uint64
	Address *blobcap.Address
}

// fetchDelegationOpts fetches the delegation proof and returns delegation options.
func (c *Client) fetchDelegationOpts(ctx context.Context, fetcher DelegationFetcher) ([]delegation.Option, error) {
	var opts []delegation.Option

	if fetcher != nil {
		c.logger.Debug("fetching delegation", zap.String("piriDID", c.piriDID.String()))
		proof, err := fetcher.GetDelegation(ctx, c.signer)
		if err != nil {
			c.logger.Error("delegation fetch error", zap.Error(err))
			return nil, fmt.Errorf("fetching delegation: %w", err)
		}
		if proof != nil {
			c.logger.Debug("found delegation",
				zap.String("issuer", proof.Issuer().DID().String()),
				zap.String("audience", proof.Audience().DID().String()))
			opts = append(opts, delegation.WithProof(delegation.FromDelegation(proof)))
		} else {
			c.logger.Debug("no delegation found", zap.String("piriDID", c.piriDID.String()))
		}
	} else {
		c.logger.Debug("no delegation fetcher configured")
	}

	return opts, nil
}

// Allocate sends a blob/allocate invocation to the piri node.
// Returns the response data, the invocation that was sent, and the receipt from piri.
func (c *Client) Allocate(ctx context.Context, req *AllocateRequest, fetcher DelegationFetcher) (*AllocateResponse, invocation.Invocation, receipt.AnyReceipt, error) {
	ctx, span := tracer.Start(ctx, "piriclient.Allocate",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("piri.did", c.piriDID.String()),
			attribute.String("space.did", req.Space.String()),
			attribute.Int64("blob.size", int64(req.Size)),
		),
	)
	defer span.End()

	start := time.Now()
	outcome := "ok"
	defer func() {
		labels := metric.WithAttributes(attribute.String("outcome", outcome))
		allocateDuration.Record(ctx, time.Since(start).Seconds(), labels)
		allocateRequests.Add(ctx, 1, labels)
	}()

	fail := func(err error, reason string) error {
		outcome = reason
		span.RecordError(err)
		span.SetStatus(codes.Error, reason)
		c.logger.Ctx(ctx).Error("allocate failed",
			zap.String("reason", reason),
			zap.Error(err),
		)
		return err
	}

	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, nil, nil, fail(err, "fetch_delegation")
	}

	inv, err := blobcap.Allocate.Invoke(
		c.signer,
		c.piriDID,
		c.piriDID.String(), // resource is the piri DID
		blobcap.AllocateCaveats{
			Space: req.Space,
			Blob: types.Blob{
				Digest: req.Digest,
				Size:   req.Size,
			},
			Cause: req.Cause,
		},
		opts...,
	)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("creating allocate invocation: %w", err), "invocation_create")
	}

	proofLinks := inv.Proofs()
	blockCount := 0
	for _, blkErr := range inv.Export() {
		if blkErr != nil {
			continue
		}
		blockCount++
	}
	c.logger.Debug("ALLOCATE invocation created",
		zap.String("issuer", inv.Issuer().DID().String()),
		zap.String("audience", inv.Audience().DID().String()),
		zap.Int("proofLinks", len(proofLinks)),
		zap.Int("blocks", blockCount))

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.connection)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("executing allocate invocation: %w", err), "execute")
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, nil, fail(fmt.Errorf("receipt not found for invocation"), "receipt_missing")
	}

	anyReader := receipt.NewAnyReceiptReader(types.Converters...)
	anyRcpt, err := anyReader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("reading receipt: %w", err), "receipt_read")
	}

	okNode, errNode := result.Unwrap(anyRcpt.Out())
	if errNode != nil {
		return nil, nil, nil, fail(fmt.Errorf("allocate failed: %s", extractErrorDetails(errNode)), "receipt_error")
	}
	if okNode == nil {
		return nil, nil, nil, fail(fmt.Errorf("allocate returned nil result"), "receipt_empty")
	}

	typedRcpt, err := receipt.Rebind[blobcap.AllocateOk, fdm.FailureModel](
		anyRcpt,
		blobcap.AllocateOkType(),
		fdm.FailureType(),
		types.Converters...,
	)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("rebinding receipt: %w", err), "receipt_rebind")
	}

	allocateOk, failErr := result.Unwrap(typedRcpt.Out())
	if (failErr != fdm.FailureModel{}) {
		return nil, nil, nil, fail(fmt.Errorf("allocate failed: %s", failErr.Message), "result_failure")
	}

	span.SetAttributes(attribute.Int64("blob.allocated_size", int64(allocateOk.Size)))
	return &AllocateResponse{
		Size:    allocateOk.Size,
		Address: allocateOk.Address,
	}, inv, anyRcpt, nil
}

// AllocateInvocation returns the invocation for the allocate request (for use in effects).
func (c *Client) AllocateInvocation(ctx context.Context, req *AllocateRequest, fetcher DelegationFetcher) (invocation.IssuedInvocation, error) {
	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, err
	}

	return blobcap.Allocate.Invoke(
		c.signer,
		c.piriDID,
		c.piriDID.String(),
		blobcap.AllocateCaveats{
			Space: req.Space,
			Blob: types.Blob{
				Digest: req.Digest,
				Size:   req.Size,
			},
			Cause: req.Cause,
		},
		opts...,
	)
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
	Put    ipld.Link // Link to the http/put invocation that uploaded the blob
}

// AcceptResponse contains the response from a blob/accept invocation.
type AcceptResponse struct {
	Site ipld.Link // Link to the location claim delegation
}

// Accept sends a blob/accept invocation to the piri node.
func (c *Client) Accept(ctx context.Context, req *AcceptRequest, fetcher DelegationFetcher) (*AcceptResponse, invocation.Invocation, receipt.AnyReceipt, error) {
	ctx, span := tracer.Start(ctx, "piriclient.Accept",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("piri.did", c.piriDID.String()),
			attribute.String("space.did", req.Space.String()),
			attribute.Int64("blob.size", int64(req.Size)),
		),
	)
	defer span.End()

	start := time.Now()
	outcome := "ok"
	defer func() {
		labels := metric.WithAttributes(attribute.String("outcome", outcome))
		acceptDuration.Record(ctx, time.Since(start).Seconds(), labels)
		acceptRequests.Add(ctx, 1, labels)
	}()

	fail := func(err error, reason string) error {
		outcome = reason
		span.RecordError(err)
		span.SetStatus(codes.Error, reason)
		c.logger.Ctx(ctx).Error("accept failed",
			zap.String("reason", reason),
			zap.Error(err),
		)
		return err
	}

	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, nil, nil, fail(err, "fetch_delegation")
	}

	// Use WithNoExpiration so the invocation CID is deterministic and matches
	// the accept invocation created in space/blob/add for effects
	opts = append(opts, delegation.WithNoExpiration())
	inv, err := blobcap.Accept.Invoke(
		c.signer,
		c.piriDID,
		c.piriDID.String(),
		blobcap.AcceptCaveats{
			Space: req.Space,
			Blob: types.Blob{
				Digest: req.Digest,
				Size:   req.Size,
			},
			Put: blobcap.Promise{
				UcanAwait: blobcap.Await{
					Selector: ".out.ok",
					Link:     req.Put,
				},
			},
		},
		opts...,
	)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("creating accept invocation: %w", err), "invocation_create")
	}

	acceptProofLinks := inv.Proofs()
	acceptBlockCount := 0
	for _, blkErr := range inv.Export() {
		if blkErr != nil {
			continue
		}
		acceptBlockCount++
	}
	c.logger.Debug("ACCEPT invocation created",
		zap.String("issuer", inv.Issuer().DID().String()),
		zap.String("audience", inv.Audience().DID().String()),
		zap.Int("proofLinks", len(acceptProofLinks)),
		zap.Int("blocks", acceptBlockCount))

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.connection)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("executing accept invocation: %w", err), "execute")
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, nil, fail(fmt.Errorf("receipt not found for invocation"), "receipt_missing")
	}

	anyReader := receipt.NewAnyReceiptReader(types.Converters...)
	anyRcpt, err := anyReader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("reading receipt: %w", err), "receipt_read")
	}

	okNode, errNode := result.Unwrap(anyRcpt.Out())
	if errNode != nil {
		return nil, nil, nil, fail(fmt.Errorf("accept failed: %s", extractErrorDetails(errNode)), "receipt_error")
	}
	if okNode == nil {
		return nil, nil, nil, fail(fmt.Errorf("accept returned nil result"), "receipt_empty")
	}

	// Extract the site link from the ok node
	var site ipld.Link
	if siteNode, lookupErr := okNode.LookupByString("site"); lookupErr == nil {
		if siteLink, asErr := siteNode.AsLink(); asErr == nil {
			site = siteLink
		}
	}

	return &AcceptResponse{
		Site: site,
	}, inv, anyRcpt, nil
}

// AcceptInvocation returns the invocation for the accept request (for use in effects).
func (c *Client) AcceptInvocation(ctx context.Context, req *AcceptRequest, fetcher DelegationFetcher) (invocation.IssuedInvocation, error) {
	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, err
	}

	opts = append(opts, delegation.WithNoExpiration())
	return blobcap.Accept.Invoke(
		c.signer,
		c.piriDID,
		c.piriDID.String(),
		blobcap.AcceptCaveats{
			Space: req.Space,
			Blob: types.Blob{
				Digest: req.Digest,
				Size:   req.Size,
			},
			Put: blobcap.Promise{
				UcanAwait: blobcap.Await{
					Selector: ".out.ok",
					Link:     req.Put,
				},
			},
		},
		opts...,
	)
}

// ReplicaAllocateRequest contains the parameters for a blob/replica/allocate invocation.
type ReplicaAllocateRequest struct {
	Space  did.DID
	Digest []byte
	Size   uint64
	Site   delegation.Delegation // Location commitment
	Cause  ipld.Link
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
	ctx, span := tracer.Start(ctx, "piriclient.ReplicaAllocate",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("piri.did", c.piriDID.String()),
			attribute.String("space.did", req.Space.String()),
			attribute.Int64("blob.size", int64(req.Size)),
			attribute.String("site.cid", req.Site.Link().String()),
		),
	)
	defer span.End()

	start := time.Now()
	outcome := "ok"
	defer func() {
		labels := metric.WithAttributes(attribute.String("outcome", outcome))
		replicaAllocateDuration.Record(ctx, time.Since(start).Seconds(), labels)
		replicaAllocateRequests.Add(ctx, 1, labels)
	}()

	fail := func(err error, reason string) error {
		outcome = reason
		span.RecordError(err)
		span.SetStatus(codes.Error, reason)
		c.logger.Ctx(ctx).Error("replica allocate failed",
			zap.String("reason", reason),
			zap.Error(err),
		)
		return err
	}

	opts, err := c.fetchDelegationOpts(ctx, fetcher)
	if err != nil {
		return nil, nil, nil, fail(err, "fetch_delegation")
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
		return nil, nil, nil, fail(fmt.Errorf("creating replica allocate invocation: %w", err), "invocation_create")
	}

	// attach the location commitment to the allocation invocation
	for b, err := range req.Site.Blocks() {
		if err != nil {
			return nil, nil, nil, fail(fmt.Errorf("iterating location commitment blocks: %w", err), "site_iterate")
		}
		if err := inv.Attach(b); err != nil {
			return nil, nil, nil, fail(fmt.Errorf("attaching location commitment block: %w", err), "site_attach")
		}
	}

	c.logger.Debug("REPLICA ALLOCATE invocation created",
		zap.Stringer("issuer", inv.Issuer().DID()),
		zap.Stringer("audience", inv.Audience().DID()),
		zap.Int("proofs", len(inv.Proofs())))

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.connection)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("executing replica allocate invocation: %w", err), "execute")
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, nil, fail(fmt.Errorf("receipt not found for invocation"), "receipt_missing")
	}

	reader := receipt.NewAnyReceiptReader(types.Converters...)
	rcpt, err := reader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("reading receipt: %w", err), "receipt_read")
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		return nil, nil, nil, fail(fmt.Errorf("allocate failed: %s", fdm.Bind(x).Message), "receipt_error")
	}

	allocateOk, err := ipld.Rebind[blobreplicacap.AllocateOk](o, blobreplicacap.AllocateOkType(), types.Converters...)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("rebinding receipt: %w", err), "receipt_rebind")
	}

	if allocateOk.Site.UcanAwait.Selector != blobreplicacap.AllocateSiteSelector {
		return nil, nil, nil, fail(fmt.Errorf("unexpected site selector: %s", allocateOk.Site.UcanAwait.Selector), "site_selector")
	}

	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(rcpt.Blocks()))
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("creating block reader: %w", err), "block_reader")
	}
	transfer, err := invocation.NewInvocationView(allocateOk.Site.UcanAwait.Link, br)
	if err != nil {
		return nil, nil, nil, fail(fmt.Errorf("creating transfer invocation view: %w", err), "transfer_view")
	}

	span.SetAttributes(attribute.Int64("blob.allocated_size", int64(allocateOk.Size)))
	return &ReplicaAllocateResponse{
		Size:     allocateOk.Size,
		Site:     allocateOk.Site,
		Transfer: transfer,
	}, inv, rcpt, nil
}
