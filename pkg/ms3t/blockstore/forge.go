package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/failure"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/locator"
	indexclient "github.com/storacha/indexing-service/pkg/client"
	"go.uber.org/zap"
)

// ErrNotFound is returned by Get when the indexing-service has no
// location commitment for the requested CID.
var ErrNotFound = errors.New("blockstore: not found")

// Forge is a read-only IpldBlockstore that resolves CIDs through the
// Storacha indexing-service and fetches the underlying bytes via
// authorized UCAN-wrapped GETs against piri storage nodes.
//
// Used in ms3t's "no_cache" mode: every Get goes to the network. There
// is no in-process block cache; the only caching is the small
// metadata cache inside the IndexLocator (digest → location
// commitment), which exists per-Forge instance and resets on process
// restart. Block bytes always traverse the network.
//
// Put is a no-op so this type can be passed as the underlying for
// CARBuffer (whose Commit calls Put for each freshly-Submitted
// block).
type Forge struct {
	locator     locator.Locator
	signer      principal.Signer
	spaceSigner principal.Signer
	spaces      []did.DID
	logger      *zap.Logger
}

var _ BlockReader = (*Forge)(nil)

// ForgeConfig wires sprue's existing services into a read-only Forge
// blockstore.
type ForgeConfig struct {
	// IndexerEndpoint is the indexing-service URL (cfg.Indexer.Endpoint).
	IndexerEndpoint string
	// IndexerDID is the indexing-service principal (cfg.Indexer.DID).
	IndexerDID string
	// Spaces scopes the locator queries; for ms3t this is the single
	// space ms3t owns.
	Spaces []did.DID
	// Signer is sprue's upload-service identity. Used as the issuer of
	// `space/content/retrieve` invocations against piri.
	Signer principal.Signer
	// SpaceSigner is the keypair of the space ms3t owns. Used to
	// self-issue space/content/retrieve delegations. The chain is
	// space → sprue → piri (with the sprue→piri hop being the actual
	// retrieve invocation that piri authorizes).
	SpaceSigner principal.Signer
	// HTTPClient is used for the underlying indexer queries. piri
	// retrievals use go-ucanto's retrieval client which manages its
	// own HTTP. Optional; defaults to http.DefaultClient.
	HTTPClient *http.Client
	// Logger is optional.
	Logger *zap.Logger
}

// NewForge constructs a Forge blockstore. Builds an indexing-service
// client and wraps it with guppy's IndexLocator.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.IndexerEndpoint == "" {
		return nil, errors.New("forge blockstore: indexer endpoint is required")
	}
	if cfg.IndexerDID == "" {
		return nil, errors.New("forge blockstore: indexer DID is required")
	}
	if len(cfg.Spaces) == 0 {
		return nil, errors.New("forge blockstore: at least one space is required")
	}
	if cfg.Signer == nil {
		return nil, errors.New("forge blockstore: signer is required")
	}
	if cfg.SpaceSigner == nil {
		return nil, errors.New("forge blockstore: space signer is required")
	}

	endpointURL, err := url.Parse(cfg.IndexerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: parse indexer endpoint: %w", err)
	}
	indexerDID, err := did.Parse(cfg.IndexerDID)
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: parse indexer DID: %w", err)
	}

	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	idxClient, err := indexclient.New(indexerDID, *endpointURL, indexclient.WithHTTPClient(httpc))
	if err != nil {
		return nil, fmt.Errorf("forge blockstore: build indexing-service client: %w", err)
	}

	authFn := newAuthorizeRetrieval(cfg.SpaceSigner, indexerDID)
	loc := locator.NewIndexLocator(idxClient, authFn)

	return &Forge{
		locator:     loc,
		signer:      cfg.Signer,
		spaceSigner: cfg.SpaceSigner,
		spaces:      cfg.Spaces,
		logger:      logger,
	}, nil
}

// GetBlock resolves the CID through the indexer and retrieves the
// underlying byte slice from piri via a UCAN-authorized
// `space/content/retrieve` invocation. The request is scoped to
// the inner block's offset/length within the containing CAR shard.
func (f *Forge) GetBlock(ctx context.Context, c cid.Cid) (block.Block, error) {
	locations, err := f.locator.Locate(ctx, f.spaces, c.Hash())
	if err != nil {
		var nf locator.NotFoundError
		if errors.As(err, &nf) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("forge: locate %s: %w", c, err)
	}
	if len(locations) == 0 {
		return nil, ErrNotFound
	}

	loc := locations[0]
	caveats, rerr := assert.LocationCaveatsReader.Read(loc.Commitment.Nb())
	if rerr != nil {
		return nil, fmt.Errorf("forge: read location caveats for %s: %w", c, rerr)
	}
	if len(caveats.Location) == 0 {
		return nil, fmt.Errorf("forge: empty location URL set for %s", c)
	}
	target := caveats.Location[0]

	// space scopes the retrieve capability. Fall back to our
	// configured space if the commitment is the legacy form without
	// a Space field.
	space := caveats.Space
	if space == (did.DID{}) {
		space = f.spaces[0]
	}

	// audience for the retrieve invocation is the storage provider
	// that issued the commitment.
	storageProvider, err := did.Parse(loc.Commitment.With())
	if err != nil {
		return nil, fmt.Errorf("forge: parse storage provider DID: %w", err)
	}

	// Self-issued retrieval proof: space → sprue. Per-call to keep
	// the chain short-lived.
	retrievalProof, err := delegation.Delegate(
		f.spaceSigner,
		f.signer,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability(contentcap.Retrieve.Can(), space.String(), ucan.NoCaveats{}),
		},
		delegation.WithExpiration(int(time.Now().Add(60*time.Second).Unix())),
	)
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieval proof: %w", err)
	}

	rangeStart := loc.Position.Offset
	rangeEnd := rangeStart + loc.Position.Length - 1

	inv, err := contentcap.Retrieve.Invoke(
		f.signer,        // issuer = sprue
		storageProvider, // audience = piri
		space.String(),  // with = space
		contentcap.RetrieveCaveats{
			Blob:  contentcap.BlobDigest{Digest: caveats.Content.Hash()},
			Range: contentcap.Range{Start: rangeStart, End: rangeEnd},
		},
		delegation.WithProof(delegation.FromDelegation(retrievalProof)),
	)
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieve invocation: %w", err)
	}

	conn, err := rclient.NewConnection(storageProvider, &target)
	if err != nil {
		return nil, fmt.Errorf("forge: build retrieval connection: %w", err)
	}

	xres, hres, err := rclient.Execute(ctx, inv, conn)
	if err != nil {
		return nil, fmt.Errorf("forge: execute retrieve for %s: %w", c, err)
	}

	rcptLink, ok := xres.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("forge: no receipt for retrieve of %s", c)
	}
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(xres.Blocks()))
	if err != nil {
		return nil, fmt.Errorf("forge: build block reader: %w", err)
	}
	anyRcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return nil, fmt.Errorf("forge: build receipt: %w", err)
	}
	rcpt, err := receipt.Rebind[contentcap.RetrieveOk, failure.FailureModel](
		anyRcpt, contentcap.RetrieveOkType(), failure.FailureType(), captypes.Converters...,
	)
	if err != nil {
		return nil, fmt.Errorf("forge: rebind receipt: %w", err)
	}
	if _, err := result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel)); err != nil {
		return nil, fmt.Errorf("forge: retrieve %s: %w", c, err)
	}

	body, err := io.ReadAll(hres.Body())
	if err != nil {
		return nil, fmt.Errorf("forge: read retrieve body for %s: %w", c, err)
	}
	if uint64(len(body)) != loc.Position.Length {
		return nil, fmt.Errorf("forge: %s short read: got %d bytes, want %d",
			c, len(body), loc.Position.Length)
	}

	return block.NewBlockWithCid(body, c)
}

// newAuthorizeRetrieval returns the AuthorizeRetrievalFunc the
// IndexLocator calls before each indexer query. The space signer
// (root authority) directly authorizes the indexer to fetch any
// blob in the space. NoCaveats means "no specific blob digest
// constraint" — the indexer pulls whichever index blob it needs
// to satisfy the lookup.
//
// Mirrors the pattern in
// github.com/storacha/guppy/cmd/retrieve.go::94. Difference: ms3t's
// "user" is itself, so the proof chain is one hop (space → indexer)
// rather than the typical (user → upload service → indexer).
func newAuthorizeRetrieval(spaceSigner principal.Signer, indexerDID did.DID) locator.AuthorizeRetrievalFunc {
	return func(spaces []did.DID) (delegation.Delegation, error) {
		caps := make([]ucan.Capability[ucan.NoCaveats], 0, len(spaces))
		for _, space := range spaces {
			caps = append(caps, ucan.NewCapability(
				contentcap.Retrieve.Can(),
				space.String(),
				ucan.NoCaveats{},
			))
		}
		return delegation.Delegate(
			spaceSigner,
			indexerDID,
			caps,
			delegation.WithExpiration(int(time.Now().Add(60*time.Second).Unix())),
		)
	}
}
