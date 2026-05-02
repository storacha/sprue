package uploader

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"go.uber.org/zap"

	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/ms3t/blockstore"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/routing"
)

// placeholderCID is the smallest legal raw-codec CID with an
// identity-hashed two-byte payload. It mirrors guppy's internal
// PlaceholderCID and is used as the "root" for the
// ShardedDagIndexView and the SpaceIndexAdd invocation: the index's
// Content() field and SpaceIndexAdd's rootCID parameter aren't
// load-bearing for inner-CID lookups (per guppy's own usage), so
// instead of inventing a synthetic root for each multi-root CAR we
// just pass this placeholder through.
var placeholderCID = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})

// Uploader is the seam between the log flusher and durable Forge
// storage.
type Uploader interface {
	// SubmitCAR ships one sealed CAR file (one log segment) to
	// Forge. The implementation streams the file body straight
	// into the HTTP PUT, never materializing it as a []block.Block
	// or re-encoding it as a CAR.
	SubmitCAR(ctx context.Context, roots []cid.Cid, src CARSource) error
}

// CARSource describes a sealed CAR file ready to ship. All fields
// refer to data that already exists on disk or was precomputed at
// seal time, so the uploader's per-flush memory footprint is
// dominated by HTTP send buffers rather than segment size.
type CARSource struct {
	// Path is the absolute path to the sealed CAR file. SubmitCAR
	// streams from this path into the HTTP PUT body.
	Path string
	// Size is the file's byte length. Set as the request's
	// Content-Length so net/http does not fall back to chunked
	// transfer encoding (piri requires Content-Length).
	Size int64
	// SHA256 is the SHA-256 multihash of the CAR's bytes. Computed
	// once at seal time and reused both as the blob digest in
	// allocate / accept and as the CAR digest the
	// ShardedDagIndexView is keyed by.
	SHA256 multihash.Multihash
	// Positions maps each block's CID to its offset/length inside
	// the CAR file, in the same shape Log appends populate at
	// write time. Used to build the index view without rescanning.
	Positions map[cid.Cid]blockstore.BlockLoc
}

// Forge is an Uploader that ships CARs to Forge from inside sprue,
// using sprue's own piriclient and indexerclient. No UCAN-over-HTTP
// loopback to sprue's own UCAN endpoint, no separate principal or
// delegation file: sprue's identity is the signer, and storage
// provider delegations are pulled live from sprue's routing service.
//
// One Submit:
//
//  1. Encode the CAR for this batch (with byte positions for each
//     inner block).
//  2. Allocate + HTTP PUT + Accept the CAR through a piri selected
//     by routing.Service.
//  3. Build a ShardedDagIndexView and archive it.
//  4. Allocate + HTTP PUT + Accept the index through a piri.
//  5. PublishIndexClaim against the indexing-service.
//
// Steps 2 and 4 share a helper that synthesizes the cause and put
// invocations that the existing space_blob_add handler builds from
// the inbound user UCAN. Here there's no inbound user UCAN — sprue's
// signer self-issues them so the audit shape matches.
type Forge struct {
	router        *routing.Service
	piriProvider  piriclient.Provider
	indexerClient *indexerclient.Client
	signer        principal.Signer
	spaceSigner   principal.Signer
	httpClient    *nethttp.Client
	logger        *zap.Logger
}

// ForgeConfig wires sprue's existing services into an Forge
// uploader. All fields are required.
//
// Signer is sprue's upload-service identity — used for piriclient
// invocations and as the audience of the self-issued retrieval
// delegation.
//
// SpaceSigner is the keypair of the space ms3t owns. ms3t generates
// and persists this on first run; its DID is the space resource for
// every PUT, and it acts as the root authority for self-issued
// space/content/retrieve delegations (so the indexer can fetch the
// index blob from piri on assert/index validation).
type ForgeConfig struct {
	Router        *routing.Service
	PiriProvider  piriclient.Provider
	IndexerClient *indexerclient.Client
	Signer        principal.Signer
	SpaceSigner   principal.Signer
	HTTPClient    *nethttp.Client // optional; defaults to nethttp.DefaultClient
	Logger        *zap.Logger
}

// NewForge validates the config and returns an Uploader that
// writes through sprue's internal services.
func NewForge(cfg ForgeConfig) (*Forge, error) {
	if cfg.Router == nil {
		return nil, errors.New("uploader: routing service is required")
	}
	if cfg.PiriProvider == nil {
		return nil, errors.New("uploader: piri provider is required")
	}
	if cfg.IndexerClient == nil {
		return nil, errors.New("uploader: indexer client is required")
	}
	if cfg.Signer == nil {
		return nil, errors.New("uploader: signer is required")
	}
	if cfg.SpaceSigner == nil {
		return nil, errors.New("uploader: space signer is required")
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = nethttp.DefaultClient
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Forge{
		router:        cfg.Router,
		piriProvider:  cfg.PiriProvider,
		indexerClient: cfg.IndexerClient,
		signer:        cfg.Signer,
		spaceSigner:   cfg.SpaceSigner,
		httpClient:    httpc,
		logger:        logger,
	}, nil
}

// SpaceDID returns the DID of the space ms3t owns.
func (u *Forge) SpaceDID() did.DID { return u.spaceSigner.DID() }

func (u *Forge) SubmitCAR(ctx context.Context, roots []cid.Cid, src CARSource) error {
	if len(roots) == 0 {
		return errors.New("uploader: at least one root required")
	}
	if src.Size <= 0 || len(src.Positions) == 0 {
		return nil
	}

	// 1. PUT the data CAR by streaming from disk. The sealed CAR
	//    file is byte-identical to what cars.WriteWithPositions
	//    would produce here (same placeholder header, same block
	//    order), and the seal step already hashed it — so we skip
	//    re-encoding and rehashing entirely.
	putCAR := func(url string, headers nethttp.Header) error {
		return httpPutFile(ctx, u.httpClient, url, headers, src.Path, src.Size)
	}
	if err := u.uploadBlob(ctx, src.SHA256, uint64(src.Size), putCAR); err != nil {
		return fmt.Errorf("uploader: ship car: %w", err)
	}

	// 2. Build a ShardedDagIndexView keyed off the CAR's multihash,
	//    using the precomputed positions from the segment.
	view := blobindex.NewShardedDagIndexView(cidlink.Link{Cid: placeholderCID}, 1)
	for c, loc := range src.Positions {
		view.SetSlice(src.SHA256, c.Hash(), blobindex.Position{
			Offset: loc.Offset,
			Length: loc.Length,
		})
	}
	archReader, err := view.Archive()
	if err != nil {
		return fmt.Errorf("uploader: archive index: %w", err)
	}
	indexBytes, err := io.ReadAll(archReader)
	if err != nil {
		return fmt.Errorf("uploader: read archived index: %w", err)
	}
	indexDigest, err := multihash.Sum(indexBytes, multihash.SHA2_256, -1)
	if err != nil {
		return fmt.Errorf("uploader: hash index: %w", err)
	}

	// 3. PUT the index blob. Small (one entry per inner CID), so
	//    in-memory is fine.
	putIndex := func(url string, headers nethttp.Header) error {
		return httpPut(ctx, u.httpClient, url, headers, indexBytes)
	}
	if err := u.uploadBlob(ctx, indexDigest, uint64(len(indexBytes)), putIndex); err != nil {
		return fmt.Errorf("uploader: ship index: %w", err)
	}

	// 4. Publish the index claim. The indexer needs to fetch our
	//    index blob from piri to validate the assertion, and piri
	//    requires UCAN auth on retrieval. We self-issue a
	//    space/content/retrieve delegation scoped to this specific
	//    index blob and pass it as clientAuth; sprue's
	//    indexerclient re-delegates from us to the indexer using
	//    that as the proof chain (mirrors the user-facing flow,
	//    just with sprue's signer playing the user's role).
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	retrievalAuth, err := contentcap.Retrieve.Delegate(
		u.spaceSigner, // issuer = space owner (root authority)
		u.signer,      // audience = sprue (next hop)
		u.SpaceDID().String(),
		contentcap.RetrieveCaveats{
			Blob:  contentcap.BlobDigest{Digest: indexDigest},
			Range: contentcap.Range{Start: 0, End: uint64(len(indexBytes)) - 1},
		},
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return fmt.Errorf("uploader: build retrieval auth: %w", err)
	}
	if err := u.indexerClient.PublishIndexClaim(ctx, u.SpaceDID(), placeholderCID, indexCID, retrievalAuth); err != nil {
		return fmt.Errorf("uploader: publish index claim: %w", err)
	}
	return nil
}

// uploadBlob runs the allocate → PUT → accept dance for one blob.
// putBody is invoked at most once per call, after a successful
// Allocate, with the URL and headers piri returned. The retry loop
// only re-runs Allocate (on ErrCandidateUnavailable), never the
// PUT itself, so a streaming putBody can safely consume its source
// in one shot. If Allocate reports the blob is already present
// (Address == nil), putBody is skipped entirely and accept proceeds.
func (u *Forge) uploadBlob(
	ctx context.Context,
	digest multihash.Multihash,
	size uint64,
	putBody func(url string, headers nethttp.Header) error,
) error {
	blob := captypes.Blob{Digest: digest, Size: size}

	// Synthesize a self-issued space/blob/add invocation as the cause.
	// Its link feeds the audit chain piri's handlers expect; never sent
	// over the wire.
	causeInv, err := spaceblobcap.Add.Invoke(
		u.signer, u.signer, u.SpaceDID().String(),
		spaceblobcap.AddCaveats{Blob: blob},
	)
	if err != nil {
		return fmt.Errorf("synthesize cause: %w", err)
	}
	cause := causeInv.Link()

	var exclusions []ucan.Principal
	for {
		provider, err := u.router.SelectStorageProvider(ctx, blob, routing.WithExclusions(exclusions...))
		if err != nil {
			return fmt.Errorf("select provider: %w", err)
		}
		log := u.logger.With(
			zap.Stringer("provider", provider.ID.DID()),
			zap.String("endpoint", provider.Endpoint.String()),
		)

		client, err := u.piriProvider.Client(provider.ID, provider.Endpoint)
		if err != nil {
			return fmt.Errorf("piri client: %w", err)
		}
		fetcher := internalDelegationFetcher{proof: provider.Proof}

		allocResp, allocInv, _, err := client.Allocate(ctx, &piriclient.AllocateRequest{
			Space:  u.SpaceDID(),
			Digest: digest,
			Size:   blob.Size,
			Cause:  cause,
		}, fetcher)
		if err != nil {
			if errors.Is(err, routing.ErrCandidateUnavailable) {
				log.Warn("provider unavailable, excluding and retrying", zap.Error(err))
				exclusions = append(exclusions, provider.ID)
				continue
			}
			return fmt.Errorf("allocate: %w", err)
		}

		// PUT bytes if piri allocated a fresh slot. If Address is nil
		// piri already has the blob; skip the upload.
		if allocResp.Address != nil {
			if err := putBody(allocResp.Address.URL.String(), allocResp.Address.Headers); err != nil {
				return fmt.Errorf("http put: %w", err)
			}
		}

		// Synthesize the http/put invocation (matches genPut in
		// sprue/pkg/service/handlers/space_blob_add.go) so Accept has
		// a stable Put link to chain off.
		putInv, err := synthesizePut(blob, allocInv)
		if err != nil {
			return fmt.Errorf("synthesize put: %w", err)
		}

		if _, _, _, err := client.Accept(ctx, &piriclient.AcceptRequest{
			Space:  u.SpaceDID(),
			Digest: digest,
			Size:   blob.Size,
			Put:    putInv.Link(),
		}, fetcher); err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		return nil
	}
}

// synthesizePut mirrors genPut in space_blob_add.go: derive a
// principal from the blob's digest, issue an http/put invocation
// with caveats that promise to fulfill from the alloc invocation's
// effects. The invocation is never executed; we only need its Link
// for AcceptRequest.Put.
func synthesizePut(blob captypes.Blob, allocInv invocation.Invocation) (invocation.Invocation, error) {
	provider, err := deriveDIDFromDigest(blob.Digest)
	if err != nil {
		return nil, err
	}
	fct := httpPutFact{id: provider.DID().String(), key: provider.Encode()}
	return httpcap.Put.Invoke(
		provider, provider, provider.DID().String(),
		httpcap.PutCaveats{
			URL: captypes.Promise{
				UcanAwait: captypes.Await{
					Selector: ".out.ok.address.url",
					Link:     allocInv.Link(),
				},
			},
			Headers: captypes.Promise{
				UcanAwait: captypes.Await{
					Selector: ".out.ok.address.headers",
					Link:     allocInv.Link(),
				},
			},
			Body: httpcap.Body{Digest: blob.Digest, Size: blob.Size},
		},
		delegation.WithFacts([]ucan.FactBuilder{fct}),
	)
}

func httpPut(ctx context.Context, client *nethttp.Client, urlStr string, headers nethttp.Header, body []byte) error {
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPut, urlStr, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range headers {
		if len(v) > 0 {
			req.Header.Set(k, v[0])
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http put status %s", resp.Status)
	}
	return nil
}

// httpPutFile streams a file body to the given URL. Setting
// req.ContentLength explicitly keeps net/http from defaulting to
// chunked transfer encoding on a non-Reader body — piri's PUT
// endpoint requires Content-Length.
func httpPutFile(ctx context.Context, client *nethttp.Client, urlStr string, headers nethttp.Header, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open car %s: %w", path, err)
	}
	defer f.Close()

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPut, urlStr, f)
	if err != nil {
		return err
	}
	req.ContentLength = size
	for k, v := range headers {
		if len(v) > 0 {
			req.Header.Set(k, v[0])
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http put status %s", resp.Status)
	}
	return nil
}

// internalDelegationFetcher matches the shape of the unexported
// delegationFetcher in space_blob_add.go: returns the storage
// provider's pre-issued delegation when the audience matches.
type internalDelegationFetcher struct {
	proof delegation.Delegation
}

func (df internalDelegationFetcher) GetDelegation(ctx context.Context, audience ucan.Principal) (delegation.Delegation, error) {
	if df.proof == nil {
		return nil, nil
	}
	if df.proof.Audience().DID() != audience.DID() {
		return nil, fmt.Errorf("delegation audience is %s, but invocation requires proof with audience %s",
			df.proof.Audience().DID(), audience.DID())
	}
	return df.proof, nil
}

// deriveDIDFromDigest mirrors deriveDID in space_blob_add.go. The
// derived principal is deterministic per digest.
func deriveDIDFromDigest(digest multihash.Multihash) (principal.Signer, error) {
	if len(digest) < ed25519.SeedSize {
		return nil, fmt.Errorf("digest too short for ed25519 seed: %d < %d", len(digest), ed25519.SeedSize)
	}
	seed := digest[len(digest)-ed25519.SeedSize:]
	pk := ed25519.NewKeyFromSeed(seed)
	return ed25519signer.FromRaw(pk)
}

// httpPutFact mirrors the unexported fact in space_blob_add.go.
// Embeds the derived principal's keys so downstream actors can
// re-derive and sign receipts.
type httpPutFact struct {
	id  string
	key []byte
}

func (hpf httpPutFact) ToIPLD() (map[string]datamodel.Node, error) {
	keys, err := qp.BuildMap(basicnode.Prototype.Any, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, hpf.id, qp.Bytes(hpf.key))
	})
	if err != nil {
		return nil, err
	}
	return map[string]datamodel.Node{
		"keys": keys,
	}, nil
}

var _ Uploader = (*Forge)(nil)
