package handlers

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"go.uber.org/zap"

	"github.com/storacha/sprue/pkg/indexerclient"
	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/state"
)

// UCANConcludeService defines the interface for the ucan/conclude handler.
type UCANConcludeService interface {
	ID() principal.Signer
	State() state.StateStore
	PiriClient(ctx context.Context) (*piriclient.Client, error)
	IndexerClient() *indexerclient.Client
	Logger() *zap.Logger
}

// WithUCANConcludeMethod registers the ucan/conclude handler.
// This handler processes receipt conclusions from clients.
// When it receives an http/put receipt, it calls blob/accept on piri
// and stores the accept receipt for later retrieval.
func WithUCANConcludeMethod(s UCANConcludeService) server.Option {
	return server.WithServiceMethod(
		ucancap.ConcludeAbility,
		server.Provide(
			ucancap.Conclude,
			func(ctx context.Context,
				cap ucan.Capability[ucancap.ConcludeCaveats],
				inv invocation.Invocation,
				iCtx server.InvocationContext,
			) (result.Result[ucancap.ConcludeOk, failure.IPLDBuilderFailure], fx.Effects, error) {
				logger := s.Logger()
				receiptLink := cap.Nb().Receipt

				logger.Debug("ucan/conclude received receipt", zap.String("receipt", receiptLink.String()))

				// Read the concluded receipt from the invocation's attached blocks
				anyReader := receipt.NewAnyReceiptReader(captypes.Converters...)
				rcpt, err := anyReader.Read(receiptLink, inv.Blocks())
				if err != nil {
					logger.Error("failed to read concluded receipt", zap.Error(err))
					// Still acknowledge even if we can't read the receipt
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// Get the invocation that the receipt is for
				ranInv, ok := rcpt.Ran().Invocation()
				if !ok {
					logger.Debug("receipt ran is not an invocation")
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// Check if this is an http/put receipt
				if len(ranInv.Capabilities()) == 0 {
					logger.Debug("invocation has no capabilities")
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				ability := ranInv.Capabilities()[0].Can()
				logger.Debug("receipt is for ability", zap.String("ability", ability))

				if ability != httpcap.PutAbility {
					// Not an http/put receipt, just acknowledge
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// This is an http/put receipt - we need to call blob/accept on piri
				logger.Debug("processing http/put receipt")

				// Extract the body (blob info) from the http/put invocation
				putCap := ranInv.Capabilities()[0]
				putMatch, err := httpcap.Put.Match(validator.NewSource(putCap, ranInv))
				if err != nil {
					logger.Error("failed to match http/put capability", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				body := putMatch.Value().Nb().Body
				digestHex := hex.EncodeToString(body.Digest)

				logger.Debug("http/put for blob",
					zap.String("digest", digestHex[:16]),
					zap.Uint64("size", body.Size))

				// Find the allocation for this blob
				alloc, err := s.State().GetAllocation(ctx, digestHex)
				if err != nil {
					logger.Error("error getting allocation", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}
				if alloc == nil {
					logger.Debug("allocation not found for digest", zap.String("digest", digestHex[:16]))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// Get the piri client (queries provider table on each request)
				piriClient, err := s.PiriClient(ctx)
				if err != nil {
					logger.Error("failed to get piri client", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}
				if piriClient == nil {
					logger.Debug("no storage provider available")
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// Call blob/accept on piri
				// Use a detached context so the accept call doesn't get canceled
				// when the HTTP request context completes (e.g., during parallel uploads)
				logger.Debug("calling piri blob/accept")
				acceptCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				acceptResp, piriRcpt, err := piriClient.Accept(acceptCtx, &piriclient.AcceptRequest{
					Space:  alloc.Space,
					Digest: body.Digest,
					Size:   body.Size,
					Put:    ranInv.Link(),
				})
				if err != nil {
					logger.Error("piri accept failed", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				logger.Debug("piri accept succeeded", zap.Any("site", acceptResp.Site))

				// Cache the location claim with the indexer
				if indexerClient := s.IndexerClient(); indexerClient != nil && acceptResp.Site != nil {
					// Extract the location claim delegation from piri's receipt blocks
					bs, bsErr := blockstore.NewBlockReader(blockstore.WithBlocksIterator(piriRcpt.Blocks()))
					if bsErr != nil {
						logger.Error("failed to create blockstore from piri receipt", zap.Error(bsErr))
					} else {
						locationClaim, claimErr := delegation.NewDelegationView(acceptResp.Site, bs)
						if claimErr != nil {
							logger.Error("failed to read location claim delegation", zap.Error(claimErr))
						} else {
							// Convert piri's DID to a libp2p peer ID for the multiaddr
							piriPeerID, peerIDErr := didToPeerID(piriClient.PiriDID())
							if peerIDErr != nil {
								logger.Error("failed to convert piri DID to peer ID", zap.Error(peerIDErr))
							} else {
								// Build piri's provider addresses for the indexer
								// Include BOTH {blobCID} and {claim} endpoints, just like piri does
								// The /p2p/ component tells the indexer the provider's peer ID
								piriBlobAddrStr := fmt.Sprintf("/dns4/piri/tcp/3000/http/p2p/%s/http-path/piece%%2F%%7BblobCID%%7D", piriPeerID.String())
								piriClaimAddrStr := fmt.Sprintf("/dns4/piri/tcp/3000/http/p2p/%s/http-path/claim%%2F%%7Bclaim%%7D", piriPeerID.String())
								piriBlobAddr, maErr1 := multiaddr.NewMultiaddr(piriBlobAddrStr)
								piriClaimAddr, maErr2 := multiaddr.NewMultiaddr(piriClaimAddrStr)
								if maErr1 != nil || maErr2 != nil {
									logger.Error("failed to create piri multiaddr",
										zap.Error(maErr1),
										zap.NamedError("claimErr", maErr2))
								} else {
									logger.Debug("caching location claim",
										zap.String("peerID", piriPeerID.String()),
										zap.Int("addresses", 2))
									cacheErr := indexerClient.CacheLocationClaim(acceptCtx, locationClaim, []multiaddr.Multiaddr{piriBlobAddr, piriClaimAddr})
									if cacheErr != nil {
										logger.Error("failed to cache location claim with indexer", zap.Error(cacheErr))
										// Don't fail - indexing is best effort
									} else {
										logger.Debug("cached location claim with indexer",
											zap.String("digest", digestHex[:16]))
									}
								}
							}
						}
					}
				}

				// Create a new receipt with the correct Ran reference
				// The piri receipt references piriclient's accept invocation, but guppy
				// is polling for the accept invocation from space/blob/add effects.
				// We re-issue the receipt with the correct Ran so guppy can find it.
				acceptOk := blobcap.AcceptOk{
					Site: acceptResp.Site,
				}
				reissuedRcpt, err := receipt.Issue(
					s.ID(),
					result.Ok[blobcap.AcceptOk, failure.IPLDBuilderFailure](acceptOk),
					ran.FromLink(alloc.AcceptInvLink),
				)
				if err != nil {
					logger.Error("failed to re-issue receipt", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				// Collect extra blocks from piri's receipt
				// This includes the location delegation blocks referenced by the Site link
				var extraBlocks []block.Block
				for blk, err := range piriRcpt.Blocks() {
					if err != nil {
						logger.Error("error iterating piri receipt blocks", zap.Error(err))
						continue
					}
					extraBlocks = append(extraBlocks, blk)
				}
				logger.Debug("collected extra blocks from piri receipt", zap.Int("count", len(extraBlocks)))

				acceptInvLink := alloc.AcceptInvLink.String()
				logger.Debug("re-issued receipt for task", zap.String("task", acceptInvLink))

				if err := s.State().PutReceipt(ctx, acceptInvLink, &state.StoredReceipt{
					Task:        alloc.AcceptInvLink,
					Receipt:     reissuedRcpt,
					ExtraBlocks: extraBlocks,
					AddedAt:     time.Now(),
				}); err != nil {
					logger.Error("failed to store receipt", zap.Error(err))
					return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
				}

				logger.Debug("stored accept receipt for task", zap.String("task", acceptInvLink))

				return result.Ok[ucancap.ConcludeOk, failure.IPLDBuilderFailure](ucancap.ConcludeOk{}), nil, nil
			},
		),
	)
}

// didToPeerID converts a did:key DID to a libp2p peer ID.
// This only works for ed25519 DIDs (did:key:z6Mk...).
func didToPeerID(d did.DID) (peer.ID, error) {
	vfr, err := verifier.Decode(d.Bytes())
	if err != nil {
		return "", fmt.Errorf("decoding DID to verifier: %w", err)
	}
	pub, err := crypto.UnmarshalEd25519PublicKey(vfr.Raw())
	if err != nil {
		return "", fmt.Errorf("unmarshaling ed25519 public key: %w", err)
	}
	return peer.IDFromPublicKey(pub)
}
