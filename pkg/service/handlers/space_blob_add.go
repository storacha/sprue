package handlers

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"time"

	"github.com/ipld/go-ipld-prime/datamodel"
	"go.uber.org/zap"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/sprue/pkg/piriclient"
	"github.com/storacha/sprue/pkg/state"
)

// SpaceBlobAddService defines the interface for the space/blob/add handler.
type SpaceBlobAddService interface {
	ID() principal.Signer
	State() state.StateStore
	PiriClient(ctx context.Context) (*piriclient.Client, error)
	Logger() *zap.Logger
}

// httpPutFact contains the fact data for the http/put invocation.
type httpPutFact struct {
	id  string
	key []byte
}

func (hpf httpPutFact) ToIPLD() (map[string]datamodel.Node, error) {
	n, err := qp.BuildMap(basicnode.Prototype.Any, 2, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "id", qp.String(hpf.id))
		qp.MapEntry(ma, "keys", qp.Map(2, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, hpf.id, qp.Bytes(hpf.key))
		}))
	})
	if err != nil {
		return nil, err
	}

	return map[string]datamodel.Node{
		"keys": n,
	}, nil
}

// WithSpaceBlobAddMethod registers the space/blob/add handler.
// This handler orchestrates blob storage - allocates on piri and returns upload URL.
func WithSpaceBlobAddMethod(s SpaceBlobAddService) server.Option {
	// Generate a blob provider identity for http/put
	blobProvider, err := ed25519signer.Generate()
	if err != nil {
		panic(fmt.Sprintf("failed to generate blob provider identity: %v", err))
	}

	return server.WithServiceMethod(
		spaceblobcap.AddAbility,
		server.Provide(
			spaceblobcap.Add,
			func(ctx context.Context,
				cap ucan.Capability[spaceblobcap.AddCaveats],
				inv invocation.Invocation,
				iCtx server.InvocationContext,
			) (result.Result[spaceblobcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {

				spaceDIDStr := cap.With()
				blobInfo := cap.Nb().Blob
				digestHex := hex.EncodeToString(blobInfo.Digest)
				logger := s.Logger()

				logger.Debug("space/blob/add",
					zap.String("space", spaceDIDStr),
					zap.String("digest", digestHex[:16]),
					zap.Uint64("size", blobInfo.Size))

				// Parse the space DID
				spaceDID, err := did.Parse(spaceDIDStr)
				if err != nil {
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("invalid space DID: %w", err)),
					), nil, nil
				}

				// Get the piri client (queries provider table on each request)
				piriClient, err := s.PiriClient(ctx)
				if err != nil {
					logger.Error("failed to get piri client", zap.Error(err))
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("failed to get piri client: %w", err)),
					), nil, nil
				}
				if piriClient == nil {
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("no storage provider available")),
					), nil, nil
				}

				// Call piri's blob/allocate - returns invocation and receipt for use in effects
				logger.Debug("calling piri blob/allocate")
				allocResp, allocateInv, allocateRcpt, err := piriClient.Allocate(ctx, &piriclient.AllocateRequest{
					Space:  spaceDID,
					Digest: blobInfo.Digest,
					Size:   blobInfo.Size,
					Cause:  inv.Link(),
				})
				if err != nil {
					logger.Error("piri allocate failed", zap.Error(err))
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("piri allocate failed: %w", err)),
					), nil, nil
				}

				logger.Debug("piri allocate succeeded",
					zap.Uint64("allocatedSize", allocResp.Size),
					zap.Uint64("requestedSize", blobInfo.Size),
					zap.String("invCID", allocateInv.Link().String()))

				// Store the allocation for later reference
				var uploadURL *url.URL
				if allocResp.Address != nil {
					uploadURL = &allocResp.Address.URL
				} else {
					firstProvider, err := s.State().GetFirstProvider(ctx)
					if err != nil {
						logger.Error("failed to get first provider", zap.Error(err))
					} else if firstProvider != nil {
						uploadURL = firstProvider.Endpoint
					}
				}

				alloc := &state.Allocation{
					Space:     spaceDID,
					Digest:    blobInfo.Digest,
					Size:      blobInfo.Size,
					Cause:     inv.Link(),
					ExpiresAt: time.Now().Add(24 * time.Hour),
					PiriNode:  piriClient.PiriDID().String(),
					UploadURL: uploadURL,
				}
				if err := s.State().PutAllocation(ctx, digestHex, alloc); err != nil {
					logger.Error("failed to store allocation", zap.Error(err))
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("storing allocation: %w", err)),
					), nil, nil
				}

				// Use piri's invocation and receipt directly (returned from Allocate call above)
				// This ensures the invocation CID in effects matches what piri signed
				piriDID := piriClient.PiriDID()

				// Create http/put invocation
				fct := httpPutFact{
					id:  blobProvider.DID().String(),
					key: blobProvider.Encode(),
				}
				httpPutInv, err := httpcap.Put.Invoke(
					blobProvider,
					blobProvider,
					blobProvider.DID().String(),
					httpcap.PutCaveats{
						URL: captypes.Promise{
							UcanAwait: captypes.Await{
								Selector: ".out.ok.address.url",
								Link:     allocateRcpt.Root().Link(),
							},
						},
						Headers: captypes.Promise{
							UcanAwait: captypes.Await{
								Selector: ".out.ok.address.headers",
								Link:     allocateRcpt.Root().Link(),
							},
						},
						Body: httpcap.Body{
							Digest: blobInfo.Digest,
							Size:   blobInfo.Size,
						},
					},
					delegation.WithFacts([]ucan.FactBuilder{fct}),
				)
				if err != nil {
					return nil, nil, fmt.Errorf("creating http put invocation: %w", err)
				}

				// Create blob/accept invocation
				// Use WithNoExpiration so the invocation CID is deterministic
				// (otherwise each invocation gets a different expiration timestamp)
				acceptInv, err := blobcap.Accept.Invoke(
					iCtx.ID(),
					piriDID,
					piriDID.String(),
					blobcap.AcceptCaveats{
						Space: spaceDID,
						Blob: captypes.Blob{
							Digest: blobInfo.Digest,
							Size:   blobInfo.Size,
						},
						Put: blobcap.Promise{
							UcanAwait: blobcap.Await{
								Selector: ".out.ok",
								Link:     httpPutInv.Root().Link(),
							},
						},
					},
					delegation.WithNoExpiration(),
				)
				if err != nil {
					return nil, nil, fmt.Errorf("creating accept invocation: %w", err)
				}

				// Store the accept invocation link in the allocation so we can use it
				// as the key when storing the receipt later
				alloc.AcceptInvLink = acceptInv.Link()
				if err := s.State().PutAllocation(ctx, digestHex, alloc); err != nil {
					logger.Error("failed to update allocation", zap.Error(err))
					return result.Error[spaceblobcap.AddOk, failure.IPLDBuilderFailure](
						failure.FromError(fmt.Errorf("updating allocation: %w", err)),
					), nil, nil
				}

				// Create ucan/conclude invocation with allocate receipt
				concludeInv, err := ucancap.Conclude.Invoke(
					iCtx.ID(),
					piriDID,
					cap.With(),
					ucancap.ConcludeCaveats{
						Receipt: allocateRcpt.Root().Link(),
					},
				)
				if err != nil {
					return nil, nil, fmt.Errorf("creating conclude invocation: %w", err)
				}
				// Attach the allocate receipt blocks to the conclude invocation
				for blk, err := range allocateRcpt.Blocks() {
					if err != nil {
						return nil, nil, fmt.Errorf("getting allocate receipt block: %w", err)
					}
					concludeInv.Attach(blk)
				}

				// Build fork effects with all the tasks
				forks := []fx.Effect{
					fx.FromInvocation(allocateInv),
					fx.FromInvocation(concludeInv),
					fx.FromInvocation(httpPutInv),
					fx.FromInvocation(acceptInv),
				}
				fxs := fx.NewEffects(fx.WithFork(forks...))

				// Return success with the Site promise
				ok := spaceblobcap.AddOk{
					Site: captypes.Promise{
						UcanAwait: captypes.Await{
							Selector: ".out.ok.site",
							Link:     acceptInv.Root().Link(),
						},
					},
				}

				logger.Debug("returning success with effects")
				return result.Ok[spaceblobcap.AddOk, failure.IPLDBuilderFailure](ok), fxs, nil
			},
		),
	)
}
