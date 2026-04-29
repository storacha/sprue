package uploader

import (
	"fmt"
	"net/url"
	"os"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/car"
	uhttp "github.com/storacha/go-ucanto/transport/http"
	guppyclient "github.com/storacha/guppy/pkg/client"
)

// LoadOrCreateSigner reads a persisted principal.Signer from path or
// generates and writes a fresh one if the file does not exist. The
// on-disk format is the canonical did:key string representation
// (signer.Format).
//
// The returned signer's DID is what the operator passes to a delegator
// when requesting a `space/blob/add` + `space/index/add` delegation.
func LoadOrCreateSigner(path string) (principal.Signer, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		s, err := signer.Generate()
		if err != nil {
			return nil, fmt.Errorf("uploader: generate signer: %w", err)
		}
		formatted, err := signer.Format(s)
		if err != nil {
			return nil, fmt.Errorf("uploader: format signer: %w", err)
		}
		if err := os.WriteFile(path, []byte(formatted), 0o600); err != nil {
			return nil, fmt.Errorf("uploader: persist signer: %w", err)
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("uploader: read signer: %w", err)
	}
	s, err := signer.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("uploader: parse signer: %w", err)
	}
	return s, nil
}

// LoadDelegations reads a CAR-encoded delegation from path. The input
// is expected to be a single delegation per file; callers needing
// multiple delegations should pass multiple paths and concatenate the
// results.
func LoadDelegations(path string) ([]delegation.Delegation, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("uploader: read delegation: %w", err)
	}
	d, err := delegation.Extract(data)
	if err != nil {
		return nil, fmt.Errorf("uploader: parse delegation %s: %w", path, err)
	}
	return []delegation.Delegation{d}, nil
}

// NewForgeClient assembles a guppy client targeting the given upload
// service. servicePrincipal is the DID of the upload service (e.g.
// sprue's did:web for production, or the local sprue did:key under
// smelt). serviceURL is the HTTP endpoint for UCAN invocations.
func NewForgeClient(
	serviceURL *url.URL,
	servicePrincipal did.DID,
	s principal.Signer,
	proofs []delegation.Delegation,
) (*guppyclient.Client, error) {
	channel := uhttp.NewChannel(serviceURL)
	codec := car.NewOutboundCodec()
	conn, err := uclient.NewConnection(servicePrincipal, channel, uclient.WithOutboundCodec(codec))
	if err != nil {
		return nil, fmt.Errorf("uploader: build connection: %w", err)
	}
	c, err := guppyclient.NewClient(
		guppyclient.WithConnection(conn),
		guppyclient.WithPrincipal(s),
		guppyclient.WithAdditionalProofs(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("uploader: build guppy client: %w", err)
	}
	return c, nil
}
