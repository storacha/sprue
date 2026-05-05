package ucan_server

import (
	"context"
	"fmt"

	"github.com/fil-forge/ucantone/principal"
	ed_verifier "github.com/fil-forge/ucantone/principal/ed25519/verifier"
	secp_verifier "github.com/fil-forge/ucantone/principal/secp256k1/verifier"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/validator"
)

// PrincipalParser is a [validator.PrincipalParserFunc] that enables support for
// both ed25519 and secp256k1 principals in UCANs.
func PrincipalParser(str string) (principal.Verifier, error) {
	if v, err := ed_verifier.Parse(str); err == nil {
		return v, nil
	}
	if v, err := secp_verifier.Parse(str); err == nil {
		return v, nil
	}
	return nil, fmt.Errorf("unknown principal type: %s", str)
}

func NewAttestationVerifier(authority principal.Verifier) validator.NonStandardSignatureVerifierFunc {
	return func(ctx context.Context, token ucan.Token, meta ucan.Container) error {
		// We only support attestations as delegations - it is expected that attested
		// delegation delegates to agent DID which is then used in the invocation.
		dlg, ok := token.(ucan.Delegation)
		if !ok {
			return fmt.Errorf("token is not a delegation")
		}
		for _, inv := range meta.Invocations() {
			if inv.Command() != ucan.AttestCommand {
				continue
			}
			// etc.
		}
		return nil
	}
}
