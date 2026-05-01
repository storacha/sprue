package ms3t

import (
	"fmt"
	"os"

	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
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
