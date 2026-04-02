package provider

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result/ok"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"

	"github.com/storacha/go-libstoracha/capabilities/types"
)

const RegisterAbility = "admin/provider/register"

type RegisterCaveats struct {
	Provider did.DID
	Endpoint string
	Proof    ipld.Link
}

func (rc RegisterCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&rc, RegisterCaveatsType(), types.Converters...)
}

type RegisterOk = ok.Unit

var Register = validator.NewCapability(
	RegisterAbility,
	schema.DIDString(),
	schema.Struct[RegisterCaveats](RegisterCaveatsType(), nil, types.Converters...),
	validator.DefaultDerives[RegisterCaveats],
)
