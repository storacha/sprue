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

const WeightSetAbility = "admin/provider/weight/set"

type WeightSetCaveats struct {
	Provider          did.DID
	Weight            int
	ReplicationWeight int
}

func (wc WeightSetCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&wc, WeightSetCaveatsType(), types.Converters...)
}

type WeightSetOk = ok.Unit

var WeightSet = validator.NewCapability(
	WeightSetAbility,
	schema.DIDString(),
	schema.Struct[WeightSetCaveats](WeightSetCaveatsType(), nil, types.Converters...),
	validator.DefaultDerives[WeightSetCaveats],
)
