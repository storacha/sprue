package weight

import (
	cdm "github.com/alanshaw/libracha/capabilities/datamodel"
	"github.com/alanshaw/ucantone/ucan/delegation/policy"
	"github.com/alanshaw/ucantone/validator/bindcap"
	"github.com/alanshaw/ucantone/validator/capability"
	wdm "github.com/storacha/sprue/pkg/capabilities/admin/provider/weight/datamodel"
)

const SetCommand = "/provider/weight/set"

type (
	SetArguments = wdm.SetArgumentsModel
	SetOK        = cdm.UnitModel
)

var Set, _ = bindcap.New[*SetArguments](
	SetCommand,
	capability.WithPolicyBuilder(
		policy.GreaterThanOrEqual(".weight", 0),
		policy.GreaterThanOrEqual(".replicationWeight", 0),
	),
)
