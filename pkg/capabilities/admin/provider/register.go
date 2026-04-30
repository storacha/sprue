package provider

import (
	cdm "github.com/alanshaw/libracha/capabilities/datamodel"
	"github.com/alanshaw/ucantone/validator/bindcap"
	pdm "github.com/storacha/sprue/pkg/capabilities/admin/provider/datamodel"
)

const RegisterCommand = "/admin/provider/register"

type (
	RegisterArguments = pdm.RegisterArgumentsModel
	RegisterOK        = cdm.UnitModel
)

var Register, _ = bindcap.New[*RegisterArguments](RegisterCommand)
