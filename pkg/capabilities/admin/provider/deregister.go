package provider

import (
	cdm "github.com/alanshaw/libracha/capabilities/datamodel"
	pdm "github.com/alanshaw/libracha/capabilities/provider/datamodel"
	"github.com/alanshaw/ucantone/validator/bindcap"
)

const DeregisterCommand = "/admin/provider/deregister"

type (
	DeregisterArguments = pdm.DeregisterArgumentsModel
	DeregisterOK        = cdm.UnitModel
)

var Deregister, _ = bindcap.New[*DeregisterArguments](DeregisterCommand)
