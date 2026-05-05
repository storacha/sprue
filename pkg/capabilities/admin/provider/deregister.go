package provider

import (
	cdm "github.com/fil-forge/libforge/capabilities/datamodel"
	"github.com/fil-forge/ucantone/validator/bindcap"
	pdm "github.com/storacha/sprue/pkg/capabilities/admin/provider/datamodel"
)

const DeregisterCommand = "/admin/provider/deregister"

type (
	DeregisterArguments = pdm.DeregisterArgumentsModel
	DeregisterOK        = cdm.UnitModel
)

var Deregister, _ = bindcap.New[*DeregisterArguments](DeregisterCommand)
