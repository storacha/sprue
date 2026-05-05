package provider

import (
	"github.com/fil-forge/ucantone/validator/bindcap"
	pdm "github.com/storacha/sprue/pkg/capabilities/admin/provider/datamodel"
)

const ListCommand = "/admin/provider/list"

type (
	ListArguments = pdm.ListArgumentsModel
	ListOK        = pdm.ListOKModel
	Provider      = pdm.ProviderModel
)

var List, _ = bindcap.New[*ListArguments](ListCommand)
