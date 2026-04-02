package admin

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/admin/provider"
)

var log = logging.Logger("cmd/client/admin")

var Cmd = &cobra.Command{
	Use:   "admin",
	Short: "Administrate a running sprue via UCAN invocations",
}

func init() {
	Cmd.AddCommand(provider.Cmd)
}
