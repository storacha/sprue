package admin

import (
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/admin/provider"
)

var Cmd = &cobra.Command{
	Use:   "admin",
	Short: "Administrate a running sprue via UCAN invocations",
}

func init() {
	Cmd.AddCommand(provider.Cmd)
}
