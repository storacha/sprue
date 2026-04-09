package provider

import (
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/admin/provider/weight"
)

var Cmd = &cobra.Command{
	Use:   "provider",
	Short: "Manage storage providers",
}

func init() {
	Cmd.AddCommand(deregisterCmd)
	Cmd.AddCommand(listCmd)
	Cmd.AddCommand(registerCmd)
	Cmd.AddCommand(weight.Cmd)
}
