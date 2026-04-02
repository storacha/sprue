package client

import (
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/admin"
)

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "Interact with a running sprue via UCAN invocations",
}

func init() {
	Cmd.AddCommand(admin.Cmd)
}
