package identity

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "identity",
	Short: "Manage identities",
}

func init() {
	Cmd.AddCommand(parseCmd)
}
