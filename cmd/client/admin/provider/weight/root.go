package weight

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "weight",
	Short: "Manage storage provider weights",
}

func init() {
	Cmd.AddCommand(setCmd)
}
