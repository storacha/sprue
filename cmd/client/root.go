package client

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/admin"
)

var log = logging.Logger("cmd/client")

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "Interact with a running sprue via UCAN invocations",
}

func init() {
	Cmd.AddCommand(admin.Cmd)
}
