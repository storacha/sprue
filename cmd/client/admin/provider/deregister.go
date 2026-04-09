package provider

import (
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/cmd/client/lib"
)

var deregisterCmd = &cobra.Command{
	Use:     "deregister <provider-did>",
	Aliases: []string{"remove", "rm"},
	Short:   "Deregister a storage provider from the service",
	Args:    cobra.ExactArgs(1),
	RunE:    doDeregister,
}

func doDeregister(cmd *cobra.Command, args []string) error {
	c, _, _, id := lib.InitClient(cmd)

	providerID, err := did.Parse(args[0])
	cobra.CheckErr(err)

	_, err = c.AdminProviderDeregister(cmd.Context(), id.Signer, providerID)
	cobra.CheckErr(err)

	cmd.Println("Provider deregistered successfully")
	return nil
}
