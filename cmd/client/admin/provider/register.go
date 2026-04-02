package provider

import (
	"net/url"

	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/cmd/client/lib"
)

var registerCmd = &cobra.Command{
	Use:     "register <provider-did> <provider-url> <proof>",
	Aliases: []string{"add"},
	Short:   "Register a storage provider with the service",
	Args:    cobra.ExactArgs(3),
	RunE:    doRegister,
}

func doRegister(cmd *cobra.Command, args []string) error {
	c, _, _, id := lib.InitClient(cmd)

	providerID, err := did.Parse(args[0])
	cobra.CheckErr(err)

	endpoint, err := url.Parse(args[1])
	cobra.CheckErr(err)

	proof, err := delegation.Parse(args[2])
	cobra.CheckErr(err)

	_, err = c.AdminProviderRegister(cmd.Context(), id.Signer, providerID, endpoint.String(), proof)
	cobra.CheckErr(err)

	cmd.Println("Provider registered successfully")
	return nil
}
