package provider

import (
	"net/url"

	"github.com/alanshaw/ucantone/did"
	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/lib"
)

var registerCmd = &cobra.Command{
	Use:     "register <provider-did> <provider-url>",
	Aliases: []string{"add"},
	Short:   "Register a storage provider with the service",
	Args:    cobra.ExactArgs(2),
	RunE:    doRegister,
}

func doRegister(cmd *cobra.Command, args []string) error {
	c, _, _, _ := lib.InitClient(cmd)

	id, err := did.Parse(args[0])
	cobra.CheckErr(err)

	endpoint, err := url.Parse(args[1])
	cobra.CheckErr(err)

	_, err = c.AdminProviderRegister(cmd.Context(), id, endpoint.String())
	cobra.CheckErr(err)

	cmd.Println("Provider registered successfully")
	return nil
}
