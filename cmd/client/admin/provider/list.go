package provider

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/sprue/cmd/client/lib"
)

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List registered storage providers",
	Args:    cobra.NoArgs,
	RunE:    doList,
}

func doList(cmd *cobra.Command, args []string) error {
	c, _, _, id := lib.InitClient(cmd)

	res, err := c.AdminProviderList(cmd.Context(), id.Signer)
	cobra.CheckErr(err)

	if len(res.Providers) == 0 {
		cmd.Println("No providers registered")
		return nil
	}

	table := lib.NewTable(cmd.OutOrStdout())
	table.SetHeader([]string{"ID", "Weight", "URL"})
	for _, p := range res.Providers {
		table.Append([]string{p.ID.String(), fmt.Sprintf("%d", p.Weight), p.Endpoint})
	}
	table.Render()

	return nil
}
