package store

import (
	// for schema embed
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-libstoracha/capabilities/types"
)

//go:embed error.ipldsch
var errorSchema []byte

var errorTS = mustLoadTS()

func mustLoadTS() *schema.TypeSystem {
	ts, err := types.LoadSchemaBytes(errorSchema)
	if err != nil {
		panic(fmt.Errorf("loading error schema: %w", err))
	}
	return ts
}

func ErrorType() schema.Type {
	return errorTS.TypeByName("Error")
}
