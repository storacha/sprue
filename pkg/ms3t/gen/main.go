// Generates CBOR marshal/unmarshal methods for ms3t types. Run from
// pkg/ms3t/:
//
//	go run ./gen
package main

import (
	"github.com/storacha/sprue/pkg/ms3t/bucket"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	cfg := cbg.Gen{MaxStringLength: 1_000_000}
	if err := cfg.WriteMapEncodersToFile("bucket/cbor_gen.go", "bucket",
		bucket.ObjectManifest{},
		bucket.Body{},
		bucket.FixedChunkerIndex{},
	); err != nil {
		panic(err)
	}
}
