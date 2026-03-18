package testutil

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-libstoracha/testutil"
)

var (
	Alice           = testutil.Alice
	Bob             = testutil.Bob
	Mallory         = testutil.Mallory
	Service         = testutil.Service
	WebService      = testutil.WebService
	RandomBytes     = testutil.RandomBytes
	RandomDID       = testutil.RandomDID
	RandomMultihash = testutil.RandomMultihash
	RandomSigner    = testutil.RandomSigner
)

func RandomCID(t *testing.T) cid.Cid {
	return cid.NewCidV1(cid.Raw, RandomMultihash(t))
}
