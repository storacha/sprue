package ipldutil

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// ToCID converts an [ipld.Link] to a [cid.Cid]. It'll type assert the link to a
// [cidlink.Link] first, and if that fails it'll attempt to parse the string
// representation of the link as a CID.
func ToCID(l ipld.Link) (cid.Cid, error) {
	if c, ok := l.(cidlink.Link); ok {
		return c.Cid, nil
	}
	return cid.Parse(l.String())
}
