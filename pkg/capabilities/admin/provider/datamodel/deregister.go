package datamodel

import "github.com/alanshaw/ucantone/did"

type DeregisterArgumentsModel struct {
	Provider did.DID `cborgen:"provider" dagjsongen:"provider"`
}
