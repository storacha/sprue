package datamodel

import "github.com/alanshaw/ucantone/did"

type SetArgumentsModel struct {
	Provider          did.DID `cborgen:"provider" dagjsongen:"provider"`
	Weight            int64   `cborgen:"weight" dagjsongen:"weight"`
	ReplicationWeight int64   `cborgen:"replicationWeight" dagjsongen:"replicationWeight"`
}
