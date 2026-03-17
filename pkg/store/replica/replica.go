package replica

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/sprue/pkg/store"
)

const (
	// ReplicaExistsErrorName is the name given to an error where the replica
	// already exists in the store.
	ReplicaExistsErrorName = "ReplicaExists"
	// ReplicaNotFoundErrorName is the name given to an error where the replica
	// is not found in the store.
	ReplicaNotFoundErrorName = "ReplicaNotFound"
)

var (
	ErrReplicaExists   = store.NewError(ReplicaExistsErrorName, "replica already exists")
	ErrReplicaNotFound = store.NewError(ReplicaNotFoundErrorName, "replica not found")
)

// Replication status for a blob.
type ReplicationStatus int

const (
	// Initial state, implies the service invoked and received a success receipt
	// for `blob/replica/allocate` from the replica node.
	Allocated ReplicationStatus = iota
	// The service has received a success receipt from the replica node for the
	// `blob/replica/transfer` task.
	Transferred
	// The service has either failed to allocate on a replica node or received an
	// error receipt for the `blob/replica/transfer` task or the receipt was never
	// communicated and the task has expired.
	Failed
)

func (s ReplicationStatus) String() string {
	switch s {
	case Allocated:
		return "allocated"
	case Transferred:
		return "transferred"
	case Failed:
		return "failed"
	}
	return "unknown"
}

type ReplicaRecord struct {
	// Space the blob is stored in.
	Space did.DID
	// Hash of the blob.
	Digest multihash.Multihash
	// The node delegated to store the replica.
	Provider did.DID
	// Status of the replication.
	Status ReplicationStatus
	// Link to `blob/replica/allocate` invocation instructing the replication.
	Cause cid.Cid
	// When the replica record was created.
	CreatedAt time.Time
	// When the replica record was last updated.
	UpdatedAt time.Time
}

type Store interface {
	// Add a replica to the store. May return [ErrReplicaExists].
	Add(ctx context.Context, space did.DID, digest multihash.Multihash, provider did.DID, status ReplicationStatus, cause cid.Cid) error
	// Retry a replication in the store, updating status + cause. May return [ErrReplicaNotFound].
	Retry(ctx context.Context, space did.DID, digest multihash.Multihash, provider did.DID, status ReplicationStatus, cause cid.Cid) error
	// Update the replication status. May return [ErrReplicaNotFound].
	SetStatus(ctx context.Context, space did.DID, digest multihash.Multihash, provider did.DID, status ReplicationStatus) error
	// List replicas for the given space/blob.
	List(ctx context.Context, space did.DID, digest multihash.Multihash) ([]ReplicaRecord, error)
}
