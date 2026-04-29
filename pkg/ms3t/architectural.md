# ms3t — S3 over Forge (MVP / prototype)

This document describes what the code in `sprue/pkg/ms3t/` actually
does today, running in smelt with the deployed wiring.

It is **not** an architecture spec for a production system. ms3t is a
prototype for "expose an S3 API on top of the Forge stack." Every
choice baked into the current shape is up for debate. The job of
this doc is to give the team enough of a map to read the code, ask
"why this and not that?", and weigh in on where to go next.

If you're looking for what isn't built yet or what was considered
and dropped, see "Choices we made (and the alternatives)" and
"Open questions" near the end.

## What ms3t is

A goroutine inside sprue that:

- Listens on a configured `host:port` and speaks the AWS S3 REST
  protocol (path-style; subset of operations: bucket CRUD, object
  PUT/GET/HEAD/DELETE, ListObjectsV2, range GETs)
- Stores object data as content-addressed CAR files in piri (via
  sprue's existing piriclient + routing + indexerclient — same
  packages sprue's own UCAN handlers use)
- Stores per-bucket "what's the current MST root?" in a small SQLite
  file alongside sprue
- Stores its own UCAN identity (a generated did:key) in a file
  alongside sprue

There is no other persistent state. ms3t holds no canonical block
data — every block read goes to the network.

## Local state

```
<data_dir>/
├── space.key             # ed25519 keypair, ms3t's UCAN identity / space root
└── ms3t-registry.db      # SQLite, one row per bucket
```

The SQLite schema (`pkg/ms3t/registry/sqlite.go`):

```sql
CREATE TABLE buckets (
    name           TEXT PRIMARY KEY,
    root_cid       BLOB,           -- current MST root, NULL for empty bucket
    forge_root_cid BLOB,           -- last root known to be in Forge
    created_at     INTEGER NOT NULL
);
```

`forge_root_cid` is plumbing for a batched-writes mode that isn't
currently active; in the deployed sync-writes mode it always equals
`root_cid` after each PUT/DELETE.

## How the data is shaped

Each S3 object's bytes get chunked into raw IPLD blocks (default 1
MiB, raw codec, sha256 multihash) and pointed at by an
`ObjectManifest` (DAG-CBOR):

```go
type ObjectManifest struct {
    Key         string
    ContentType string
    Created     int64
    Body        Body
}

type Body struct {
    Size      int64
    ChunkSize int64
    Chunks    []cid.Cid
    SHA256    []byte    // for ETag
}
```

The bucket itself is an MST keyed by S3 key, with leaves pointing at
manifest CIDs. The "current state" of a bucket is a single CID — the
MST root — held in the registry.

Every PUT/DELETE produces a new MST root via the
forked-from-atproto MST in `pkg/ms3t/mst/`, which is content-addressed
all the way down. The MST itself is fully described in its package
docs.

## How the data lives in Forge

For every S3 PUT, ms3t produces **one CAR file** containing:

- the new body chunks (raw blocks)
- the new ObjectManifest
- the mutated MST nodes (the path from leaf to root)

Plus a small **index blob** (also a CAR) describing where each inner
block sits within the data CAR, byte-offset-and-length, encoded as a
`blobindex.ShardedDagIndexView`.

Both blobs are uploaded to piri. The index is registered with the
indexing-service via `assert/index`. From that point onward, any
inner CID (an MST node, a manifest, a body chunk) is resolvable via:

1. Indexer query: `multihash → (CAR multihash, byte offset, length)`
2. Piri ranged GET: read `[offset, offset+length)` of the CAR

The indexer + piri retrieval flow are how reads find anything.

## The PUT flow

```
S3 client       ms3t              sprue services       piri      indexer
   │             │                       │              │           │
   │ PUT k=v     │                       │              │           │
   ├────────────▶│                       │              │           │
   │             │ load HEAD root_cid    │              │           │
   │             │ from registry         │              │           │
   │             │                       │              │           │
   │             │ chunk body into       │              │           │
   │             │ raw blocks (in mem)   │              │           │
   │             │                       │              │           │
   │             │ mst.Add(key, mfCid):                 │           │
   │             │  reads existing nodes via Forge ─────┤           │
   │             │  ◀─ indexer + ranged piri GETs       │           │
   │             │  produces new path nodes (in mem)    │           │
   │             │                                                  │
   │             │ pack body + manifest + mst nodes into one CAR    │
   │             │                                                  │
   │             │ piriclient.Allocate(carHash, carSize)            │
   │             ├──────────────────────▶│              │           │
   │             │◀── presigned URL ─────┤              │           │
   │             │                                                  │
   │             │ HTTP PUT carBytes ────────────────▶│              │
   │             │                                                  │
   │             │ piriclient.Accept                                │
   │             ├──────────────────────▶│              │           │
   │             │                                                  │
   │             │ build ShardedDagIndexView over CAR offsets       │
   │             │                                                  │
   │             │ Allocate + PUT + Accept the index blob ──┐       │
   │             │                                          ▼       │
   │             │                                                  │
   │             │ self-issue space/content/retrieve                │
   │             │   delegation (space → sprue) for the index blob  │
   │             │                                                  │
   │             │ indexerclient.PublishIndexClaim ─────────────────▶
   │             │                                                  │
   │             │ registry: CAS root_cid old → new                 │
   │             │                                                  │
   │ 200 OK + ETag                                                  │
   │◀────────────┤                                                  │
```

This is **synchronous**: every step blocks the client's PUT. Three
piri round trips per PUT (data CAR allocate+PUT+accept, index
allocate+PUT+accept, then index claim publication). Read-after-write
is correct because the assert/index has been published before 200 is
returned.

## The GET flow

```
S3 client       ms3t              indexer        piri
   │             │                   │             │
   │ GET k       │                   │             │
   ├────────────▶│                   │             │
   │             │ load HEAD root from registry    │
   │             │                                 │
   │             │ for each MST node walked from   │
   │             │   root toward the leaf:         │
   │             │   1. indexer query for cid ────▶│
   │             │   2. self-issue retrieve UCAN   │
   │             │   3. rclient.Execute on piri ──────────▶│
   │             │      ◀── block bytes (Range) ─────────┤
   │             │   4. parse, follow next link            │
   │             │                                         │
   │             │ once at the leaf manifest:              │
   │             │   for each body chunk: same dance       │
   │             │                                         │
   │             │ stream reassembled body to client       │
   │ 200 + bytes │                                         │
   │◀────────────┤                                         │
```

Every block read is a network round trip. There is no local cache
serving any of these reads.

The `rclient.Execute` call wraps the GET with a UCAN auth header
(`X-Agent-Message`) carrying a `space/content/retrieve` invocation
chained back to the space root — piri rejects unauthenticated
retrievals.

## Where the UCAN identity comes from

ms3t generates and persists its own ed25519 keypair on first run.
That keypair is the **space**: a `did:key` whose private half is in
`<data_dir>/space.key`. ms3t is the root UCAN authority over its own
space, which lets it self-issue all the delegations it needs:

- For the indexer: a blanket `space/content/retrieve` with
  `NoCaveats` so the indexer can fetch any blob in the space when
  validating an index claim
- For piri retrievals: a 60-second `space/content/retrieve` proof
  per Get, attached to a typed retrieve invocation
- For PublishIndexClaim: a per-call retrieval delegation scoped to
  the specific index blob

Sprue uses its own identity (`upload.pem` in smelt) for the piri
allocate/accept invocations and as the audience of ms3t's
self-issued retrieval delegations. So:

- **Sprue identity**: signs piri-side blob lifecycle invocations
- **ms3t space keypair**: signs anything that needs to chain back to
  "the owner of this space"

## Components map

```
                  ┌────────────────────────┐
S3 client ──────▶ │ ms3t HTTP listener     │  pkg/ms3t/server/
                  │ (S3 protocol → service)│
                  └───────────┬────────────┘
                              │
                  ┌───────────┴────────────┐
                  │ bucket.Service         │  pkg/ms3t/bucket/
                  │ load HEAD, mutate MST, │
                  │ build CAR, commit      │
                  └─────┬───────────┬──────┘
                        │           │
       ┌────────────────┘           └─────────────────┐
       │                                              │
   ┌───▼─────────────┐                       ┌────────▼────────────┐
   │ registry.SQL    │                       │ blockstore.Forge    │
   │ SQLite, HEAD    │                       │ reads via indexer   │
   │ pointer per     │                       │ + piri rclient.     │
   │ bucket          │                       │ Put: no-op.         │
   └─────────────────┘                       └─────────┬───────────┘
                                                       │
                  ┌────────────────────────┐           │
                  │ uploader.Internal      │  ◀────────┘ writes side
                  │ Submit: encode CAR,    │
                  │ piriclient + indexer-  │
                  │ client per call        │
                  └────────────┬───────────┘
                               │
            ┌──────────────────┼──────────────────────┐
            ▼                  ▼                      ▼
     ┌──────────┐       ┌──────────┐          ┌────────────────┐
     │ sprue    │       │ piri     │          │ indexing-      │
     │ routing  │       │ blob     │          │ service        │
     │ piriclient       │ store    │          │ assert/index   │
     └──────────┘       └──────────┘          └────────────────┘
       (in-process              (HTTP w/ UCAN auth)
        Go calls)
```

ms3t calls **sprue's services in-process** (Go function calls into
`pkg/piriclient`, `pkg/routing`, `pkg/indexerclient`). It does not
loopback through sprue's HTTP/UCAN handler. sprue's own UCAN
endpoint and ms3t's S3 endpoint are two unrelated listeners in the
same process.

## Choices we made (and the alternatives)

These are **prototype decisions**, made to ship something working.
Each is a place the team should weigh in on whether the choice
holds up.

### Sync writes, no local block cache

Every PUT blocks on three Forge round trips. Every GET hits the
network for every block. There is no local SQLite blockstore active
in this mode.

- **Why we picked this**: forces the read path to actually work
  end-to-end against real Forge. Closes the read-after-write race
  by construction. Simplest possible state model: only the registry
  is mutable.
- **Why it's awkward**: `aws s3 sync` of many small files is slow.
  An MST traversal during a PUT pays N network round trips for N
  existing nodes on the path, even though those nodes are
  deterministic.
- **Alternative we have code for**: `Batched(Internal)` uploader +
  SQLite blockstore as a read-through cache. This is the default
  when `ms3t.forge.no_cache: false`. Faster, but the
  `forge_root_cid` machinery has to actually do something — and the
  read-after-write window opens.

### ms3t owns its space

ms3t generates its own ed25519 keypair and is the root UCAN
authority over its own space. Self-issues every delegation it needs.

- **Why we picked this**: zero out-of-band provisioning. The first
  time sprue starts with `forge.enabled`, ms3t writes a key and
  uses it. No "go ask the delegator for a delegation, paste it
  here."
- **Why it's awkward**: ms3t-as-space-root is unusual. In a real
  multi-tenant deployment this doesn't model what we'd want — each
  S3 customer would presumably have their own space, with ms3t
  acting as a tenant-aware orchestrator.
- **Alternative we considered**: ms3t holds an externally-issued
  delegation chain into a pre-provisioned space. Better tenant
  story, requires delegation provisioning machinery.

### One CAR per S3 op (body + structural)

Body chunks ride in the same CAR as the structural blocks. The
indexer maps inner CIDs to byte ranges within the outer CAR. One
data-CAR upload + one index-blob upload per PUT.

- **Why we picked this**: matches what guppy does for filesystem
  uploads — minimum number of piri round trips per PUT. Body
  retrievals work via ranged GETs against the outer CAR.
- **Why it's awkward**: rules out direct-passthrough of body bytes
  (we'd want body chunks as their own piri blobs so a 307 redirect
  has a stable URL target).
- **Alternative we considered**: separate piri blobs per body
  chunk, smaller structural CAR for the MST + manifest. Doubles
  the per-PUT round trip count but enables passthrough.

### ms3t in the data path

The S3 client uploads body bytes to ms3t; ms3t uploads to piri.
Same on the read side. ms3t pays the bandwidth.

- **Why we picked this**: the alternative (direct passthrough)
  needs a Forge feature we don't have — see "Direct passthrough"
  under future directions.
- **Why it's awkward**: the operator running sprue + piri pays
  bandwidth twice (client→sprue, sprue→piri) when conceptually
  the bytes only need to move once. In a federated model where
  piri storage is run by different operators, this becomes
  structurally wrong (sprue's operator pays to deposit bytes onto
  someone else's hardware).

### Embedded in sprue

ms3t lives at `sprue/pkg/ms3t/` and is wired by sprue's fx graph.
No deployment artifact distinct from sprue.

- **Why we picked this**: zero auth coordination — ms3t is sprue,
  it has all sprue's identities and clients in-process. One binary
  to ship, one config file.
- **Why it's awkward**: every sprue release ships ms3t, every ms3t
  change requires a sprue release. Sprue maintainers inherit MST
  + S3 protocol surface area.
- **Alternative**: standalone ms3t binary, talks to sprue/piri via
  external UCAN-over-HTTP. (This exists at
  github.com/frrist/ms3t — a separate repo that was the original
  prototype before we copied into sprue.)

### Sticky-bucket routing (assumed but not built)

The current code assumes a single ms3t instance per bucket, via the
in-process `sync.Mutex` per-bucket lock. There is no cross-instance
coordination.

- **Why we picked this**: works for a single-process MVP.
- **What's needed for HA**: either sticky-bucket routing at a load
  balancer (hash bucket name → ms3t instance) or multi-writer with
  CAS retry and cache invalidation. Not implemented.

## Operational characteristics observed

These are observations from smelt, not promises:

- `aws s3 cp small.txt s3://demo/k` (small file): a few hundred
  milliseconds inside the docker network, dominated by the three
  Forge round trips
- `aws s3 cp s3://demo/k -` immediately after: works (sync writes
  close the race)
- `aws s3 sync` of many small files: visibly slow — each file pays
  the full Forge round-trip cost serially per S3 PUT
- `aws s3 ls`: walks MST through the network; cost grows with
  bucket size

We have not measured anything precisely. These are rough impressions.

## Known limitations

- **Slow.** Sync writes + no read cache. No effort has gone into
  performance.
- **No GC.** S3 DELETE removes the leaf from the MST. Body chunks
  become unreferenced from the current root, but we don't tell
  Forge to expire them. Storage grows monotonically.
- **No multipart upload.** S3 client splits files >8 MB into
  multipart by default; we don't implement it. Operators have to
  set `multipart_threshold = 5GB` in their AWS profile.
- **No `aws-chunked` body decoding.** The current AWS CLI default
  upload format. Operators have to set
  `request_checksum_calculation = when_required` to disable it.
- **Single-tenant.** One ms3t = one space.
- **Single-instance.** No HA story.
- **Disk and Guppy uploaders are dead code in sprue's wiring.**
  They exist in `pkg/ms3t/uploader/` for the standalone-ms3t use
  case; sprue only wires `Internal` (when forge enabled) or `Disk`
  (when forge disabled, in the cache mode that isn't currently
  deployed).

## Future directions (not implemented)

### Direct passthrough

The S3 client uploads body bytes directly to a piri presigned URL
via 307 redirect; ms3t never sees the bytes. Symmetric on reads.

- ms3t becomes purely control-plane
- Bandwidth shifts to piri's operator (correct in the federated
  model)
- Blocked on a Forge feature: piri/sprue must gate the
  client-visible 200 on an ms3t-side commit hook so ms3t can
  finalize the MST mutation before the client believes the PUT
  succeeded. Without this, the PUT-to-MST-commit window is a real
  race.

### Async writes

`Batched(Internal)` uploader: ack the PUT after local commit, ship
to Forge in the background. Faster, but introduces a window where
PUT-then-immediate-GET fails until the batch flushes. Code already
exists; it's the default mode when `no_cache: false`. We just don't
run with it.

### Read-through cache

SQLite blockstore populated on writes, consulted before falling
through to Forge. Order-of-magnitude speedup on hot reads at the
cost of cache invalidation complexity (when does ms3t know its
cached version is stale? Probably "never on its own" — would need
inputs from sprue's existing replay/invalidation mechanisms.).

### Multi-tenant

One ms3t serving N S3 customers, each in their own space. Requires
either:
- Per-tenant space delegations imported into ms3t (provisioning
  machinery), or
- ms3t generating + tracking per-tenant spaces, with some external
  authority for tenant identity

### Multi-instance

Either sticky-bucket routing at a load balancer (bucket name → ms3t
instance via consistent hash) or proper multi-writer with CAS retry
+ cache invalidation. Both unbuilt.

### Multipart upload + aws-chunked

Real S3 compatibility. Both are well-defined extensions of the
current per-PUT model — multipart effectively becomes "many
UploadPart calls accumulate body chunks; CompleteMultipartUpload
fires the MST mutation."

### GC

Walk reachable from current HEAD (and any retained snapshots), mark
those CIDs, ask Forge to expire the rest. Forge would need to grow
an `assert/expire`-style claim, and we'd need a retention policy.

## Open questions for the team

1. **Sync vs batched writes for MVP**: is `aws s3 sync` slowness
   acceptable for now, or should we wire `Batched` and accept the
   read-after-write window?

2. **Tenant model**: when we want N S3 customers, do they share
   ms3t's space or each get their own? The latter implies a
   provisioning step we currently avoid.

3. **Where should ms3t actually run?** Embedded in sprue is what
   we have. Standalone ms3t-with-Guppy works too (the original
   prototype). Embedded-in-piri was discussed and rejected. Are
   there scenarios where standalone matters more than we've
   assumed?

4. **Direct passthrough's commit-hook feature**: is this on
   anyone's roadmap? It's the lever for federated topologies. If
   not, the "ms3t in the data path" choice becomes load-bearing
   for any deployment beyond a single operator.

5. **Server-side concat for large GETs**: a multi-chunk body has
   no clean direct-passthrough path because there's no single URL
   to redirect to. Either large-object reads always go through
   ms3t (current behavior), or piri grows a "stream this ordered
   list of multihashes as one body" capability.

6. **MST for buckets, registry for buckets**: the registry
   (bucket → root CID) is itself a `string → CID` map. We could
   make it an MST too, store the registry MST in Forge, and have
   only one mutable pointer (the registry MST root). Discussed
   earlier; rejected for now because the registry needs SQL-style
   transactional CAS that Forge doesn't provide.

7. **Should the standalone ms3t repo at github.com/frrist/ms3t
   continue to exist?** It has the same code (modulo imports) and
   no consumer. The Disk and Guppy uploaders only make sense
   there.

## Reading the code

If you're new to ms3t and want to follow a request through:

- **PUT**: `pkg/ms3t/server/handlers.go::putObject` →
  `bucket.Service.PutObject` (in `pkg/ms3t/bucket/bucket.go`) →
  `chunker.putBody` → `mst.Add` → `CARBuffer.Commit` →
  `uploader.Internal.Submit` (in `pkg/ms3t/uploader/internal.go`)
  → registry CAS

- **GET**: `pkg/ms3t/server/handlers.go::getObject` →
  `bucket.Service.GetObject` → `mst.Get` (every node fetched via
  `blockstore.Forge.Get` in `pkg/ms3t/blockstore/forge.go`) →
  manifest decoded → body chunks fetched the same way → streamed
  to client

- **Where things plug into sprue**: `internal/fx/ms3t.go`. This is
  the only sprue-side file that knows about ms3t.

- **The MST itself**: `pkg/ms3t/mst/`. This is a fork of the
  atproto MST with relaxed key validation. Standalone, no
  dependencies on the rest of ms3t.
