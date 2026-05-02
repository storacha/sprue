# ms3t — S3 over Forge (current state)

This document describes the implementation under `sprue/pkg/ms3t/`
as it stands today. ms3t is an embedded S3 protocol listener that
runs in-process inside sprue (or, in tests, against an in-memory
harness) and translates S3 requests into mutations of a per-bucket
Merkle Search Tree, durably journaled to a local LSM-style log and
asynchronously shipped to Forge.

It is still a prototype: many S3 features are unimplemented (see
"Not implemented" near the end), and several knobs that future
production work will tighten are noted as TODOs in code.

## At a glance

- **Protocol layer** — `github.com/versity/versitygw`. We get a
  near-complete S3 REST front end (sigv4, path-style addressing,
  the standard verb shapes) by implementing versitygw's
  `backend.Backend` interface.
- **Backend adapter** — `pkg/ms3t/s3frontend.Backend`. Wires every
  served verb into ms3t's domain primitives. Anything we haven't
  implemented inherits `ErrNotImplemented` from versitygw's
  `backend.BackendUnsupported`.
- **Per-op transaction** — `pkg/ms3t/bucketop.Tx`. Acquires the
  per-bucket lock, snapshots the bucket's published Root from the
  registry, and exposes a per-tx staging buffer + CBOR-typed view
  over it. On Commit it fsyncs the batch into the log and
  CAS-advances the registry Root in one shot.
- **Storage tiers** — an LSM-style local log:
  - *Hot*  — current open segment (CAR + .ops sidecar) on local
    disk. AppendBatch fsyncs both files before returning.
  - *Warm* — sealed segments retained on local disk for fast reads.
  - *Cold* — segments shipped off-host to Forge (piri CAR + index
    claim). The layered read tier falls through to Forge on misses.
- **Persistent metadata** — Postgres, under the `ms3t` schema.
  Per-bucket Root + per-segment lifecycle live in the same database.
- **Identity** — ms3t owns its own ed25519 keypair (the *space*) and
  is the root UCAN authority for self-issued
  `space/content/retrieve` delegations. Sprue's identity is the
  audience for piri allocate/accept invocations.

## On-disk layout

```
<DataDir>/
├── space.key                              # ed25519 keypair (UCAN identity)
└── segments/
    ├── seg-NNNNNNNNNNNNNNNNNNNN.car       # one CAR per segment
    ├── seg-NNNNNNNNNNNNNNNNNNNN.ops       # per-batch (bucket, root) records
    └── seg-NNNNNNNNNNNNNNNNNNNN.idx       # JSON sidecar (sealed only)
```

- `.car` — CAR v1 with a placeholder root in the header. Block
  frames are appended via `cars.WriteBlocksAt`. Per-batch fsync.
- `.ops` — append-only sidecar of `[bucket: text, root: bytes]`
  CBOR records, each prefixed by a 4-byte big-endian length. One
  record per AppendBatch (one S3 op).
- `.idx` — written atomically (tmp + rename) at seal time. JSON:
  `{seq, size_bytes, sha256_hex, sealed_at, blocks: [{cid,
  offset, length}], op_roots: [{bucket, root}]}`. The post-crash
  source of truth for sealed segments.

## Postgres schema

Migrations are in `pkg/ms3t/migrations/sql/`, applied via goose
against the caller-provided `*pgxpool.Pool` at startup. All ms3t
tables live under the `ms3t` schema; goose's bookkeeping is at
`ms3t.goose_db_version`, so it never collides with other migrations
on the same database.

```sql
CREATE TABLE ms3t.buckets (
    name           TEXT     PRIMARY KEY,
    root_cid       BYTEA,           -- current MST root, NULL for empty bucket
    forge_root_cid BYTEA,           -- last MST root whose blocks shipped to Forge
    created_at     BIGINT   NOT NULL
);

CREATE TABLE ms3t.segments (
    seq         BIGINT   PRIMARY KEY,
    state       TEXT     CHECK (state IN ('open','sealed','flushed')),
    sealed_at   BIGINT,
    flushed_at  BIGINT,
    size_bytes  BIGINT   DEFAULT 0,
    car_sha256  BYTEA
);

CREATE TABLE ms3t.segment_op_roots (
    seq         BIGINT,
    seq_within  INT,
    bucket      TEXT     NOT NULL,
    root_cid    BYTEA    NOT NULL,
    PRIMARY KEY (seq, seq_within),
    FOREIGN KEY (seq) REFERENCES ms3t.segments(seq) ON DELETE CASCADE
);
CREATE INDEX ON ms3t.segment_op_roots (bucket, seq);

CREATE SEQUENCE ms3t.segment_seq;
```

`forge_root_cid` is the per-bucket high-water mark of "what's
durably in Forge." When the flusher succeeds, it advances
`forge_root_cid` for every op-root the segment carried in the same
transaction that flips the segment's state to `flushed`.

## Per-object data shape

Each S3 object is represented by an `ObjectManifest` block whose
`Body` field describes how the bytes are framed. The Body shape is
polymorphic via the `Format` string; the only codec today is
`fixed-v1`.

```go
type ObjectManifest struct {
    Key         string `cborgen:"k"`
    ContentType string `cborgen:"ct"`
    Created     int64  `cborgen:"t"`
    Body        Body   `cborgen:"b"`
}

type Body struct {
    Size    int64   `cborgen:"s"`
    SHA256  []byte  `cborgen:"h"`     // hex of this is the ETag we serve today
    Content cid.Cid `cborgen:"c"`     // points at format-specific DAG root
    Format  string  `cborgen:"f"`
}

const FormatFixed = "fixed-v1"

type FixedChunkerIndex struct {
    ChunkSize int64     `cborgen:"cs"`
    Chunks    []cid.Cid `cborgen:"c"`
}
```

The `BodyCodec` interface (`pkg/ms3t/bucket/chunker.go`) is the
seam:

```go
type BodyWriter interface {
    Chunk(ctx context.Context, w blockstore.WriteStore, r io.Reader) (Body, error)
}
type BodyReader interface {
    Format() string
    Open(ctx context.Context, bs blockstore.ReadStore, body Body) io.ReadCloser
    OpenRange(ctx context.Context, bs blockstore.ReadStore, body Body, start, end int64) io.ReadCloser
}
type BodyCodec interface { BodyWriter; BodyReader }
```

`FixedChunker` reads the body in `ChunkSize`-byte (default 1 MiB)
segments, writes each as a raw IPLD block, and finishes with a
`FixedChunkerIndex` CBOR block listing the chunk CIDs in order.
`Body.Content` points at the index. Reads lazily fetch the index on
first call and stream chunks; ranged reads translate the absolute
range into `(chunkIndex, in-chunk-offset)` and skip ahead.

Adding a new codec is a new `BodyCodec` implementation plus a new
`Format` constant; the Body / Manifest shape stays stable.

## Bucket as MST

The bucket is a Merkle Search Tree (forked from the atproto MST in
`pkg/ms3t/mst/`, with relaxed key validation) keyed by S3 object
key. Each leaf points at an ObjectManifest CID. The bucket's
"current state" is a single MST root CID held at
`ms3t.buckets.root_cid`.

Public MST methods used by the backend: `Add`, `Update`, `Delete`,
`Get`, `GetPointer`, `WalkLeavesFromNocache`. The MST is
content-addressed all the way down — every mutation produces a new
root CID. Mutated nodes are written through the staging buffer
(which feeds the log on Commit) via `tx.Put`/`tx.PutBlock`.

## Storage tiers (LSM)

```
   ┌────────────────────────────────────────────┐
   │ HOT   open segment                         │
   │       AppendBatch fsyncs CAR + .ops sidecar│ ◀─┐
   │       before returning                     │   │
   └──────────────┬─────────────────────────────┘   │
                  │                                 │
        seal-on-bytes  /  seal-on-age               │ reads
                  │                                 │ fall
   ┌──────────────▼─────────────────────────────┐   │ through
   │ WARM  sealed segments on local disk        │   │ here
   │       .idx sidecar persisted               │   │
   │       (atomic tmp+rename)                  │ ◀─┤
   │       MarkSegmentSealed in Postgres        │   │
   └──────────────┬─────────────────────────────┘   │
                  │                                 │
            Flush callback                          │
                  │                                 │
   ┌──────────────▼─────────────────────────────┐   │
   │ COLD  shipped to Forge (piri + indexer)    │   │
   │       per-bucket forge_root_cid advanced   │ ◀─┤
   │       retention sweeps after cfg.Retain    │   │
   └──────────────┬─────────────────────────────┘   │
                  │                                 │
            network reads                           │
                  ▼                                 ▼
       ┌──────────────────┐               ┌──────────────────┐
       │ blockstore.Forge │               │ Layered.GetBlock │
       │ indexer + piri   │               │ open → sealed →  │
       │ ranged GETs      │               │ Forge fall-through│
       └──────────────────┘               └──────────────────┘
```

The read path (`blockstore.Layered`):

1. Open segment's in-memory index (CIDs from blocks just appended).
2. Sealed segments on local disk, newest-first by seq.
3. Forge — only reached on local miss. `blockstore.Forge` queries
   the indexer for the block's `(CAR multihash, offset, length)`,
   self-issues a scoped retrieval delegation, and does a ranged
   GET against piri.

The write path (per S3 op):

1. `bucketop.Coordinator.Begin(bucket)` — clones the bucket name
   (defends against fiber's recycled request buffer), acquires the
   per-bucket lock, snapshots the bucket's State from the registry.
2. `BodyCodec.Chunk(ctx, tx, body)` — writes body chunks +
   FixedChunkerIndex through `tx.PutBlock`/`tx.Put` (which buffer
   in `OpStaging`).
3. `tx.Put(manifest)` — writes the ObjectManifest block.
4. `t.Add(key, mfCid)` (or Update / Delete) → `t.GetPointer(tx)` —
   serializes the new MST nodes through the same staging buffer,
   returns the new root CID.
5. `tx.Commit(newRoot)`:
   - `staging.Commit` calls `log.AppendBatch(blocks, OpRoot{bucket,
     root})`. Segment.append fsyncs CAR + .ops before returning.
   - `reg.CASRoot(bucket, expect, next)` advances the bucket Root
     in Postgres.
   - Releases the per-bucket lock.
6. Return 200 to the client.

The flush path (background goroutine in `logstore.Store`):

1. Pick a sealed segment off the queue.
2. Build a `uploader.CARSource` from segment metadata
   (`{Path, Size, SHA256, Positions}` — every field already on the
   segment, no rescan).
3. `uploader.Forge.SubmitCAR`:
   - Allocate + HTTP PUT (streaming straight from `CARSource.Path`)
     + Accept the data CAR via a piri selected by routing.
   - Build a `ShardedDagIndexView` from `CARSource.Positions`,
     archive it, allocate + PUT + Accept the index blob.
   - Self-issue a `space/content/retrieve` delegation scoped to the
     index blob.
   - Publish the index claim against the indexing-service.
4. `meta.MarkSegmentFlushed(seq, flushedAt, opRoots)` in one
   Postgres transaction — flips state to `flushed`, writes
   `flushed_at`, advances `forge_root_cid` for every op-root the
   segment carried.
5. Retention: if there are more than `Retain` flushed segments on
   disk, retire the oldest (close fds, unlink files, delete the
   Postgres row).

The default seal triggers (set in `pkg/ms3t/logstore/config.go`)
are 64 MiB or 5s; both can be overridden via `ServerConfig`.

## Module map

```
pkg/ms3t/
├── server.go                — Server, ServerConfig, ServerDeps, New, newFlushFunc
├── module.go                — fx Module + registerLifecycle (production wiring)
├── util.go                  — LoadOrCreateSigner (space.key)
│
├── s3frontend/              — versitygw backend.Backend implementation
│   ├── backend.go           — Backend, Recover (no-op), Drain (Coordinator.Close)
│   ├── bucket.go            — bucket-level handlers + ACL/policy/lock/versioning stubs
│   └── object.go            — object-level handlers + listWalk + lookupManifest
│
├── bucketop/                — per-bucket write transaction primitive
│   └── bucketop.go          — Coordinator, Tx, WithTx, WithLock, MutateFn
│
├── blockstore/              — read/write contracts + impls + Log seam
│   ├── store.go             — Reader, Writer, Store, BlockReader/Writer, etc.
│   ├── log.go               — Log interface, OpRoot, BlockLoc
│   ├── staging.go           — OpStaging (per-op buffer)
│   ├── layered.go           — Layered (composite read tier)
│   └── forge.go             — Forge (network base reader; no writes)
│
├── logstore/                — LSM-style segment-based log
│   ├── store.go             — Store, Open, AppendBatch, Get, Close
│   ├── segment.go           — Segment lifecycle + on-disk format
│   ├── recovery.go          — startup reconciliation
│   ├── config.go            — Config (Dir/SealBytes/SealAge/Retain/Flush/Meta)
│   └── types.go             — Meta interface, SegmentMeta, State
│
├── uploader/                — ship sealed segment to Forge
│   └── forge.go             — Uploader interface, CARSource, Forge.SubmitCAR
│
├── registry/                — Postgres-backed bucket and segment metadata
│   ├── registry.go          — Registry interface + State
│   ├── postgres.go          — Postgres bucket methods
│   └── segments.go          — Postgres methods satisfying logstore.Meta
│
├── bucket/                  — per-object data model + body codec
│   ├── manifest.go          — ObjectManifest, Body, FormatFixed, FixedChunkerIndex
│   ├── chunker.go           — BodyWriter / BodyReader / BodyCodec / FixedChunker
│   └── cbor_gen.go          — generated by gen/
│
├── mst/                     — atproto fork (relaxed key validation)
│
├── cars/                    — CAR encoding / scanning helpers
│   ├── encoder.go
│   └── reader.go
│
├── migrations/              — goose-applied SQL embed
│   └── sql/{00001_init,00002_segments}.sql
│
├── testing/                 — smoke harness + curated suite tests
│   ├── harness.go           — StartHarness + in-memory deps fakes
│   ├── integration.go       — Run/RunT, upstream Suite values
│   ├── smoke_test.go        — TestSmoke_* / TestSmokeXFail_* tables
│   ├── harness_test.go      — TestHarnessLifecycle
│   └── listbuckets_test.go  — TestListBucketsNamesStable (regression)
│
└── gen/                     — cborgen for bucket/cbor_gen.go
```

## Interfaces and seams

| Contract | Production impl | Test impl |
|---|---|---|
| `versitygw/backend.Backend` | `s3frontend.Backend` | (same; harness boots the full server) |
| `blockstore.Log` | `logstore.Store` | (same) |
| `blockstore.BlockReader` | `blockstore.Forge` | `testing.nopBaseReader` |
| `registry.Registry` + `logstore.Meta` | `*registry.Postgres` (one struct, both interfaces) | `testing.memStore` (one struct, both) |
| `uploader.Uploader` | `uploader.Forge` | `testing.nopUploader` |
| `bucket.BodyCodec` | `*bucket.FixedChunker` | (same) |

`s3frontend.Backend` is constructed with `(reg, rs, log, codec)` —
note that the read seam is a `blockstore.ReadStore` (no Put method),
so write paths can't accidentally route through it. Writes go via
`bucketop.Tx` which exposes the staging buffer behind the same
`Reader`/`Writer`/`BlockReader`/`BlockWriter` interfaces.

## Lifecycle: Server.New → Start → Stop

`pkg/ms3t/server.go::New(ctx, cfg, deps)`:

1. Validate inputs (`Addr`, `DataDir`, `RootAccess/RootSecret`, all
   `ServerDeps` fields present).
2. Apply defaults (`Region` → `us-east-1`, `ChunkSize` →
   `bucket.DefaultChunkSize` = 1 MiB, `MaxConnections` /
   `MaxRequests` → 4096).
3. Build a `logstore.FlushFunc` closure capturing the uploader +
   meta — this is what runs per sealed segment off the flush
   goroutine.
4. `logstore.Open(...)` — runs recovery (see next section), starts
   the flush + seal-ticker goroutines.
5. Construct `blockstore.NewLayered(log, deps.BaseBlockReader)`.
6. Construct `s3frontend.New(deps.Registry, layered, log, codec)`.
7. Build the versitygw `s3api.S3ApiServer` with single-account IAM,
   no audit/event sinks, generous concurrency limits.

`Start`: calls `Backend.Recover` (a no-op today; the LSM already
recovered in `logstore.Open`) and spawns the listener goroutine
(`s3api.ServeMultiPort`).

`Stop`: shuts the listener down and calls `Backend.Drain`, which
calls `Coordinator.Close` → `Log.Close` (force-seal the open
segment, drain the flush queue). Returns the joined error of both
steps.

## Recovery on startup

`logstore.Open` runs full reconciliation between disk and Postgres
before accepting writes:

1. Scan `<DataDir>/segments/` for `.car` files.
2. Query `Meta.ListUnflushedSegments()` for open/sealed rows.
3. Reconcile by `seq`:
   - **File + DB open** → rebuild as open via `cars.ScanFile` +
     `readAllOps`. Force-seal at startup; we never resume an open
     segment from a previous process.
   - **File + DB sealed** → load from `.idx`, re-enqueue for flush.
   - **File + .idx, no DB row** → rehydrate the DB row (the .idx is
     authoritative for sealed state), keep for retention.
   - **File only (no .idx, orphan from a torn seal)** → rebuild as
     open, seed DB, force-seal.
   - **DB row, no file** → log error, delete the DB row.
4. Sealed segments are placed at the head of the read fall-through
   list (newest-first by seq) so reads find recent writes first.

## Identity / Forge wiring

ms3t generates and persists its own ed25519 keypair on first run at
`<DataDir>/space.key`. That keypair is the **space**: a `did:key`
whose ms3t is the root UCAN authority over.

| Identity | Used for |
|---|---|
| ms3t's space signer | self-issuing `space/content/retrieve` delegations (read path, indexer claim publication, piri retrievals) |
| sprue's identity | piri allocate/accept invocations, audience of ms3t's self-issued retrieval delegations |

Sprue is the audience for those delegations because `uploader.Forge`
talks to piri *as sprue*. ms3t-as-space-root keeps zero-out-of-band
provisioning at the cost of a not-very-multi-tenant story; that's a
tradeoff to revisit if/when ms3t serves more than one customer.

## Testing surface

- **`pkg/ms3t/testing/harness.go`** — `StartHarness(ctx, opts...)`
  boots a real `*ms3t.Server` on a random `127.0.0.1` port with
  in-memory deps (`memStore` for Registry+Meta, `nopBaseReader` for
  the Layered base, `nopUploader` so flush is a no-op). Options:
  `WithLogger`, `WithRegion`, `WithCredentials`, `WithChunkSize`,
  `WithSealConfig`, `WithReadyTimeout`. Each call gets its own
  scratch tempdir; cleanup is registered against the test's `t`.
- **`pkg/ms3t/testing/integration.go`** — wraps versitygw's upstream
  `tests/integration` package. `Run(ctx, c, suite) Result` snapshots
  versitygw's package-level pass/fail counters before/after and
  returns the delta; `RunT(t, c, suite) Result` drives `Run` and
  reports failures via `t.Errorf`. Curated `Suite` constants:
  `Smoke`, `CRUD`, `Multipart`, `Tagging`, `ObjectLock`,
  `Versioning`, `Auth`, `Full`.
- **`pkg/ms3t/testing/smoke_test.go`** — one top-level `Test` per S3
  group (`TestSmoke_CreateBucket`, `TestSmoke_PutObject`, …) plus
  matching `TestSmokeXFail_*` for cases ms3t fails today. Each test
  is a table-driven Go test (so GoLand renders one play-icon per
  row). XFail tests treat per-case failures as `t.Skip` and only
  fail if a case unexpectedly passes — that's the cue to promote
  the row to the matching `TestSmoke_*`.

Today: **66 cases** pass via `TestSmoke_*`, **53 cases** are tracked
as known-failing via `TestSmokeXFail_*` (total 119, matching the
upstream Smoke set).

## Not implemented

- **Multipart upload.** Per project decision, multipart in-flight
  state will live in service-side storage, NOT folded into the MST.
  Out of scope today.
- **ACLs, bucket policy, object lock, versioning, tagging.** The
  always-called middleware methods (`GetBucketAcl`,
  `GetBucketPolicy`, `GetObjectLockConfiguration`,
  `GetBucketVersioning`) return polite empty / "not configured"
  responses so PUT/GET don't trip on `ErrNotImplemented`. The
  full surface is unimplemented.
- **Standard ETag.** S3 uses `md5(body)` hex for single-part PUTs
  (and a different format for multipart). ms3t currently returns
  `sha256(body)` hex. Adding md5 tracking to the `Body` record is
  the agreed fix; tracked under the `PutObject_success` smoke case.
- **User metadata round-trip** (`x-amz-meta-*`,
  `Content-Disposition`, etc.). `ObjectManifest` doesn't carry a
  user-metadata map yet. Tracked under `PutObject_with_metadata` /
  `HeadObject_success` smoke cases.
- **Range support on HeadObject.** GetObject honors `Range`;
  HeadObject doesn't.
- **Conditional reads/writes** (`If-Match`, `If-None-Match`).
- **Server-side checksum surface** (CRC64NVME, etc.). The body's
  sha256 is computed, but we don't surface checksum response
  headers in `x-amz-checksum-*` form, and we don't validate
  client-supplied checksums.
- **GC of unreferenced bodies.** `forge_root_cid` is a high-water
  mark — anything reachable from `root_cid` but not from
  `forge_root_cid` is "in flight" — but there's no expiry path to
  Forge yet, so storage grows monotonically.
- **Multi-tenancy.** ms3t is the space owner; one instance ↔ one
  space.
- **Multi-instance / HA.** The per-bucket lock is in-process. A
  multi-writer story would need cross-process coordination.

## Known TODOs in code

- `pkg/ms3t/blockstore/staging.go` — `OpStaging` buffers an entire
  S3 op's blocks in memory until Commit. For multi-GB PUTs this
  bounds peak memory at ≈ payload size. A file-backed alternative
  (CAR-shaped temp file + `cid → (offset, length)` index) would
  cap the per-tx footprint at one chunk + index. The interface is
  unchanged; only the storage backend would swap.
- `pkg/ms3t/registry/segments.go` — orphan `forge_root_cid` if
  `staging.Commit` succeeds but `reg.CASRoot` fails afterwards.
  Proposed fix: conditional `UPDATE … AND root_cid = $newRoot` in
  the per-op-root advance, so flush only advances `forge_root_cid`
  for buckets whose Root we actually recorded.

## Reading the code

If you're new and want to follow a request through:

- **PUT**: `s3frontend.Backend.PutObject` (object.go) →
  `bucketop.Coordinator.WithTx` (bucketop.go) →
  `bucket.FixedChunker.Chunk` (chunker.go) →
  `mst.MerkleSearchTree.Add` + `GetPointer` → `Tx.Commit` →
  `OpStaging.Commit` → `logstore.Store.AppendBatch` →
  `Segment.append` (fsyncs) → `registry.Postgres.CASRoot` → 200 OK.
- **GET**: `s3frontend.Backend.GetObject` (object.go) →
  `lookupManifest` (registry → MST.Get over Layered → manifest
  decode) → `FixedChunker.Open[Range]` over Layered → stream to
  client. Every miss past the open segment falls through to sealed
  segments and finally to `blockstore.Forge` (indexer + piri).
- **Flush**: `logstore.Store.flushLoop` → `cfg.Flush` (which is
  `newFlushFunc` from `server.go`) → builds `CARSource` from
  `Segment.{CARPath, Size, SHA256, BlockPositions}` →
  `uploader.Forge.SubmitCAR` (allocate + PUT + accept + index +
  claim) → `meta.MarkSegmentFlushed`.
- **Recovery**: `logstore.recovery.go` reconciles
  `<DataDir>/segments/` against `Meta.ListUnflushedSegments`.
- **Where ms3t plugs into sprue**: `pkg/ms3t/module.go::Module` is
  the only fx-aware file. `registerLifecycle` builds the
  production-only collaborators (Forge reader, Postgres registry,
  Forge uploader, space signer, migrations) and hands them to
  `New` from `server.go`.
- **The MST itself**: `pkg/ms3t/mst/`. Standalone fork of the
  atproto MST with relaxed key validation; no other ms3t deps.
