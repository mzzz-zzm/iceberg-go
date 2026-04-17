# Transaction Workflow: How a Write Gets from Your Code to S3

This document traces what happens when you call `txn.Append()` followed by
`txn.Commit()` in plain English, with every file that gets written named
explicitly so you can verify it in S3.

The running example is two workers appending to the same table at the same time.

---

## The cast of file types

Before diving in, it helps to know the three layers of files Iceberg writes for
every snapshot:

| File type | Format | What it contains |
|---|---|---|
| **Data file** | Parquet | Your actual rows |
| **Manifest** | Avro | A list of data files, plus column statistics for each one |
| **Manifest list** (snapshot file) | Avro | A list of manifests — one per snapshot |

The hierarchy is: **manifest list → manifests → data files**.

---

## Phase 0 — Load the table

> **Code location:** `CatalogIO.LoadTable()` interface — `table/table.go:63`

```go
tbl, _ := cat.LoadTable(ctx, []string{"warehouse", "events"})
// tbl.CurrentSnapshot().SnapshotID == 100
```

The catalog returns a `*Table`. It holds the current metadata in memory and a
pointer to the catalog (`tbl.cat`) for later use. The table is at **snapshot 100**.

---

## Phase 1 — Create a Transaction

> **Code location:** `Table.NewTransaction()` — `table/table.go:108`; `Transaction` struct (fields: `pendingProducers`, `mx`, `committed`) — `table/transaction.go:70`; `Transaction.apply()` — `table/transaction.go:85`

```go
txn := tbl.NewTransaction()
```

**What happens in code** (`table/table.go` → `NewTransaction()`):

- A `MetadataBuilder` is created from the current table metadata. It is a
  mutable working copy — a scratch pad. Every time you call `apply()` something
  is recorded here.
- `txn.tbl` keeps a pointer to the original `*Table` — needed later to call
  `cat.LoadTable` and `cat.CommitTable`.
- `txn.pendingProducers` starts as an empty slice.
- `txn.committed` is `false`.

Think of the transaction as a **draft document**. The live table is the
published version. Nothing you do to the draft touches the published version
until `Commit()` succeeds.

---

## Phase 2 — Add data (`Append`)

```go
err := txn.Append(ctx, recordReader, nil)
```

Four things happen in sequence. None of them touch the catalog.

### Step 2a — Write the Parquet data file to S3

> **Code location:** `Transaction.Append()` — `table/transaction.go:327`; `recordsToDataFiles()` — `table/arrow_utils.go:1455`

```go
itr := recordsToDataFiles(ctx, tbl.Location(), t.meta, recordWritingArgs{...})
for df, err := range itr {
    appendFiles.appendDataFile(df)
}
```

`recordsToDataFiles` reads from the `RecordReader` and writes one or more
`.parquet` files into the table's data directory on S3. Each Parquet file
gets a `DataFile` descriptor back in memory (path, size, row count, column
statistics). The catalog is not involved.

```
S3 after this step:
  s3://my-bucket/warehouse/events/data/
      00000-abc-data.parquet        ← your rows, written permanently
```

This write is **permanent and never repeated**. If the commit later fails
completely, the file sits in S3 as an orphan and is cleaned up by a periodic
maintenance job. It is never referenced by any snapshot, so readers ignore it.

### Step 2b — Write the Manifest Avro file to S3

> **Code location:** `snapshotProducer.commit()` — `table/snapshot_producers.go:824`; `snapshotProducer.manifests()` — `table/snapshot_producers.go:621`; `snapshotProducer.manifestProducer()` — `table/snapshot_producers.go:721`

Inside `appendFiles.commit(ctx)` → `sp.manifests(ctx)` → `sp.manifestProducer()`:

A **manifest** is an Avro file that catalogs the data file written above. It
includes the data file's path plus statistics (min/max values, null counts)
so query engines can skip it when filtering.

```
S3 after this step:
  s3://my-bucket/warehouse/events/metadata/
      abc123-m1.avro                ← manifest listing 00000-abc-data.parquet
```

This is called **eager writing** — the manifest is written during `Append()`,
not during `Commit()`. This is intentional: it means `Commit()` can retry
without re-reading your Parquet files.

### Step 2c — Write the Manifest List Avro file to S3

> **Code location:** `newManifestListFileName()` — `table/snapshot_producers.go:70`; `sp.lastManifestListPath` set inside `snapshotProducer.commit()` — `table/snapshot_producers.go:824`

Still inside `appendFiles.commit(ctx)`:

```go
fname := newManifestListFileName(sp.snapshotID, sp.attempt, sp.commitUuid)
// produces: "snap-200-0-abc123.avro"
//                 ^^^  ^ ^^^^^^
//             snapshot  attempt  uuid
sp.lastManifestListPath = manifestListFilePath
```

A **manifest list** (also called a snapshot file) is an Avro file that lists
all manifests belonging to one snapshot. It is the root of the file hierarchy
for that snapshot.

```
S3 after this step:
  s3://my-bucket/warehouse/events/metadata/
      snap-200-0-abc123.avro        ← manifest list for snapshot 200
        └── abc123-m1.avro          (points to this manifest)
              └── 00000-abc-data.parquet
```

### Step 2d — Register with the transaction, do NOT touch the catalog yet

> **Code location:** `t.pendingProducers = append(...)` — `table/transaction.go:355`; `t.apply()` — `table/transaction.go:85`

```go
t.pendingProducers = append(t.pendingProducers, appendFiles)
return t.apply(updates, reqs)
```

`t.apply()` validates the requirements against the draft metadata (e.g. "is
the table still at snapshot 100?") and stores the update instructions in
`t.meta.updates`. The catalog still says the table is at snapshot 100.

**State at the end of `Append()`:**

- Three files are in S3 (Parquet + manifest + manifest list).
- The catalog still says the table is at snapshot 100.
- The transaction has `appendFiles` in `pendingProducers`.

---

## Phase 3 — `Commit()` is called

> **Code location:** `Transaction.Commit()` — `table/transaction.go:1650`

Both Worker A and Worker B call `txn.Commit(ctx)` at roughly the same time.

### The mutex

> **Code location:** `table/transaction.go:1651`

```go
t.mx.Lock()
defer t.mx.Unlock()
```

This protects against two goroutines calling `Commit` on the **same**
`Transaction` object. It does not protect against two separate transactions
on the same table — that is handled by the catalog's compare-and-swap.

### Route decision

> **Code location:** `table/transaction.go:1666`

```go
if len(t.pendingProducers) > 0 {
    return t.commitWithRetry(...)   // OCC retry path
}
// original single-attempt path (SetProperties, UpdateSchema, etc.)
```

Because `Append` added a producer to `pendingProducers`, both workers go into
`commitWithRetry`.

---

## Phase 4 — The OCC retry loop

> **Code location:** `Transaction.commitWithRetry()` — `table/transaction.go:1705`; retry property constants — `table/properties.go:97`; orphaned manifest-list cleanup (deferred) — `table/transaction.go:1715`

```go
func (t *Transaction) commitWithRetry(ctx context.Context,
    maxRetries, minWaitMS, maxWaitMS, totalTimeMS int) (*Table, error)
```

Default values for a table with no custom retry properties:

| Property | Default | Meaning |
|---|---|---|
| `commit.retry.num-retries` | 4 | Up to 5 total attempts (1 original + 4 retries) |
| `commit.retry.min-wait-ms` | 100 | Start backoff at 100 ms |
| `commit.retry.max-wait-ms` | 60 000 | Cap backoff at 60 s |
| `commit.retry.total-timeout-ms` | 1 800 000 | Wall-clock deadline of 30 minutes |

### Attempt 0 — the first try

Both workers send a PUT request to the REST catalog:

```
PUT /v1/namespaces/warehouse/tables/events/commits

requirements:
  - type: "assert-ref-snapshot-id"
    ref: "main"
    snapshot-id: 100          ← "only commit if the table is still at snapshot 100"
  - type: "assert-table-uuid"
    uuid: "..."

updates:
  - action: "add-snapshot"     snapshot: {id: 200, manifest-list: "snap-200-0-abc123.avro", ...}
  - action: "set-snapshot-ref" ref: "main", snapshot-id: 200
```

**Worker A arrives first.** The catalog checks: "is `main` still at 100?
Yes." It advances the table to snapshot 200. Worker A succeeds and returns.

**Worker B arrives next.** The catalog checks: "is `main` still at 100?
No — it is now at 200." It returns **HTTP 409**. Worker B gets
`rest.ErrCommitFailed`, which wraps `table.ErrCommitConflict`.

```go
if !errors.Is(err, ErrCommitConflict) {
    return nil, err   // 5xx or auth error: stop immediately
}
// 409: retryable — continue to attempt 1
```

Worker B records its manifest list as a potential orphan:

> **Code location:** `ErrCommitConflict` definition — `table/table.go:51`; retryable-error check — `table/transaction.go:1851`

```go
orphanedManifestLists = ["s3://.../snap-200-0-def456.avro"]
```

### Attempt 1 — Worker B retries

**1. Sleep (exponential backoff with ±25% jitter)**

> **Code location:** `occBackoff()` — `table/transaction.go:1866`; sleep call — `table/transaction.go:1776`

```go
sleepDuration := occBackoff(0, 100, 60000)
// = min(60000, 100 × 2⁰) × rand(0.75, 1.25) ≈ 75–125 ms
```

**2. Reload the table**

> **Code location:** `table/transaction.go:1784`

```go
freshTbl, _ := t.tbl.cat.LoadTable(ctx, t.tbl.identifier)
// freshTbl.CurrentSnapshot().SnapshotID == 200  (Worker A's data is now visible)
```

**3. Rebase the producer** (`sp.rebase(freshMeta)`)

> **Code location:** call — `table/transaction.go:1789`; `snapshotProducer.rebase()` impl — `table/snapshot_producers.go:911`

```go
sp.attempt++               // 0 → 1, so next manifest list gets a new filename
sp.parentSnapshotID = 200  // was 100, now points to Worker A's snapshot
// Merge snapshot 200 into local snapshot list so existingManifests() can find it
// Update currentSnapshotID so next AssertRefSnapshotID says 200, not 100
```

**4. Validate conflicts**

> **Code location:** call — `table/transaction.go:1797`; `fastAppendFiles.validateConflicts()` — `table/snapshot_producers.go:119`

```go
isolationLevel := "serializable"  // table default
sp.validateConflicts(ctx, originalBase, freshMeta, isolationLevel)
```

Worker B used `fastAppendFiles`. Its `validateConflicts` always returns nil:

```go
func (fa *fastAppendFiles) validateConflicts(...) error {
    return nil   // appending files never conflicts with other appends
}
```

**5. Regenerate the manifest list** (`sp.commit(ctx)`)

> **Code location:** call — `table/transaction.go:1812`; `snapshotProducer.commit()` impl — `table/snapshot_producers.go:824`

`existingManifests()` now reads from snapshot 200 (Worker A's snapshot) and
returns Worker A's manifests as existing ones. Worker B's own manifest
(`def456-m1.avro`) is still in `sp.addedFiles` and is listed as added.
The manifest list filename uses `attempt=1`:

```
S3 after this step:
  s3://my-bucket/warehouse/events/metadata/
      snap-201-1-def456.avro        ← new manifest list, attempt 1
        ├── abc123-m1.avro          (Worker A's manifest, now "existing")
        └── def456-m1.avro          (Worker B's own manifest, "added")
```

**6. Send the updated CAS request**

```
PUT /v1/namespaces/warehouse/tables/events/commits

requirements:
  - type: "assert-ref-snapshot-id"  ref: "main"  snapshot-id: 200  ← updated
  - type: "assert-table-uuid"  uuid: "..."

updates:
  - action: "add-snapshot"     snapshot: {id: 201, manifest-list: "snap-201-1-def456.avro", ...}
  - action: "set-snapshot-ref" ref: "main"  snapshot-id: 201
```

The catalog checks: "is `main` at 200? Yes." It advances to snapshot 201.
Worker B succeeds.

**7. Clean up the orphaned manifest list from attempt 0**

> **Code location:** deferred cleanup — `table/transaction.go:1718`

```go
// defer runs on return and deletes:
fs.Remove("s3://.../snap-200-0-def456.avro")
// ^ was written during attempt 0 but never committed — now deleted
```

---

## Final state in S3

```
Snapshot 100 (original)
└── (pre-existing manifests and data files)

Snapshot 200 (Worker A — succeeded on attempt 0)
└── snap-200-0-abc123.avro
      └── abc123-m1.avro
            └── worker-a-data.parquet

Snapshot 201 (Worker B — succeeded on attempt 1)
└── snap-201-1-def456.avro
      ├── abc123-m1.avro          (inherited from snapshot 200)
      └── def456-m1.avro
            └── worker-b-data.parquet

Deleted by deferred cleanup:
    snap-200-0-def456.avro        (Worker B's orphaned attempt-0 manifest list)
```

Both workers' data is in the table. No rows were lost.

---

## What changes for Delete / Overwrite

If Worker B was doing a **Delete** (`txn.Delete(filter, ...)`) instead of an
Append, the `validateConflicts` step does actual work. The producer is
`overwriteFiles` with `deleteExpression` set to the filter.

> **Code location:** `overwriteFiles.validateConflicts()` — `table/snapshot_producers.go:291`; `ErrConflictingWrite` definition — `table/table.go:56`

```go
func (of *overwriteFiles) validateConflicts(ctx, originalBase, freshMeta, _) error {
    if of.deleteExpression == nil {
        return nil   // file-based compaction: always safe
    }
    // Expression-based delete: did Worker A add any rows matching our filter?
    newFiles, _ := concurrentDataFiles(ctx, io, originalBase, freshMeta)
    for _, f := range newFiles {
        matches, _ := eval(f)   // does this file match "status='cancelled'"?
        if matches {
            return fmt.Errorf("%w: ...", ErrConflictingWrite)
            // Worker A just added new cancelled rows our delete would miss — abort
        }
    }
    return nil  // Worker A's files don't overlap — safe to retry
}
```

`ErrConflictingWrite` is **not** `ErrCommitConflict`. The retry loop check:

```go
if !errors.Is(err, ErrCommitConflict) {
    return nil, err   // ErrConflictingWrite hits here — no more retries
}
```

The caller must reload the table, re-scan, recompute the delete, and start
a fresh transaction.

### The two isolation levels

| Level | What it means | Result when a concurrent append lands in the same partition |
|---|---|---|
| `serializable` (default) | "My delete must cover every matching row, including ones added after I started." | Commit **rejected** with `ErrConflictingWrite` |
| `snapshot` | "My delete only needs to cover rows that existed when I opened the transaction." | Commit **retries and succeeds** — new rows are simply not deleted |

Set it per table:

> **Code location:** `WriteDeleteIsolationLevelKey` — `table/properties.go:82`; `Transaction.SetProperties()` — `table/transaction.go:155`

```go
txn.SetProperties(iceberg.Properties{
    table.WriteDeleteIsolationLevelKey: table.IsolationLevelSnapshot,
})
```

---

## Complete data journey (one-page summary)

```
txn.Append()
  ├─ [WRITE PARQUET]      → S3  ...data/worker-a.parquet
  ├─ [WRITE MANIFEST]     → S3  ...metadata/abc123-m1.avro
  ├─ [WRITE MFST LIST]    → S3  ...metadata/snap-200-0-abc123.avro
  └─ [REGISTER PRODUCER]  → txn.pendingProducers

txn.Commit()
  └─ commitWithRetry()
       │
       attempt 0
       ├─ [CAS PUT] → catalog  "advance to snap 200, if still at 100"
       │  Worker A: HTTP 200 ✓ → done, cleanup orphan list (empty), return
       │  Worker B: HTTP 409 ✗ → add snap-200-0-def456.avro to orphan list
       │
       attempt 1  (Worker B only)
       ├─ [SLEEP]           ~100 ms
       ├─ [RELOAD]          → catalog  (table is now at snap 200)
       ├─ [REBASE]          parentSnapshot = 200, attempt = 1
       ├─ [VALIDATE]        fastAppend: always nil
       ├─ [WRITE MFST LIST] → S3  snap-201-1-def456.avro
       ├─ [CAS PUT]         → catalog  "advance to snap 201, if still at 200"
       │  HTTP 200 ✓ → done
       └─ [CLEANUP]         delete snap-200-0-def456.avro  (orphan from attempt 0)
```
