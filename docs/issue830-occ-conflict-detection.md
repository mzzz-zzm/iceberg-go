# Issue #830 — OCC Retry and Semantic Conflict Detection

## What the issue was about

Apache Iceberg tables support multiple concurrent writers. Since every writer must read
the current table metadata, add files, and then try to update the catalog's record of
the table in a single atomic step (called a "commit"), two writers can race against each
other. This pattern is called **Optimistic Concurrency Control (OCC)**:

1. Writer reads current metadata (snapshot A).
2. Writer builds its changes (writes data files, builds manifests).
3. Writer tells the catalog: "update the table to snapshot B, but only if it is still at
   snapshot A right now."
4. If another writer already advanced the table to snapshot A', the catalog rejects the
   request with an HTTP 409 Conflict.

The Java Iceberg SDK has always handled step 4 by automatically retrying: it reloads the
table, merges its changes on top of the new HEAD, and tries again. The Go SDK in v0.5.0
**did not have this retry loop at all**. Any 409 response was returned directly to the
caller as a fatal `ErrCommitConflict`. In a production environment with multiple writers
(e.g. multiple streaming ingest workers writing to the same table), this meant most
commits would fail rather than succeed.

Beyond simple retrying, the Java SDK also does **semantic conflict detection**: not every
conflict is safe to retry. For example, if Writer A is committing equality delete files
that cover partition `category='hot'`, and Writer B just appended new data rows in that
same partition while Writer A was building its commit, retrying Writer A on top of Writer
B's snapshot would silently leave those new rows undeleted. This is a data correctness
problem, not a CAS race. Java refuses to retry in this situation and returns a conflict
error instead.

---

## What was changed and why

### 1. New error sentinels (`table/table.go`)

Two distinct error values now exist, reflecting the two fundamentally different failure
modes.

**Before:** No sentinel errors at all; a 409 response was just returned verbatim.

**After:**

```go
// ADDED
var ErrCommitConflict = errors.New("commit conflict: table was modified concurrently")

// ADDED
var ErrConflictingWrite = errors.New("conflicting write: concurrent changes prevent a safe retry")
```

`ErrCommitConflict` (wrapping a catalog 409) is always retryable — a pure CAS race.
`ErrConflictingWrite` is not retryable — the data would be incorrect even after a
successful CAS.

---

### 2. New property constants (`table/properties.go`)

**Before:** No OCC-related property constants existed; there was no way to control retry
behaviour or isolation level from a table's properties.

**After:**

```go
// ADDED — retry budget (match Java defaults exactly)
CommitNumRetriesKey     = "commit.retry.num-retries"     // default 4
CommitMinRetryWaitMSKey = "commit.retry.min-wait-ms"     // default 100
CommitMaxRetryWaitMSKey = "commit.retry.max-wait-ms"     // default 60 000
CommitTotalRetryTimeMSKey = "commit.retry.total-timeout-ms" // default 1 800 000

// ADDED — isolation level for equality delete operations
WriteDeleteIsolationLevelKey     = "write.delete.isolation-level"
WriteDeleteIsolationLevelDefault = IsolationLevelSerializable  // "serializable"
IsolationLevelSerializable       = "serializable"
IsolationLevelSnapshot           = "snapshot"
```

The retry properties let each table independently tune its retry budget. The isolation
level matches Java's `IsolationLevel` enum and controls how aggressively conflict
detection blocks equality delete commits.

---

### 3. The OCC retry loop (`table/transaction.go`)

This is the largest single change. `Transaction.Commit()` went from a one-shot attempt to
a full retry loop with backoff, manifest rebuilding, and cleanup of orphaned files.

**Before:**

```go
func (t *Transaction) Commit(ctx context.Context) (*Table, error) {
    // ...
    if len(t.meta.updates) > 0 {
        t.reqs = append(t.reqs, AssertTableUUID(t.meta.uuid))
        tbl, err := t.tbl.doCommit(ctx, t.meta.updates, t.reqs)
        if err != nil {
            return tbl, err   // <-- any error (including 409) is fatal
        }
        // post-commit hooks...
        return tbl, err
    }
    return t.tbl, nil
}
```

**After (simplified):**

```go
func (t *Transaction) Commit(ctx context.Context) (*Table, error) {
    // ...
    if len(t.pendingProducers) > 0 {
        // Read retry knobs from table properties
        maxRetries  := props.GetInt(CommitNumRetriesKey, CommitNumRetriesDefault)
        minWaitMS   := props.GetInt(CommitMinRetryWaitMSKey, CommitMinRetryWaitMSDefault)
        maxWaitMS   := props.GetInt(CommitMaxRetryWaitMSKey, CommitMaxRetryWaitMSDefault)
        totalTimeMS := props.GetInt(CommitTotalRetryTimeMSKey, CommitTotalRetryTimeMSDefault)
        return t.commitWithRetry(ctx, maxRetries, minWaitMS, maxWaitMS, totalTimeMS)
    }
    // original single-attempt path for non-retryable operations
    ...
}
```

The new `commitWithRetry` method runs a loop up to `maxRetries` times:

```
attempt 0:  send the manifest-list files that were already written
            if OK  → done
            if 409 → mark manifest-list as orphan, continue

attempt 1:  sleep(exponential backoff with ±25% jitter)
            LoadTable → get fresh metadata
            rebase each pending producer (advance parent snapshot pointer)
            validateConflicts → abort immediately if data-correctness conflict
            sp.commit() → write new manifest-list pointing to rebased manifests
            send updated CAS request
            if OK  → done, delete orphaned manifest-lists
            if 409 → continue

... up to maxRetries
```

Orphaned manifest-list files (written during failed attempts) are collected and deleted
with best-effort cleanup on return, matching Java's behaviour.

**pendingProducers** is a new `[]*snapshotProducer` field on `Transaction`. Each
operation that can be retried adds its producer to this slice. The data files (Parquet)
are never re-written; only the cheap Avro manifest and manifest-list files are
regenerated on each retry.

Operations that now register a pending producer (and thus participate in the retry loop):

| Operation | Before | After |
|-----------|--------|-------|
| `Append` | no retry | added to `pendingProducers` |
| `AddDataFiles` | no retry | added to `pendingProducers` |
| `AddFiles` | no retry | added to `pendingProducers` |
| `ReplaceDataFiles` | no retry | added to `pendingProducers` |
| `Delete` (copy-on-write) | no retry | added to `pendingProducers` |
| `Overwrite` | no retry | added to `pendingProducers` |
| `RowDelta` | no retry, wrong producer | uses new `rowDeltaFiles` producer, added to `pendingProducers` |

---

### 4. The `validateConflicts` contract (`table/snapshot_producers.go`)

All producers now implement a `validateConflicts` method:

```go
// ADDED to producerImpl interface
validateConflicts(
    ctx           context.Context,
    originalBase  Metadata,   // metadata when the transaction opened
    freshMeta     Metadata,   // freshly loaded metadata after a 409
    isolationLevel string,    // "serializable" or "snapshot"
) error
```

Each producer type handles this differently:

**`fastAppendFiles` and `mergeAppendFiles`** — always return nil. Appending files never
conflicts with anything: the worst case is that two writers both appended, and the retry
loop correctly chains them.

```go
// ADDED
func (fa *fastAppendFiles) validateConflicts(_ context.Context, _ Metadata, _ Metadata, _ string) error {
    // FastAppend only adds files; it never removes any.
    return nil
}
```

**`overwriteFiles`** — has a new `deleteExpression` field. If `nil` (file-based
compaction), it is safe to retry because replace operations identify files by path, not
predicate. If non-nil (expression-based delete/overwrite like "delete all rows where
category='hot'"), it checks whether any concurrent snapshot added data files matching
that expression. If so, those rows would be silently missed by the overwrite, which is a
data correctness problem.

```go
// ADDED field
type overwriteFiles struct {
    base             *snapshotProducer
    deleteExpression iceberg.BooleanExpression  // nil for file-based replacements
}

// ADDED method
func (of *overwriteFiles) validateConflicts(ctx context.Context,
    originalBase Metadata, freshMeta Metadata, _ string) error {

    if of.deleteExpression == nil {
        return nil  // file-based: always safe
    }
    newFiles, _ := concurrentDataFiles(ctx, of.base.io, originalBase, freshMeta)
    eval, _     := newInclusiveMetricsEvaluator(schema, of.deleteExpression, ...)
    for _, f := range newFiles {
        if matches, _ := eval(f); matches {
            return fmt.Errorf("%w: concurrent append added files matching the overwrite filter",
                ErrConflictingWrite)
        }
    }
    return nil
}
```

**`rowDeltaFiles`** (new type) — handles equality delete conflict detection for `RowDelta`
operations (see next section).

---

### 5. `RowDelta` wired into the retry loop (`table/row_delta.go`)

`RowDelta` commits data files (inserts) and delete files (equality or positional deletes)
together. Before this change it used the same producer as a plain append, was never added
to `pendingProducers`, and explicitly carried a "not yet implemented" comment about
conflict detection.

**Before:**

```go
// Comment said: "conflict detection for concurrent writers is not yet implemented"

func (rd *RowDelta) Commit(ctx context.Context) error {
    // ...
    op := rd.Operation()
    producer := newFastAppendFilesProducer(op, rd.txn, wfs, nil, rd.props)
    // ...
    updates, reqs, err := producer.commit(ctx)
    // ...
    return rd.txn.apply(updates, reqs)  // no retry registration
}
```

**After:**

```go
func (rd *RowDelta) Commit(ctx context.Context) error {
    // ...
    op := rd.Operation()

    // Collect equality delete files for conflict detection
    eqDeleteFiles := make([]iceberg.DataFile, 0, len(rd.delFiles))
    for _, f := range rd.delFiles {
        if f.ContentType() == iceberg.EntryContentEqDeletes {
            eqDeleteFiles = append(eqDeleteFiles, f)
        }
    }
    producer := newRowDeltaFilesProducer(op, rd.txn, wfs, nil, rd.props, eqDeleteFiles)
    // ...
    updates, reqs, err := producer.commit(ctx)
    // ...
    rd.txn.pendingProducers = append(rd.txn.pendingProducers, producer)  // ADDED
    return rd.txn.apply(updates, reqs)
}
```

The new `rowDeltaFiles` producer:

```go
// ADDED
type rowDeltaFiles struct {
    *fastAppendFiles              // reuses all manifest-writing logic
    eqDeleteFiles []iceberg.DataFile  // only needed for conflict detection
}

func (rd *rowDeltaFiles) validateConflicts(ctx context.Context,
    originalBase Metadata, freshMeta Metadata, isolationLevel string) error {

    if len(rd.eqDeleteFiles) == 0 {
        return nil  // positional-only or data-only: always safe
    }
    if isolationLevel == IsolationLevelSnapshot {
        return nil  // application opted in to allowing missed deletes
    }
    // Serializable isolation: fail if any concurrent commit added data
    // in the same partitions our equality deletes cover.
    newFiles, _ := concurrentDataFiles(ctx, rd.base.io, originalBase, freshMeta)
    if partitionsOverlap(newFiles, rd.eqDeleteFiles) {
        return fmt.Errorf("%w: concurrent append added rows in partitions "+
            "covered by equality delete files", ErrConflictingWrite)
    }
    return nil
}
```

**Why this matters:** An equality delete file says "delete all rows where event_id is in
this set." If Writer B appended new rows with those same event IDs in partition
`category='hot'` after Writer A's transaction opened, and Writer A's equality delete does
not have an opportunity to cover those new rows, retrying Writer A's commit on top of
Writer B's snapshot would silently leave those rows in the table. The serializable check
prevents this.

The two isolation levels:

#### `serializable` (the default)

> "I need to be certain my deletes cover **all** matching rows — including any that were just added by someone else."

If a concurrent writer appended rows in the same partition your equality delete covers,
the commit is **rejected** with `ErrConflictingWrite`. You must handle the error and
decide what to do (for example, re-read the data and recompute your deletes).

**When to use it:** Anywhere correctness matters — CDC pipelines, data quality jobs,
anything where a row surviving a delete would be a bug.

#### `snapshot`

> "My deletes only need to cover the rows that existed when I started. Rows added after that are someone else's problem."

A concurrent append in the same partition is **ignored**. Your commit retries
successfully, and the newly appended rows are simply not covered by your delete file.
They remain in the table.

**When to use it:** Bulk backfill deletes or analytics workloads where "best effort" is
acceptable and a strict guarantee is not required. You are consciously trading correctness
for throughput.

#### Concrete example

- Partition `category='hot'` has 100 rows with `event_id` in `{1, 2, 3}`.
- Writer A builds an equality delete: "delete all rows where `event_id` is in `{1, 2, 3}`."
- While Writer A is working, Writer B appends 50 new rows — some also have `event_id=2`.

| Level | Result |
|-------|--------|
| `serializable` | Writer A's commit is rejected. The new rows with `event_id=2` will not slip through. |
| `snapshot` | Writer A's commit succeeds. The new rows with `event_id=2` are **not deleted** and remain in the table silently. |

---

### 6. Conflict detection helpers (`table/snapshot_producers.go`)

Three helper functions were added to support the validation logic above.

**`concurrentDataFiles`** — given the metadata at transaction open time and the fresh
metadata from after a 409, it walks the snapshot chain backwards from the new HEAD until
it reaches the transaction's base, collecting all data files that other writers added in
between.

```go
// ADDED
func concurrentDataFiles(ctx context.Context, io iceio.IO,
    originalBase, freshMeta Metadata) ([]iceberg.DataFile, error) {

    baseID := int64(-1)
    if s := originalBase.CurrentSnapshot(); s != nil {
        baseID = s.SnapshotID
    }
    // Walk HEAD → base, collecting intervening snapshots
    var concurrentSnaps []*Snapshot
    for s := freshMeta.CurrentSnapshot(); s != nil && s.SnapshotID != baseID; {
        concurrentSnaps = append(concurrentSnaps, s)
        if s.ParentSnapshotID == nil { break }
        s = freshMeta.SnapshotByID(*s.ParentSnapshotID)
    }
    // For each intervening snapshot, collect data files
    var result []iceberg.DataFile
    for _, snap := range concurrentSnaps {
        files, _ := dataFilesAddedBySnapshot(snap, io)
        result = append(result, files...)
    }
    return result, nil
}
```

**`dataFilesAddedBySnapshot`** — reads only the manifests that a specific snapshot
itself wrote (identified by `manifest.SnapshotID() == snap.SnapshotID`), so it does not
re-scan inherited manifests from earlier snapshots. It returns only data files (not
delete files) with status `ADDED`.

**`partitionsOverlap`** — compares the partition values of two groups of data files using
`reflect.DeepEqual` on the `Partition() map[int]any` values that each `iceberg.DataFile`
exposes. Returns true if any pair has the same partition, meaning the concurrent append
landed in a partition covered by a pending equality delete.

---

### 7. `snapshotProducer` now tracks attempt count and manifest-list path

Two small fields were added to `snapshotProducer` to support the retry loop:

```go
type snapshotProducer struct {
    // ADDED
    attempt              int    // incremented by rebase(); makes each retry's
                                // manifest-list filename unique
    // ADDED
    lastManifestListPath string // path of the manifest-list written by commit();
                                // used by commitWithRetry to track orphans
    // ...existing fields...
}
```

The `commit()` method was updated to use `sp.attempt` (instead of the hard-coded `0`)
when naming the manifest-list file:

```go
// Before
fname := newManifestListFileName(sp.snapshotID, 0, sp.commitUuid)

// After
fname := newManifestListFileName(sp.snapshotID, sp.attempt, sp.commitUuid)
```

This reproduces Java's naming pattern: `snap-<snapshotID>-<attempt>-<uuid>.avro`.

---

### 8. Two pre-existing data races fixed

While running tests with `-race`, two concurrent-access bugs in the upstream code were
found and fixed.

**`schema.go` — `MarshalJSON` mutated a shared field while two goroutines marshaled
manifests in parallel:**

```go
// Before — writes to s.IdentifierFieldIDs while another goroutine may be reading it
func (s *Schema) MarshalJSON() ([]byte, error) {
    if s.IdentifierFieldIDs == nil {
        s.IdentifierFieldIDs = []int{}  // data race: mutates shared field
    }
    type Alias Schema
    return json.Marshal(struct{ ...; *Alias }{..., Alias: (*Alias)(s)})
}

// After — uses a local copy so the shared *Schema is never written to
func (s *Schema) MarshalJSON() ([]byte, error) {
    ids := s.IdentifierFieldIDs
    if ids == nil {
        ids = []int{}   // local variable only
    }
    type Alias Schema
    aliasCopy := *(*Alias)(s)           // copy the struct by value
    aliasCopy.IdentifierFieldIDs = ids  // update only the copy
    return json.Marshal(struct{ ...; *Alias }{..., Alias: &aliasCopy})
}
```

**`visitors.go` — `ExpressionEvaluator` returned a bound method on a shared struct,
causing a race when the scanner called it from multiple goroutines:**

```go
// Before — single exprEvaluator shared by all callers of the returned function
func ExpressionEvaluator(s *Schema, unbound BooleanExpression,
    caseSensitive bool) (func(StructLike) (bool, error), error) {
    bound, _ := BindExpr(s, unbound, caseSensitive)
    return (&exprEvaluator{bound: bound}).Eval, nil  // data race: e.st is shared
}

// After — creates a fresh evaluator for every call, so it is goroutine-safe
func ExpressionEvaluator(s *Schema, unbound BooleanExpression,
    caseSensitive bool) (func(StructLike) (bool, error), error) {
    bound, _ := BindExpr(s, unbound, caseSensitive)
    return func(st StructLike) (bool, error) {
        return VisitExpr(bound, &exprEvaluator{bound: bound, st: st})
    }, nil
}
```

---

## What the tests verify

Six new tests in `table/occ_issue830_regression_test.go` exercise the conflict detection
logic directly, on top of the seven OCC retry tests that were already written:

| Test | What it checks |
|------|----------------|
| `TestIssue830_IsolationLevelPropertyConstants` | Property key strings and defaults match Java's enum values |
| `TestIssue830_ConflictValidation_RowDelta_EqDeleteSerializable` | Equality delete + concurrent append in same partition → `ErrConflictingWrite` |
| `TestIssue830_ConflictValidation_RowDelta_EqDeleteSnapshotIsolation` | Same scenario with `isolation-level=snapshot` → commit succeeds |
| `TestIssue830_ConflictValidation_RowDelta_NoConflictDifferentPartition` | Concurrent append in a *different* partition → no conflict, commit succeeds |
| `TestIssue830_ConflictValidation_RowDelta_PosDeleteNoConflict` | Positional-only `RowDelta` (no equality deletes) → never conflicts |
| `TestIssue830_ConflictValidation_Overwrite_ExpressionConflict` | Delete expressions (AlwaysTrue) + concurrent append → `ErrConflictingWrite` |

All 17 packages compile and their tests pass under `go test ./... -race -count=1`.

---

## Summary of files changed

| File | Change |
|------|--------|
| `table/table.go` | Added `ErrCommitConflict` and `ErrConflictingWrite` sentinels |
| `table/properties.go` | Added `CommitRetry*` constants and `WriteDeleteIsolationLevel*` constants |
| `table/transaction.go` | Added `pendingProducers` field, OCC retry loop `commitWithRetry`, registered all retryable operations, set `deleteExpression` on overwrite producers |
| `table/snapshot_producers.go` | Added `validateConflicts` to `producerImpl` interface, implemented it for all producers, added `rowDeltaFiles` type, added `concurrentDataFiles` / `dataFilesAddedBySnapshot` / `partitionsOverlap` helpers, added `attempt` + `lastManifestListPath` fields and `rebase()` method to `snapshotProducer` |
| `table/row_delta.go` | Switched to `newRowDeltaFilesProducer`, added to `pendingProducers`, updated doc comment |
| `schema.go` | Fixed data race in `MarshalJSON` |
| `visitors.go` | Fixed data race in `ExpressionEvaluator` |
| `table/occ_issue830_regression_test.go` | Added 6 conflict-detection tests (13 total in file) |
