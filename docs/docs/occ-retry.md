# OCC Retry for Concurrent Appends

This page explains the optimistic concurrency control (OCC) retry feature added to
`iceberg-go` to address [issue #830](https://github.com/apache/iceberg-go/issues/830).
It describes what used to happen, what happens now, what you need to know when using
the API, and how to run the integration tests against real AWS services.

---

## What is OCC and why does it matter?

When two processes append to the same Iceberg table at the same time, one of them
will win the race to update the catalog metadata and the other will receive an
HTTP 409 (Conflict) response. This is expected behaviour — the catalog is doing
a compare-and-swap to keep the table consistent.

The way a library handles that conflict determines whether the losing writer's work
is simply thrown away or automatically retried. Before this change, `iceberg-go`
threw the error away immediately. After this change, `iceberg-go` behaves the same
as the Java SDK: it reloads the table, reattaches the already-written data files to
the new table HEAD, and tries the catalog commit again.

---

## Before: what used to happen

```
Writer A                              Writer B
   │                                     │
   ├─► write Parquet data files (S3)     ├─► write Parquet data files (S3)
   ├─► write manifest Avro files (S3)    ├─► write manifest Avro files (S3)
   ├─► write manifest list file (S3)     ├─► write manifest list file (S3)
   │                                     │
   ├─► PUT /tables/.../commits  ◄── wins the race
   │        HTTP 200 ✓                   │
   │                                     ├─► PUT /tables/.../commits
   │                                     │        HTTP 409 ✗
   │                                     │
   │                              error bubbles up to caller
   │                              orphaned files remain in S3 forever
```

What this meant in practice:

- The losing writer returned `rest.ErrCommitFailed` immediately. There was no retry.
- The Parquet files, manifest files, and manifest list written by the losing writer
  became orphans in S3. They were never referenced and could only be removed by a
  separate cleanup job.
- Applications had to implement retry logic themselves (and most did not, so data was
  silently dropped or the whole operation failed).
- The `commit.retry.*` table properties were documented in `metadata.go` but were
  never actually read at runtime.

---

## After: what happens now

```
Writer A                              Writer B
   │                                     │
   ├─► write Parquet data files (S3)     ├─► write Parquet data files (S3)
   │   [done once, never repeated]       │   [done once, never repeated]
   │                                     │
   ├─► write manifest + manifest list    ├─► write manifest + manifest list
   │                                     │
   ├─► PUT /tables/.../commits  ◄── wins the race
   │        HTTP 200 ✓                   │
   │                                     ├─► PUT /tables/.../commits
   │                                     │        HTTP 409 ✗
   │                                     │
   │                              ┌──────┴────────────────────┐
   │                              │  OCC retry loop begins    │
   │                              │                           │
   │                              │  1. sleep (backoff)       │
   │                              │  2. reload table from cat │
   │                              │  3. point producer at new │
   │                              │     HEAD snapshot         │
   │                              │  4. rewrite manifest +    │
   │                              │     manifest list only    │
   │                              │  5. PUT /tables/...       │
   │                              │        HTTP 200 ✓         │
   │                              │                           │
   │                              │  clean up orphaned        │
   │                              │  manifest lists from      │
   │                              │  failed attempts          │
   │                              └───────────────────────────┘
   │
ALL data ends up in the table. Both writers succeed.
```

Key points of the new design:

1. **Parquet data files are written exactly once.** The retry only re-writes the
   cheap Avro metadata files (manifest and manifest list). This matches Java's
   behaviour exactly.
2. **Only HTTP 409 triggers a retry.** A 5xx error (server crash, network failure)
   propagates immediately just like before — the retry is not a general network-error
   handler.
3. **Retry limits are read from table properties.** The four `commit.retry.*`
   properties (described below) control how many retries are allowed and how long
   to wait between them.
4. **Orphaned manifest lists are cleaned up automatically.** Any manifest list file
   written during a failed attempt is deleted after the transaction finishes
   (whether it eventually succeeded or not).
5. **The public API has not changed.** Callers continue to call
   `txn.Append()` / `txn.AddDataFiles()` / `txn.Commit()` exactly as before.

---

## Retry properties

Set these as Iceberg table properties when you create or alter the table.
They apply per-transaction and are read fresh at the start of each `Commit()` call.

| Property key | Default | What it controls |
|---|---|---|
| `commit.retry.num-retries` | `4` | Maximum number of additional attempts after the first. A value of 4 means up to 5 total attempts. |
| `commit.retry.min-wait-ms` | `100` | Minimum sleep duration in milliseconds before the first retry. |
| `commit.retry.max-wait-ms` | `60000` | Maximum sleep duration in milliseconds. The backoff never exceeds this cap. |
| `commit.retry.total-timeout-ms` | `1800000` | Hard wall-clock limit (30 minutes by default). If the deadline passes, the commit fails with a timeout error even if retries remain. |

The backoff between attempts follows this formula (matching Java exactly):

```
delay = min(max-wait-ms, min-wait-ms × 2^attempt) × random(0.75 … 1.25)
```

- Attempt 0 → 1: `100 × 1 × jitter` ≈ 75–125 ms
- Attempt 1 → 2: `100 × 2 × jitter` ≈ 150–250 ms
- Attempt 2 → 3: `100 × 4 × jitter` ≈ 300–500 ms
- …and so on, doubling each time until capped at `max-wait-ms`.

---

## Which operations are retried

| Transaction method | Retried? | Why |
|---|---|---|
| `Append()` | Yes | FastAppend — appending new data never conflicts with other appends |
| `AppendTable()` | Yes | Same as `Append()` |
| `AddDataFiles()` | Yes | Same as `Append()` — data is pre-built by the caller |
| `AddFiles()` | Yes | Same as `Append()` |
| `ReplaceDataFiles()` | No | Overwrite — may conflict with concurrent deletes |
| `ReplaceDataFilesWithDataFiles()` | No | Overwrite — same reason |
| `ReplaceFiles()` | No | Overwrite — same reason |
| `SetProperties()` | No | Not a snapshot operation |
| `UpdateSchema()` / `UpdateSpec()` | No | Structural changes, not snapshot operations |
| `ExpireSnapshots()` | No | Asserts specific snapshot refs; not an append |

This mirrors Java's rule: only `FastAppend` and `MergeAppend` are free of conflict
validation, so only those operations enter the retry loop.

---

## How to detect a conflict error in your code

If you need to distinguish a commit conflict from other errors you can use
`errors.Is()`:

```go
import (
    "errors"
    "github.com/apache/iceberg-go/table"
)

_, err := txn.Commit(ctx)
if errors.Is(err, table.ErrCommitConflict) {
    // the retry loop exhausted all attempts — all retries saw a 409
    log.Println("all retry attempts were rejected by the catalog")
} else if err != nil {
    // a different error (network failure, auth error, etc.)
    log.Printf("commit failed for a non-conflict reason: %v", err)
}
```

`table.ErrCommitConflict` is also wrapped inside `rest.ErrCommitFailed`, so existing
code that checks `errors.Is(err, rest.ErrCommitFailed)` continues to work without
any changes.

---

## Important notes

**1. The transaction is still single-use.**
Calling `Commit()` a second time on the same `Transaction` returns an error. If the
retry loop exhausts all attempts and returns `ErrCommitConflict`, you should create a
fresh transaction, reload the table, and retry the whole application-level operation.

**2. Context cancellation is respected.**
If the `context.Context` passed to `Commit()` is cancelled mid-retry, the error
returned will be `context.Canceled` (or `context.DeadlineExceeded`). The deferred
cleanup of orphaned files still runs.

**3. Non-append operations are not affected.**
`ReplaceDataFiles`, `ReplaceFiles`, etc. use the original single-attempt path. A 409
on those operations still surfaces immediately as `rest.ErrCommitFailed`.

**4. Property overrides at the table level.**
All four retry properties are read from the table's stored properties at the time
`Commit()` is called. You can tighten or loosen retry behaviour per-table without
changing any application code.

**5. Orphan cleanup is best-effort.**
If the deferred cleanup cannot reach the file system (e.g. a network partition occurs
right at the end of the transaction), orphaned manifest list files are left in place.
They are harmless — no snapshot references them — but a periodic metadata cleanup job
will remove them eventually.

**6. `StagedTable()` continues to work.**
The manifests and manifest list for the first attempt are written eagerly inside
`Append()`, so `StagedTable()` can be used to inspect what would be committed before
calling `Commit()`. If `Commit()` later enters the retry loop, fresh manifests are
written on each subsequent attempt without touching the staged data.

---

## Testing against real AWS services

### Prerequisites

| What | Details |
|---|---|
| AWS credentials | IAM user or role with `s3tables:*` and `s3:*` permissions on your table bucket |
| S3 Tables bucket | A bucket configured as an S3 Tables bucket in your region |
| REST catalog endpoint | S3 Tables exposes an Iceberg REST endpoint — format is `https://s3tables.<region>.amazonaws.com/iceberg` |
| Go toolchain | 1.21+ |

### Environment variables

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<your access key>
export AWS_SECRET_ACCESS_KEY=<your secret key>

# S3 Tables REST catalog endpoint
export ICEBERG_S3TABLES_CATALOG=https://s3tables.us-east-1.amazonaws.com/iceberg

# Your S3 Tables warehouse ARN (the S3 Tables bucket ARN)
export ICEBERG_S3TABLES_WAREHOUSE=arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket
```

### Run the OCC retry integration tests

```bash
cd iceberg-go
go test ./table/... \
  -tags integration_aws \
  -run TestOCCRetryAWS \
  -v \
  -timeout 5m
```

This runs four tests:

| Test | What it checks |
|---|---|
| `TestSingleWriterOCCRetry` | A single writer commits an append. Verifies basic end-to-end flow with explicit retry settings. |
| `TestConcurrentWritersOCCRetry` | Two goroutines append to the same table simultaneously. Both should succeed and the final table should contain both rows. |
| `TestRetryTimeoutRespectsProperty` | Sets `total-timeout-ms=500` and verifies the property is honoured (commit either succeeds quickly or fails within the timeout window). |
| `TestErrCommitConflictFromRestCatalog` | Verifies that `rest.ErrCommitFailed` wraps `table.ErrCommitConflict` — confirms the error chain is correct without relying on a network call. |

### Verify with Athena

After running `TestConcurrentWritersOCCRetry`, both rows should be queryable through Amazon Athena. To check:

1. Open the Athena console in your AWS region.
2. Run a `SHOW DATABASES` or point Athena at the Glue Data Catalog / Lake Formation
   database that is backed by your S3 Tables bucket.
3. Query the table that the test created:

```sql
-- Replace occ_retry_test and concurrent_occ with the actual namespace and table name
SELECT * FROM occ_retry_test.concurrent_occ;
```

You should see exactly 2 rows (one from each concurrent writer). If you see only 1
row, one commit was lost — that would indicate the retry loop is not working.

4. Optionally, verify the snapshot history shows two separate append snapshots:

```sql
SELECT snapshot_id, committed_at, operation
FROM "occ_retry_test"."concurrent_occ$snapshots"
ORDER BY committed_at;
```

Both snapshots should have `operation = 'append'`.

### Cleaning up test tables

The integration tests call `DropTable` in a `defer` after each test (best-effort).
If the suite is interrupted before teardown, clean up manually:

```bash
# Using the AWS CLI (S3 Tables API)
aws s3tables delete-table \
  --table-bucket-arn arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket \
  --namespace occ_retry_test \
  --name concurrent_occ \
  --region us-east-1
```

---

## Changed files summary

| File | Change |
|---|---|
| `table/properties.go` | Added four `CommitRetry*` constants matching Java's `TableProperties.COMMIT_*` |
| `table/table.go` | Added `ErrCommitConflict` sentinel error |
| `table/snapshot_producers.go` | Added `attempt` counter, `lastManifestListPath` tracking, and `rebase()` method to `snapshotProducer` |
| `table/transaction.go` | Added `pendingProducers` field; rewrote `Commit()` to dispatch to `commitWithRetry()`; added `occBackoff()` |
| `catalog/rest/rest.go` | Changed `ErrCommitFailed` from a plain error to a type that wraps both `ErrRESTError` and `table.ErrCommitConflict` |
| `table/occ_retry_test.go` | 10 unit tests — all run without any external infrastructure |
| `table/occ_retry_aws_integration_test.go` | 4 integration tests — require real AWS S3 Tables (`-tags integration_aws`) |
