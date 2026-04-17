// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

// Regression tests for https://github.com/apache/iceberg-go/issues/830
// "feat: concurrent writer conflict detection (OCC + retry)"
//
// Issue summary
// -------------
// iceberg-go v0.5.0 sends AssertRefSnapshotID as a precondition on every
// Transaction.Commit().  When two goroutines race to commit, the one that
// "loses" receives HTTP 409 (ErrCommitFailed).  v0.5.0 treats this as a
// terminal error — there is no retry loop, no rebase, and no configurable
// back-off.
//
// This file pairs every "reproduce" test (shows the bug — commits fail) with
// a corresponding "fixed" test (commits succeed with the OCC retry loop).
//
// Executable commands
// -------------------
// Run reproduce tests (expectedly get errors, demonstrating the bug):
//
//   go test ./table/ -run TestIssue830_Reproduce -v
//
// Run fixed tests (should all pass, demonstrating the resolution):
//
//   go test ./table/ -run TestIssue830_Fixed -v
//
// Run everything:
//
//   go test ./table/ -run TestIssue830 -race -v
//
// For live AWS integration tests see occ_retry_aws_integration_test.go.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// stock behavior properties — reproduces v0.5.0: zero retries, any 409 fatal
// ---------------------------------------------------------------------------

// stockProps returns properties that mimic v0.5.0 behavior: no retry budget,
// so the very first ErrCommitFailed is returned immediately to the caller.
func stockProps() iceberg.Properties {
	return iceberg.Properties{
		table.CommitNumRetriesKey:       "0", // v0.5.0 had no retry loop at all
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
}

// fastProps returns properties for the fixed code: 4 retries with no sleep
// (so unit tests run instantly).
func fastProps() iceberg.Properties {
	return iceberg.Properties{
		// CommitNumRetriesKey intentionally omitted — uses default of 4 (Java parity).
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
}

// ---------------------------------------------------------------------------
// regression830Table creates a fresh table backed by a mock catalog that
// returns `conflicts` HTTP 409 responses before allowing a commit to succeed.
// This is the minimal harness needed to reproduce + verify issue #830.
// ---------------------------------------------------------------------------

func regression830Table(
	t *testing.T,
	location string,
	props iceberg.Properties,
	conflicts int,
) (*table.Table, *conflictThenSucceedCatalog) {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)

	all := iceberg.Properties{table.PropertyFormatVersion: "2"}
	for k, v := range props {
		all[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, all)
	require.NoError(t, err)

	cat := &conflictThenSucceedCatalog{
		current:       meta,
		conflictsLeft: conflicts,
		location:      location,
	}

	tbl := table.New(
		[]string{"default", "regression830"},
		meta,
		location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)
	return tbl, cat
}

// regression830ArrowTable builds a single-row Arrow table for writing.
func regression830ArrowTable(t *testing.T) arrow.Table {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	tbl, err := array.TableFromJSON(memory.DefaultAllocator, sc,
		[]string{`[{"id": 1, "val": "hello"}]`})
	require.NoError(t, err)
	return tbl
}

// ===========================================================================
// 1. Single writer — reproduce
// ===========================================================================

// TestIssue830_Reproduce_SingleConflictFatal reproduces the core bug from
// issue #830: a single catalog 409 is returned as a terminal ErrCommitFailed
// when the retry budget is zero (v0.5.0 behavior).
//
// In v0.5.0, Transaction.Commit calls catalog.CommitTable exactly once.
// Any 409 propagates directly to the caller with no recovery.
//
//	go test ./table/ -run TestIssue830_Reproduce_SingleConflictFatal -v
func TestIssue830_Reproduce_SingleConflictFatal(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	// Stock v0.5.0 behavior: zero retries.  Even a single conflict is fatal.
	tbl, cat := regression830Table(t, location, stockProps(), 1 /* conflicts */)

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(context.Background())

	// --- assert the bug ---
	require.Error(t, err, "stock v0.5.0 behavior: a single 409 must propagate as an error")
	assert.True(t, errors.Is(err, table.ErrCommitFailed),
		"the error must wrap ErrCommitFailed (was: %v)", err)

	// Exactly 1 commit attempt; no LoadTable (no retry).
	assert.Equal(t, int32(1), cat.commitTableCalls.Load(), "only 1 commit attempt expected")
	assert.Equal(t, int32(0), cat.loadTableCalls.Load(), "no LoadTable calls expected (no retry)")
}

// ===========================================================================
// 1. Single writer — fixed
// ===========================================================================

// TestIssue830_Fixed_SingleConflictRetried verifies that the OCC retry loop
// introduced to resolve issue #830 automatically retries the commit after a
// 409, reloads the table metadata, rebuilds manifests, and eventually succeeds.
//
//	go test ./table/ -run TestIssue830_Fixed_SingleConflictRetried -v
func TestIssue830_Fixed_SingleConflictRetried(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	// Fixed code: default 4 retries with no sleep so the test runs instantly.
	tbl, cat := regression830Table(t, location, fastProps(), 1 /* conflicts */)

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(context.Background())

	// --- assert the fix ---
	require.NoError(t, err, "fixed code: a single 409 must be retried transparently")

	// 2 commit attempts (1 conflict + 1 success) and 1 LoadTable to rebase.
	assert.Equal(t, int32(2), cat.commitTableCalls.Load(),
		"expected 2 commit attempts: 1 failed + 1 success")
	assert.Equal(t, int32(1), cat.loadTableCalls.Load(),
		"expected 1 LoadTable call to refresh metadata before retry")
}

// ===========================================================================
// 2. Multiple conflicts — reproduce
// ===========================================================================

// TestIssue830_Reproduce_MaxRetriesExhausted shows that when the number of
// consecutive catalog conflicts exceeds the retry budget, the final
// ErrCommitFailed is returned to the caller.
//
//	go test ./table/ -run TestIssue830_Reproduce_MaxRetriesExhausted -v
func TestIssue830_Reproduce_MaxRetriesExhausted(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	// maxRetries = 2, but the catalog returns 3 conflicts → budget exceeded.
	props := iceberg.Properties{
		table.CommitNumRetriesKey:       "2",
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
	tbl, cat := regression830Table(t, location, props, 3 /* conflicts > maxRetries */)

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(context.Background())

	require.Error(t, err)
	assert.True(t, errors.Is(err, table.ErrCommitFailed),
		"exhausted budget must surface ErrCommitFailed (was: %v)", err)

	// 3 total attempts: 1 initial + 2 retries.
	assert.Equal(t, int32(3), cat.commitTableCalls.Load(),
		"expected 3 total commit attempts (maxRetries+1)")
}

// ===========================================================================
// 2. Multiple conflicts — fixed
// ===========================================================================

// TestIssue830_Fixed_MultipleConflictsResolvedWithinBudget ensures that as
// long as the number of conflicts does not exceed the retry budget, every
// attempt is recovered and the commit ultimately succeeds.
//
//	go test ./table/ -run TestIssue830_Fixed_MultipleConflictsResolvedWithinBudget -v
func TestIssue830_Fixed_MultipleConflictsResolvedWithinBudget(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	// Default 4 retries; 4 conflicts = exactly at the budget edge.
	tbl, cat := regression830Table(t, location, fastProps(), table.CommitNumRetriesDefault)

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(context.Background())

	require.NoError(t, err, "commit must succeed: conflict count == retry budget")
	assert.Equal(t, int32(table.CommitNumRetriesDefault+1), cat.commitTableCalls.Load(),
		"expected %d commit attempts (budget+1)", table.CommitNumRetriesDefault+1)
}

// ===========================================================================
// 3. Concurrent writers — reproduce
// ===========================================================================

// occCASCatalog is a thread-safe OCC mock that tracks committed metadata:
// the first CommitTable call with a matching AssertRefSnapshotID succeeds,
// any subsequent caller whose base snapshot is now stale gets ErrCommitFailed.
//
// This closely models what a real REST catalog does (server-side CAS).
type occCASCatalog struct {
	mu       sync.Mutex
	current  table.Metadata
	location string
}

func newOCCCASCatalog(meta table.Metadata, location string) *occCASCatalog {
	return &occCASCatalog{current: meta, location: location}
}

func (c *occCASCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	meta := c.current
	c.mu.Unlock()
	return table.New(
		[]string{"default", "occ_cas"},
		meta,
		c.location+"/metadata/current.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	), nil
}

func (c *occCASCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	reqs []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate every requirement against the current metadata (CAS precondition).
	for _, req := range reqs {
		if err := req.Validate(c.current); err != nil {
			// Stale base — another writer already advanced the HEAD.
			return nil, "", fmt.Errorf("%w: CAS precondition failed: %v",
				table.ErrCommitFailed, err)
		}
	}

	// All requirements satisfied — apply updates and advance HEAD.
	newMeta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = newMeta
	return newMeta, c.location + "/metadata/committed.metadata.json", nil
}

// TestIssue830_Reproduce_ConcurrentWriters_NoRetryOneFails shows that without
// the retry loop, concurrent FastAppend writers lose any commit race with a
// terminal ErrCommitFailed.
//
// Scenario:
//
//   - Writer A and Writer B both read the table metadata simultaneously
//     (snapshot ID = nil, no snapshots yet).
//
//   - Writer A commits first, advancing the snapshot pointer.
//
//   - Writer B's CommitTable call now has a stale AssertRefSnapshotID → 409.
//
//   - With CommitNumRetriesKey = "0" (v0.5.0 behavior), Writer B fails.
//
//     go test ./table/ -run TestIssue830_Reproduce_ConcurrentWriters_NoRetryOneFails -v -race
func TestIssue830_Reproduce_ConcurrentWriters_NoRetryOneFails(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)
	props := iceberg.Properties{
		table.PropertyFormatVersion: "2",
	}
	// v0.5.0 behavior: zero retries.
	for k, v := range stockProps() {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	cat := newOCCCASCatalog(meta, location)

	newTbl := func() *table.Table {
		// Each writer gets a fresh *Table view of the same initial metadata
		// (simulating independent clients that all read at t=0).
		return table.New(
			[]string{"default", "occ_cas"},
			meta, // intentionally stale for writer B after writer A commits
			location+"/metadata/v1.metadata.json",
			func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
			cat,
		)
	}

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	// Writer A commits first — acquires the initial (nil) snapshot.
	txA := newTbl().NewTransaction()
	require.NoError(t, txA.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))
	_, err = txA.Commit(context.Background())
	require.NoError(t, err, "Writer A must succeed")

	// Writer B uses a table view still pinned to the old (nil) snapshot.
	// Its AssertRefSnapshotID will fail the CAS check → 409.
	arrowTbl2 := regression830ArrowTable(t)
	defer arrowTbl2.Release()

	txB := newTbl().NewTransaction()
	require.NoError(t, txB.AppendTable(context.Background(), arrowTbl2, arrowTbl2.NumRows(), nil))
	_, err = txB.Commit(context.Background())

	// --- assert the bug ---
	require.Error(t, err, "Writer B must fail: stale base, no retry (v0.5.0 behavior)")
	assert.True(t, errors.Is(err, table.ErrCommitFailed),
		"error must wrap ErrCommitFailed (was: %v)", err)
}

// ===========================================================================
// 3. Concurrent writers — fixed
// ===========================================================================

// TestIssue830_Fixed_ConcurrentWriters_BothSucceed verifies that with the OCC
// retry loop enabled, two concurrent FastAppend writers both succeed — even
// when one of them loses the initial CAS race.
//
// Scenario:
//
//   - Writer A commits first (succeeds immediately).
//
//   - Writer B's commit fails with 409 (stale base).
//
//   - Writer B reloads the table (now sees Writer A's snapshot), rebases its
//     pending manifests, and retries the catalog PUT → succeeds.
//
//   - Final table has two snapshots (one per writer).
//
//     go test ./table/ -run TestIssue830_Fixed_ConcurrentWriters_BothSucceed -v -race
func TestIssue830_Fixed_ConcurrentWriters_BothSucceed(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)
	props := iceberg.Properties{
		table.PropertyFormatVersion: "2",
	}
	// Fixed code: 4 retries, no sleep.
	for k, v := range fastProps() {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	cat := newOCCCASCatalog(meta, location)

	newTbl := func() *table.Table {
		return table.New(
			[]string{"default", "occ_cas"},
			meta,
			location+"/metadata/v1.metadata.json",
			func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
			cat,
		)
	}

	arrowTbl := regression830ArrowTable(t)
	defer arrowTbl.Release()

	// Writer A commits first.
	txA := newTbl().NewTransaction()
	require.NoError(t, txA.AppendTable(context.Background(), arrowTbl, arrowTbl.NumRows(), nil))
	_, err = txA.Commit(context.Background())
	require.NoError(t, err, "Writer A must succeed")

	// Writer B uses stale metadata and will get 409 on the first attempt.
	// The OCC retry loop reloads via LoadTable (cat.LoadTable returns the
	// updated metadata after Writer A's commit), rebases, and retries.
	arrowTbl2 := regression830ArrowTable(t)
	defer arrowTbl2.Release()

	txB := newTbl().NewTransaction()
	require.NoError(t, txB.AppendTable(context.Background(), arrowTbl2, arrowTbl2.NumRows(), nil))
	_, err = txB.Commit(context.Background())

	// --- assert the fix ---
	require.NoError(t, err, "Writer B must succeed after OCC retry rebases against Writer A's snapshot")

	// Verify final state: the catalog has committed both writers' snapshots.
	cat.mu.Lock()
	finalMeta := cat.current
	cat.mu.Unlock()

	finalSnap := finalMeta.CurrentSnapshot()
	assert.NotNil(t, finalSnap, "final metadata must have a current snapshot")
}

// ===========================================================================
// 4. Default property values match Java SnapshotProducer
// ===========================================================================

// TestIssue830_PropertyDefaultsMatchJava verifies that the commit.retry.*
// property defaults match the Java SnapshotProducer constants exactly.
// Any drift here would change behaviour relative to the Java reference
// implementation described in issue #830.
//
//	go test ./table/ -run TestIssue830_PropertyDefaultsMatchJava -v
func TestIssue830_PropertyDefaultsMatchJava(t *testing.T) {
	assert.Equal(t, "commit.retry.num-retries", table.CommitNumRetriesKey)
	assert.Equal(t, 4, table.CommitNumRetriesDefault,
		"Java COMMIT_NUM_RETRIES_DEFAULT = 4")

	assert.Equal(t, "commit.retry.min-wait-ms", table.CommitMinRetryWaitMsKey)
	assert.Equal(t, 100, table.CommitMinRetryWaitMsDefault,
		"Java COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100")

	assert.Equal(t, "commit.retry.max-wait-ms", table.CommitMaxRetryWaitMsKey)
	assert.Equal(t, 60_000, table.CommitMaxRetryWaitMsDefault,
		"Java COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60_000")

	assert.Equal(t, "commit.retry.total-timeout-ms", table.CommitTotalRetryTimeoutMsKey)
	assert.Equal(t, 1_800_000, table.CommitTotalRetryTimeoutMsDefault,
		"Java COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 1_800_000")
}

// ===========================================================================
// 5. Semantic conflict validation — isolation level property constants
// ===========================================================================

// TestIssue830_IsolationLevelPropertyConstants verifies the property names and
// defaults match the Java IsolationLevel enum constants.
//
//	go test ./table/ -run TestIssue830_IsolationLevelPropertyConstants -v
func TestIssue830_IsolationLevelPropertyConstants(t *testing.T) {
	assert.Equal(t, "write.delete.isolation-level", table.WriteDeleteIsolationLevelKey,
		"must match Java TableProperties.WRITE_DELETE_ISOLATION_LEVEL")
	assert.Equal(t, table.IsolationLevel("serializable"), table.IsolationSerializable,
		"must match Java IsolationLevel.SERIALIZABLE.name()")
	assert.Equal(t, table.IsolationLevel("snapshot"), table.IsolationSnapshot,
		"must match Java IsolationLevel.SNAPSHOT.name()")
	assert.Equal(t, table.IsolationSerializable, table.WriteDeleteIsolationLevelDefault,
		"Java defaults to SERIALIZABLE")
}

// ---------------------------------------------------------------------------
// Helpers for conflict-validation tests
// ---------------------------------------------------------------------------

// conflictValidationTable sets up a v2 partitioned table suitable for
// conflict-detection tests. Returns the initial Metadata and an
// occCASCatalog ready for concurrent use.
func conflictValidationTable(
	t *testing.T,
	location string,
	extraProps iceberg.Properties,
) (table.Metadata, *iceberg.PartitionSpec, *occCASCatalog) {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 3, Name: "event_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "category",
			Transform: iceberg.IdentityTransform{},
		},
	)

	props := iceberg.Properties{
		table.PropertyFormatVersion:     "2",
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
	for k, v := range extraProps {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	return meta, &spec, newOCCCASCatalog(meta, location)
}

func fsFunc(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

// makeDataFile creates a test iceberg.DataFile with the given partition value.
func makeDataFile(t *testing.T, spec *iceberg.PartitionSpec, path string, category string) iceberg.DataFile {
	t.Helper()
	var partition map[int]any
	if category != "" {
		partition = map[int]any{1000: category}
	}
	b, err := iceberg.NewDataFileBuilder(*spec, iceberg.EntryContentData, path,
		iceberg.ParquetFile, partition, nil, nil, 100, 1024)
	require.NoError(t, err)
	return b.Build()
}

// makeEqDeleteFile creates an equality-delete DataFile targeting event_id (field 3)
// in the given partition.
func makeEqDeleteFile(t *testing.T, spec *iceberg.PartitionSpec, path string, category string) iceberg.DataFile {
	t.Helper()
	var partition map[int]any
	if category != "" {
		partition = map[int]any{1000: category}
	}
	b, err := iceberg.NewDataFileBuilder(*spec, iceberg.EntryContentEqDeletes, path,
		iceberg.ParquetFile, partition, nil, nil, 5, 512)
	require.NoError(t, err)
	return b.EqualityFieldIDs([]int{3}).Build()
}

// ===========================================================================
// 5a. RowDelta + equality deletes — serializable isolation (default)
// ===========================================================================

// TestIssue830_ConflictValidation_RowDelta_EqDeleteSerializable verifies that a
// RowDelta commit with equality delete files conflicts with a concurrent append
// that added data in the same partition under serializable isolation (the default).
//
// Setup:
//
//   - Worker B commits a data file in partition category="hot".
//
//   - Worker A (reading the OLD metadata) commits a RowDelta with an equality
//     delete in partition category="hot" (targets event_id).
//
//   - Worker A's commit races with the already-committed Worker B state.
//
//   - On OCC retry, validateConflicts detects the overlap → ErrConflictingDataFiles.
//
//     go test ./table/ -run TestIssue830_ConflictValidation_RowDelta_EqDeleteSerializable -v
func TestIssue830_ConflictValidation_RowDelta_EqDeleteSerializable(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	meta, spec, cat := conflictValidationTable(t, location, nil /* serializable by default */)

	// Worker B: commits a data file in category="hot".
	workerBTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	txB := workerBTbl.NewTransaction()
	dfB := makeDataFile(t, spec, location+"/data/file-B.parquet", "hot")
	require.NoError(t, txB.AddDataFiles(context.Background(), []iceberg.DataFile{dfB}, nil))
	_, err = txB.Commit(context.Background())
	require.NoError(t, err, "Worker B must succeed")

	// Worker A: uses the stale initial metadata (didn't see Worker B's commit).
	workerATbl := table.New(
		[]string{"default", "conflict_test"},
		meta, // stale — no snapshot yet
		location+"/metadata/v1.metadata.json",
		fsFunc, cat,
	)
	eqDel := makeEqDeleteFile(t, spec, location+"/data/eq-delete-A.parquet", "hot")

	txA := workerATbl.NewTransaction()
	rd := txA.NewRowDelta(nil)
	rd.AddDeletes(eqDel)
	require.NoError(t, rd.Commit(context.Background()))

	_, err = txA.Commit(context.Background())

	// Serializable isolation: Worker B's data in the same partition is a conflict.
	require.Error(t, err)
	assert.True(t, errors.Is(err, table.ErrConflictingDataFiles),
		"serializable isolation must surface ErrConflictingDataFiles (was: %v)", err)
}

// ===========================================================================
// 5b. RowDelta + equality deletes — snapshot isolation
// ===========================================================================

// TestIssue830_ConflictValidation_RowDelta_EqDeleteSnapshotIsolation verifies that
// the same scenario as the serializable test SUCCEEDS when the table is configured
// with snapshot isolation — the equality deletes may not cover the new data, but
// the application has opted in to that behaviour.
//
//	go test ./table/ -run TestIssue830_ConflictValidation_RowDelta_EqDeleteSnapshotIsolation -v
func TestIssue830_ConflictValidation_RowDelta_EqDeleteSnapshotIsolation(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	meta, spec, cat := conflictValidationTable(t, location, iceberg.Properties{
		table.WriteDeleteIsolationLevelKey: string(table.IsolationSnapshot),
	})

	// Worker B: commits a data file in category="hot".
	workerBTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	txB := workerBTbl.NewTransaction()
	dfB := makeDataFile(t, spec, location+"/data/file-B.parquet", "hot")
	require.NoError(t, txB.AddDataFiles(context.Background(), []iceberg.DataFile{dfB}, nil))
	_, err = txB.Commit(context.Background())
	require.NoError(t, err, "Worker B must succeed")

	// Worker A: stale metadata + equality delete in same partition.
	workerATbl := table.New(
		[]string{"default", "conflict_test"},
		meta,
		location+"/metadata/v1.metadata.json",
		fsFunc, cat,
	)
	eqDel := makeEqDeleteFile(t, spec, location+"/data/eq-delete-A.parquet", "hot")

	txA := workerATbl.NewTransaction()
	rd := txA.NewRowDelta(nil)
	rd.AddDeletes(eqDel)
	require.NoError(t, rd.Commit(context.Background()))

	// Snapshot isolation: the overlap is accepted — commit must succeed.
	_, err = txA.Commit(context.Background())
	require.NoError(t, err,
		"snapshot isolation must allow eq-delete commit despite concurrent data in same partition")
}

// ===========================================================================
// 5c. RowDelta + equality deletes — no conflict (different partition)
// ===========================================================================

// TestIssue830_ConflictValidation_RowDelta_NoConflictDifferentPartition verifies that
// a concurrent append in a DIFFERENT partition does not cause a conflict, even under
// serializable isolation.
//
//	go test ./table/ -run TestIssue830_ConflictValidation_RowDelta_NoConflictDifferentPartition -v
func TestIssue830_ConflictValidation_RowDelta_NoConflictDifferentPartition(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	meta, spec, cat := conflictValidationTable(t, location, nil /* serializable */)

	// Worker B: commits a data file in category="cold" (DIFFERENT partition).
	workerBTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	txB := workerBTbl.NewTransaction()
	dfB := makeDataFile(t, spec, location+"/data/file-B.parquet", "cold")
	require.NoError(t, txB.AddDataFiles(context.Background(), []iceberg.DataFile{dfB}, nil))
	_, err = txB.Commit(context.Background())
	require.NoError(t, err, "Worker B must succeed")

	// Worker A: equality delete in category="hot" (different partition from Worker B).
	workerATbl := table.New(
		[]string{"default", "conflict_test"},
		meta,
		location+"/metadata/v1.metadata.json",
		fsFunc, cat,
	)
	eqDel := makeEqDeleteFile(t, spec, location+"/data/eq-delete-A.parquet", "hot")

	txA := workerATbl.NewTransaction()
	rd := txA.NewRowDelta(nil)
	rd.AddDeletes(eqDel)
	require.NoError(t, rd.Commit(context.Background()))

	// No partition overlap → no conflict → must succeed.
	_, err = txA.Commit(context.Background())
	require.NoError(t, err,
		"serializable isolation must allow eq-delete when concurrent data is in a different partition")
}

// ===========================================================================
// 5d. RowDelta with position deletes only — never conflicts
// ===========================================================================

// TestIssue830_ConflictValidation_RowDelta_PosDeleteNoConflict verifies that a RowDelta
// containing only positional delete files (no equality deletes) is always safe to retry
// regardless of isolation level, because positional deletes are referenced by exact
// file path and do not suffer from the "missed concurrent data" problem.
//
//	go test ./table/ -run TestIssue830_ConflictValidation_RowDelta_PosDeleteNoConflict -v
func TestIssue830_ConflictValidation_RowDelta_PosDeleteNoConflict(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	meta, spec, cat := conflictValidationTable(t, location, nil /* serializable */)

	// Worker B: commits a data file in the same partition.
	workerBTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	txB := workerBTbl.NewTransaction()
	dfB := makeDataFile(t, spec, location+"/data/file-B.parquet", "hot")
	require.NoError(t, txB.AddDataFiles(context.Background(), []iceberg.DataFile{dfB}, nil))
	_, err = txB.Commit(context.Background())
	require.NoError(t, err)

	// Worker A: position delete (no equality field IDs).
	workerATbl := table.New(
		[]string{"default", "conflict_test"},
		meta,
		location+"/metadata/v1.metadata.json",
		fsFunc, cat,
	)
	b, err := iceberg.NewDataFileBuilder(*spec, iceberg.EntryContentPosDeletes,
		location+"/data/pos-delete-A.parquet",
		iceberg.ParquetFile, map[int]any{1000: "hot"}, nil, nil, 5, 512)
	require.NoError(t, err)
	posDelFile := b.Build()

	txA := workerATbl.NewTransaction()
	rd := txA.NewRowDelta(nil)
	rd.AddDeletes(posDelFile)
	require.NoError(t, rd.Commit(context.Background()))

	// Positional-only RowDelta: always safe to retry — no conflict.
	_, err = txA.Commit(context.Background())
	require.NoError(t, err,
		"positional-only RowDelta must never report ErrConflictingDataFiles")
}

// ===========================================================================
// 5e. Overwrite — expression-based conflict
// ===========================================================================

// TestIssue830_ConflictValidation_Overwrite_ExpressionConflict verifies that a
// Delete (copy-on-write) operation fails with ErrConflictingDataFiles when a
// concurrent commit added data files that match the delete filter — those rows
// would be missed by the deletion and remain in the table.
//
// Setup:
//
//   - Seed: a data file is committed while base is empty.
//
//   - Worker B: appends another data file while Worker A is open (stale base).
//
//   - Worker A: executes a "delete all" (AlwaysTrue filter) from the stale base.
//
//   - Worker A's commit races with Worker B → 409 → retry.
//
//   - validateConflicts: Worker B's new file matches AlwaysTrue → ErrConflictingDataFiles.
//
//     go test ./table/ -run TestIssue830_ConflictValidation_Overwrite_ExpressionConflict -v
func TestIssue830_ConflictValidation_Overwrite_ExpressionConflict(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	props := iceberg.Properties{
		table.PropertyFormatVersion:     "2",
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, props)
	require.NoError(t, err)
	cat := newOCCCASCatalog(meta, location)

	// Seed the table with one data file so the Delete has something to scan.
	seedTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)
	txSeed := seedTbl.NewTransaction()
	b0, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		location+"/data/seed.parquet", iceberg.ParquetFile, nil, nil, nil, 50, 512)
	require.NoError(t, err)
	require.NoError(t, txSeed.AddDataFiles(context.Background(), []iceberg.DataFile{b0.Build()}, nil))
	_, err = txSeed.Commit(context.Background())
	require.NoError(t, err)

	// Snapshot what Worker A will use as base (post-seed, pre-Worker-B).
	baseTbl, err := cat.LoadTable(context.Background(), nil)
	require.NoError(t, err)

	// Worker B: appends a NEW data file AFTER Worker A's base snapshot.
	txB := baseTbl.NewTransaction()
	bB, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		location+"/data/file-B.parquet", iceberg.ParquetFile, nil, nil, nil, 30, 256)
	require.NoError(t, err)
	require.NoError(t, txB.AddDataFiles(context.Background(), []iceberg.DataFile{bB.Build()}, nil))
	_, err = txB.Commit(context.Background())
	require.NoError(t, err, "Worker B must succeed")

	// Worker A: from the base snapshot (doesn't see Worker B yet).
	// Deletes ALL data (AlwaysTrue filter) — equivalent to a full-table overwrite.
	txA := baseTbl.NewTransaction()
	err = txA.Delete(context.Background(), iceberg.AlwaysTrue{}, nil)
	require.NoError(t, err)

	_, err = txA.Commit(context.Background())

	// Worker A's delete would miss Worker B's newly-added file — must be rejected.
	require.Error(t, err)
	assert.True(t, errors.Is(err, table.ErrConflictingDataFiles),
		"delete must surface ErrConflictingDataFiles when concurrent data matches the filter (was: %v)", err)
}
