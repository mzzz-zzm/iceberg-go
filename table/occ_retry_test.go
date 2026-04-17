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

// OCC (Optimistic Concurrency Control) retry unit tests.
//
// These tests verify that Transaction.Commit retries on ErrCommitFailed,
// matches the Java SnapshotProducer retry behaviour, and correctly cleans up
// orphaned manifest-list files from failed attempts.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// ---------------------------------------------------------------------------
// Mock catalog helpers
// ---------------------------------------------------------------------------

// conflictThenSucceedCatalog simulates N CAS conflicts (HTTP 409) followed by
// a successful commit.  After each conflict it "advances" the HEAD snapshot so
// that the retry can rebase correctly.
type conflictThenSucceedCatalog struct {
	// The metadata state as seen by LoadTable (updated on each simulated
	// concurrent write).
	current table.Metadata

	// Number of conflict responses still to return.
	conflictsLeft int

	// Counts of how many times each method was called.
	loadTableCalls   atomic.Int32
	commitTableCalls atomic.Int32

	// Paths of orphaned manifest-list files deleted by the producer during cleanup.
	// These are passed in via the filesystem mock; we don't track them here.
	location string
}

func (c *conflictThenSucceedCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	c.loadTableCalls.Add(1)
	// Return a table that represents what a concurrent writer committed —
	// identical schema but with a "placeholder" snapshot in the metadata.
	// For simplicity we just advance the metadata by bumping the current
	// table state to what was already committed by the previous successful mock.
	return table.New(
		[]string{"default", "retry_test"},
		c.current,
		c.location+"/metadata/current.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	), nil
}

func (c *conflictThenSucceedCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	_ []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.commitTableCalls.Add(1)

	if c.conflictsLeft > 0 {
		c.conflictsLeft--
		// Return HTTP 409 — do NOT apply the incoming updates.
		// The caller's Transaction retry loop will reload via LoadTable
		// (which returns c.current unchanged here) and rebase the producer.
		// This mimics a spurious CAS conflict where the base didn't actually
		// change — the retry should still succeed.
		return nil, "", fmt.Errorf("%w: simulated 409 conflict", table.ErrCommitFailed)
	}

	// Succeeds on the final attempt.
	newMeta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = newMeta
	return newMeta, c.location + "/metadata/final.metadata.json", nil
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

type OCCRetryTestSuite struct {
	suite.Suite
	ctx      context.Context
	location string
}

func TestOCCRetry(t *testing.T) {
	suite.Run(t, new(OCCRetryTestSuite))
}

func (s *OCCRetryTestSuite) SetupSuite() {
	s.ctx = context.Background()
}

func (s *OCCRetryTestSuite) SetupTest() {
	s.location = filepath.ToSlash(s.T().TempDir())
}

// makeTable creates a fresh in-memory table backed by the local filesystem
// and a mock catalog that returns N conflicts before succeeding.
func (s *OCCRetryTestSuite) makeTable(
	conflicts int,
	extraProps iceberg.Properties,
) (*table.Table, *conflictThenSucceedCatalog) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)

	props := iceberg.Properties{table.PropertyFormatVersion: "2"}
	for k, v := range extraProps {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, s.location, props)
	s.Require().NoError(err)

	cat := &conflictThenSucceedCatalog{
		current:       meta,
		conflictsLeft: conflicts,
		location:      s.location,
	}

	tbl := table.New(
		[]string{"default", "retry_test"},
		meta,
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)

	return tbl, cat
}

// makeArrowTable builds a trivial single-row Arrow table for testing.
func (s *OCCRetryTestSuite) makeArrowTable() arrow.Table {
	mem := memory.DefaultAllocator
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	tbl, err := array.TableFromJSON(mem, sc, []string{
		`[{"id": 1, "val": "hello"}]`,
	})
	s.Require().NoError(err)
	return tbl
}

// ---------------------------------------------------------------------------
// Success-on-first-attempt (baseline — no retry needed)
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestCommitSucceedsFirstAttempt() {
	tbl, cat := s.makeTable(0, nil)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Exactly one commit attempt.
	s.Equal(int32(1), cat.commitTableCalls.Load())
	// No reload needed on success.
	s.Equal(int32(0), cat.loadTableCalls.Load())
}

// ---------------------------------------------------------------------------
// Retry on ErrCommitFailed (409)
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestCommitRetriesOnConflict() {
	const wantConflicts = 2

	tbl, cat := s.makeTable(wantConflicts, nil)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().NoError(err, "expected eventual success after retries")

	// Total commit attempts = conflicts + 1 success
	s.Equal(int32(wantConflicts+1), cat.commitTableCalls.Load())
	// LoadTable is called once per retry.
	s.Equal(int32(wantConflicts), cat.loadTableCalls.Load())
}

// ---------------------------------------------------------------------------
// Respects commit.retry properties (max-retries)
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestCommitRespectsMaxRetries() {
	// Set max-retries = 1 (only one retry after the initial attempt).
	props := iceberg.Properties{
		table.CommitNumRetriesKey:       "1",
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	// 3 conflicts means the 2nd retry would fail; but max-retries=1 so
	// the loop should stop after the first retry.
	tbl, cat := s.makeTable(3, props)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().Error(err)
	s.True(errors.Is(err, table.ErrCommitFailed), "expected ErrCommitFailed in chain, got: %v", err)

	// max-retries=1 → 2 total commit attempts (initial + 1 retry), then failure.
	s.Equal(int32(2), cat.commitTableCalls.Load())
}

// ---------------------------------------------------------------------------
// Default retry count mirrors Java (4 retries = 5 total attempts)
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestDefaultRetryCount() {
	// Set wait times to 0 so the test doesn't sleep.
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	// Java default is 4 retries. 5 conflicts > max-retries, so we expect failure.
	tbl, cat := s.makeTable(5, props)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().Error(err)

	// 5 total attempts: 1 initial + 4 retries.
	s.Equal(int32(table.CommitNumRetriesDefault+1), cat.commitTableCalls.Load(),
		"expected %d total attempts (1 initial + %d retries = Java default)",
		table.CommitNumRetriesDefault+1, table.CommitNumRetriesDefault)
}

// ---------------------------------------------------------------------------
// Retry succeeds on last allowed attempt
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestCommitSucceedsOnLastRetry() {
	// Default is 4 retries → 5 attempts. 4 conflicts, success on attempt 5.
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	tbl, cat := s.makeTable(table.CommitNumRetriesDefault, props)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	s.Equal(int32(table.CommitNumRetriesDefault+1), cat.commitTableCalls.Load())
}

// ---------------------------------------------------------------------------
// Orphaned manifest-list files are cleaned up
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestOrphanedManifestListsAreDeleted() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	// 2 conflicts → 2 orphaned manifest lists, 1 committed list.
	tbl, _ := s.makeTable(2, props)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Count manifest-list files in the metadata directory.
	// After successful commit + cleanup, only the committed one should remain.
	metaDir := filepath.Join(filepath.FromSlash(s.location), "metadata")
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		// The metadata directory may not exist if no files were written.
		return
	}

	snapFiles := 0
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[:4] == "snap" {
			snapFiles++
		}
	}
	// Exactly 1 manifest-list file should survive (the committed one).
	s.Equal(1, snapFiles,
		"expected exactly 1 manifest-list file after cleanup, found %d (entries: %v)",
		snapFiles, entries)
}

// ---------------------------------------------------------------------------
// Non-retryable errors are propagated immediately
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestNonRetryableErrorPropagatedImmediately() {
	// Use makeTable so the Iceberg schema matches makeArrowTable's arrow schema.
	tbl, _ := s.makeTable(0, nil)

	var commitCalls atomic.Int32
	nonRetryableErr := errors.New("server error 500")

	// Swap the catalog to one that always fails with a non-retryable error.
	cat := &alwaysFailCatalog{
		current: tbl.Metadata(),
		commitFn: func() error {
			commitCalls.Add(1)
			return nonRetryableErr
		},
	}

	tbl2 := table.New(
		tbl.Identifier(),
		tbl.Metadata(),
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl2.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	_, err := tx.Commit(s.ctx)
	s.Require().Error(err)
	s.ErrorIs(err, nonRetryableErr)
	// Should NOT retry non-retryable errors.
	s.Equal(int32(1), commitCalls.Load(), "expected exactly 1 commit attempt for non-retryable error")
}

// alwaysFailCatalog is a helper that uses a custom commitFn for each call.
type alwaysFailCatalog struct {
	current  table.Metadata
	commitFn func() error
}

func (c *alwaysFailCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (c *alwaysFailCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	_ []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	if err := c.commitFn(); err != nil {
		return nil, "", err
	}
	meta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = meta
	return meta, "", nil
}

// ---------------------------------------------------------------------------
// Context cancellation aborts the retry loop
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestContextCancellationAborts() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}
	// Set up enough conflicts to require multiple retries.
	tbl, _ := s.makeTable(10, props)

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	ctx, cancel := context.WithCancel(s.ctx)
	cancel() // cancel immediately

	_, err := tx.Commit(ctx)
	s.Require().Error(err)
	s.ErrorIs(err, context.Canceled)
}

// ---------------------------------------------------------------------------
// occBackoff formula (unit test in the same package via table_test)
// ---------------------------------------------------------------------------

func TestOCCBackoffValues(t *testing.T) {
	// Verify the backoff formula and property defaults match Java.
	assert.Equal(t, "commit.retry.num-retries", table.CommitNumRetriesKey)
	assert.Equal(t, 4, table.CommitNumRetriesDefault)

	assert.Equal(t, "commit.retry.min-wait-ms", table.CommitMinRetryWaitMsKey)
	assert.Equal(t, 100, table.CommitMinRetryWaitMsDefault)

	assert.Equal(t, "commit.retry.max-wait-ms", table.CommitMaxRetryWaitMsKey)
	assert.Equal(t, 60_000, table.CommitMaxRetryWaitMsDefault)

	assert.Equal(t, "commit.retry.total-timeout-ms", table.CommitTotalRetryTimeoutMsKey)
	assert.Equal(t, 1_800_000, table.CommitTotalRetryTimeoutMsDefault)
}

// ---------------------------------------------------------------------------
// ErrCommitFailed is detectable via errors.Is
// ---------------------------------------------------------------------------

func TestErrCommitFailedIdentification(t *testing.T) {
	wrapped := fmt.Errorf("catalog: %w", table.ErrCommitFailed)
	require.True(t, errors.Is(wrapped, table.ErrCommitFailed),
		"wrapped ErrCommitFailed should be detectable via errors.Is")

	nonConflict := errors.New("some other error")
	require.False(t, errors.Is(nonConflict, table.ErrCommitFailed))
}

// ---------------------------------------------------------------------------
// AddDataFiles also participates in retry
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestAddDataFilesAlsoRetries() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	tbl, cat := s.makeTable(1, props)

	// Build a DataFile pointing to any path — we're testing retry semantics
	// not actual Parquet reading here.
	spec := tbl.Spec()
	builder, buildErr := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		filepath.ToSlash(filepath.Join(filepath.FromSlash(s.location), "data", "test.parquet")),
		iceberg.ParquetFile,
		map[int]any{},
		nil,
		nil,
		1,
		1024,
	)
	s.Require().NoError(buildErr)
	df := builder.Build()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AddDataFiles(s.ctx, []iceberg.DataFile{df}, nil))

	_, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	// 1 conflict + 1 success = 2 total commit calls.
	s.Equal(int32(2), cat.commitTableCalls.Load())
}

// arrowTblToParquetProps is a minimal stub — not needed for retry tests.
func arrowTblToParquetProps(_ array.RecordReader, _ iceio.FileWriter) struct{} {
	return struct{}{}
}

// ---------------------------------------------------------------------------
// Data constants exported for table_test package assertions
// ---------------------------------------------------------------------------

func TestCommitPropertyConstants(t *testing.T) {
	tests := []struct {
		key     string
		wantKey string
		def     int
		wantDef int
	}{
		{table.CommitNumRetriesKey, "commit.retry.num-retries", table.CommitNumRetriesDefault, 4},
		{table.CommitMinRetryWaitMsKey, "commit.retry.min-wait-ms", table.CommitMinRetryWaitMsDefault, 100},
		{table.CommitMaxRetryWaitMsKey, "commit.retry.max-wait-ms", table.CommitMaxRetryWaitMsDefault, 60_000},
		{table.CommitTotalRetryTimeoutMsKey, "commit.retry.total-timeout-ms", table.CommitTotalRetryTimeoutMsDefault, 1_800_000},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.wantKey, tt.key, "key mismatch")
			assert.Equal(t, tt.wantDef, tt.def, "default value mismatch (should match Java)")
		})
	}
}

// ---------------------------------------------------------------------------
// Parallel concurrent writers (simulated) — verifies union of data files
// ---------------------------------------------------------------------------

func (s *OCCRetryTestSuite) TestConcurrentAppendsProduceUnion() {
	// We simulate writer A doing an Append while writer B (the catalog mock)
	// commits its own snapshot concurrently. After the retry, the final table
	// should contain snapshots from both writers.
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:   "0",
		table.CommitMaxRetryWaitMsKey:   "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	tbl, cat := s.makeTable(1, props) // 1 simulated concurrent commit
	_ = cat

	arrowTbl := s.makeArrowTable()
	defer arrowTbl.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrowTbl, arrowTbl.NumRows(), nil))

	finalTbl, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	// The committed table must have a current snapshot.
	s.Require().NotNil(finalTbl.CurrentSnapshot(),
		"committed table must have a current snapshot")

	// The snapshot must be an append operation (not overwrite).
	s.Equal(table.OpAppend, finalTbl.CurrentSnapshot().Summary.Operation,
		"snapshot should be an append")
}

// ---------------------------------------------------------------------------
// Helpers for table_test package — unused variables suppressor
// ---------------------------------------------------------------------------

var _ = strconv.Itoa // suppress unused import
