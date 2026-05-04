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

// Reproducer for:
//   Bug: RowDelta.validate uses AlwaysTrue filter — false conflicts for
//        concurrent appends to different partitions
//
// This file is self-contained.  Drop it into table/ on an unmodified upstream
// checkout and run:
//
//	go test ./table/ -run TestBugRepro_RowDeltaFalseConflictDifferentPartition -v
//
// Expected on upstream main (unfixed):
//
//	--- FAIL: TestBugRepro_RowDeltaFalseConflictDifferentPartition
//	    bug_repro_rowdelta_partition_test.go:NN:
//	        Error:        Received unexpected error:
//	                      "commit failed, refresh and try again: concurrent data files added"

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// rowDeltaRepro3Catalog is a minimal in-memory CAS catalog for
// TestBugRepro_RowDeltaFalseConflictDifferentPartition.
type rowDeltaRepro3Catalog struct {
	mu       sync.Mutex
	current  table.Metadata
	location string
}

func newRowDeltaRepro3Catalog(meta table.Metadata, location string) *rowDeltaRepro3Catalog {
	return &rowDeltaRepro3Catalog{current: meta, location: location}
}

func (c *rowDeltaRepro3Catalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	meta := c.current
	c.mu.Unlock()
	return table.New(
		ident, meta, c.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	), nil
}

func (c *rowDeltaRepro3Catalog) CommitTable(
	_ context.Context,
	ident table.Identifier,
	reqs []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, req := range reqs {
		if err := req.Validate(c.current); err != nil {
			return nil, "", fmt.Errorf("%w: CAS check failed: %v", table.ErrCommitFailed, err)
		}
	}

	newMeta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = newMeta
	return newMeta, c.location + "/metadata/committed.metadata.json", nil
}

// makeRepro3DataFile builds a synthetic data-file entry in the given partition.
// The path does not need to physically exist; AddDataFiles records the metadata.
func makeRepro3DataFile(t *testing.T, spec *iceberg.PartitionSpec, path, category string) iceberg.DataFile {
	t.Helper()
	var partition map[int]any
	if category != "" {
		partition = map[int]any{1000: category}
	}
	b, err := iceberg.NewDataFileBuilder(
		*spec, iceberg.EntryContentData, path,
		iceberg.ParquetFile, partition, nil, nil, 100, 1024,
	)
	require.NoError(t, err)
	return b.Build()
}

// makeRepro3EqDeleteFile builds a synthetic equality-delete file entry in the
// given partition, targeting field ID 3 (event_id).
func makeRepro3EqDeleteFile(t *testing.T, spec *iceberg.PartitionSpec, path, category string) iceberg.DataFile {
	t.Helper()
	var partition map[int]any
	if category != "" {
		partition = map[int]any{1000: category}
	}
	b, err := iceberg.NewDataFileBuilder(
		*spec, iceberg.EntryContentEqDeletes, path,
		iceberg.ParquetFile, partition, nil, nil, 5, 512,
	)
	require.NoError(t, err)
	return b.EqualityFieldIDs([]int{3}).Build()
}

// TestBugRepro_RowDeltaFalseConflictDifferentPartition demonstrates that
// RowDelta.validate calls validateNoConflictingDataFiles with iceberg.AlwaysTrue{}
// as the filter predicate.  AlwaysTrue matches every data file in every
// partition, so a concurrent FastAppend in a completely different partition
// causes the RowDelta commit to be incorrectly rejected with
// ErrConflictingDataFiles even though the two writes cannot interfere.
//
// Scenario:
//  1. A baseline commit establishes snapshot S0 so that Writer A starts from a
//     non-empty base (this isolates the AlwaysTrue bug from the separate empty-
//     base conflict-bypass bug).
//  2. Worker B appends a data file in partition category="cold".
//  3. Writer A starts from S0 (snapshot before Worker B's commit), adds an
//     equality-delete targeting field event_id in partition category="hot".
//  4. Writer A's first CommitTable call fails the CAS check (S0 vs Worker B's
//     HEAD) → ErrCommitFailed → OCC retry runs.
//  5. On retry: newConflictContext returns [Worker B's snapshot] as concurrent.
//     RowDelta.validate calls validateNoConflictingDataFiles(AlwaysTrue{}) →
//     Worker B's "cold" file matches AlwaysTrue → ErrConflictingDataFiles.
//
// The two partitions are disjoint.  Under correct semantics the commit must
// succeed.  Before the fix it is rejected.
func TestBugRepro_RowDeltaFalseConflictDifferentPartition(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	ctx := context.Background()

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
		table.PropertyFormatVersion:        "2",
		table.CommitNumRetriesKey:          "1",
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	metaEmpty, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	cat := newRowDeltaRepro3Catalog(metaEmpty, location)
	fsF := func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

	newTbl := func(base table.Metadata) *table.Table {
		return table.New(
			[]string{"default", "repro3"},
			base,
			location+"/metadata/v1.metadata.json",
			fsF, cat,
		)
	}

	// Step 1: Establish a baseline snapshot S0 so Writer A has a non-empty base.
	// This is necessary to isolate Bug 3 (AlwaysTrue false positive) from Bug 2
	// (empty-base conflict bypass).  With a non-empty base, newConflictContext
	// correctly populates concurrent with Worker B's snapshot.
	baseline := makeRepro3DataFile(t, &spec, location+"/data/baseline.parquet", "warm")
	txSetup := newTbl(metaEmpty).NewTransaction()
	require.NoError(t, txSetup.AddDataFiles(ctx, []iceberg.DataFile{baseline}, nil))
	_, err = txSetup.Commit(ctx)
	require.NoError(t, err, "baseline commit must succeed")

	cat.mu.Lock()
	metaS0 := cat.current // snapshot S0 — this is Writer A's view of the table
	cat.mu.Unlock()

	// Step 2: Worker B appends a data file in partition category="cold".
	workerBTbl, err := cat.LoadTable(ctx, []string{"default", "repro3"})
	require.NoError(t, err)
	dfCold := makeRepro3DataFile(t, &spec, location+"/data/worker-b-cold.parquet", "cold")
	txB := workerBTbl.NewTransaction()
	require.NoError(t, txB.AddDataFiles(ctx, []iceberg.DataFile{dfCold}, nil))
	_, err = txB.Commit(ctx)
	require.NoError(t, err, "Worker B must commit successfully")

	// Step 3: Writer A starts from S0 (before Worker B's commit) and adds an
	// equality-delete in partition category="hot" — a completely different
	// partition from Worker B's "cold" file.
	writerATbl := newTbl(metaS0) // stale: does not know about Worker B's "cold" commit
	eqDelHot := makeRepro3EqDeleteFile(t, &spec, location+"/data/writer-a-eq-del-hot.parquet", "hot")
	txA := writerATbl.NewTransaction()
	rd := txA.NewRowDelta(nil)
	rd.AddDeletes(eqDelHot)
	require.NoError(t, rd.Commit(ctx))

	// Step 4: Writer A commits.
	//   Attempt 1: CAS fails — S0 vs Worker B's HEAD → ErrCommitFailed.
	//   Retry:     newConflictContext(base=S0, current=S0+Worker_B, ...) builds
	//              concurrent=[Worker_B_snapshot].
	//              RowDelta.validate calls validateNoConflictingDataFiles(AlwaysTrue{}).
	//              AlwaysTrue matches Worker B's "cold" data file.
	//              BUG: ErrConflictingDataFiles is returned.
	//              FIX: only "hot" eq-delete partition is checked; "cold" ≠ "hot"
	//              → no conflict → commit succeeds.
	_, err = txA.Commit(ctx)
	require.NoError(t, err,
		"serializable isolation must allow an eq-delete when the only concurrent "+
			"data file is in a completely different partition (AlwaysTrue bug)")
}
