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
//   Bug: Snapshot manifest list is not rebuilt on OCC retry — concurrent writes are lost
//
// This file is self-contained.  Drop it into table/ on an unmodified upstream
// checkout and run:
//
//	go test ./table/ -run TestBugRepro_ManifestListNotInheritedOnRetry -v
//
// Expected on upstream main (unfixed):
//
//	--- FAIL: TestBugRepro_ManifestListNotInheritedOnRetry
//	    bug_repro_manifest_list_test.go:NN: expected 2 manifests (one per writer);
//	        got 1 — manifest list not rebuilt on OCC retry

package table_test

import (
	"context"
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
	"github.com/stretchr/testify/require"
)

// manifestListRepro1Catalog is a minimal in-memory catalog that enforces
// optimistic concurrency control (CAS) via AssertRefSnapshotID.  It is used
// only by TestBugRepro_ManifestListNotInheritedOnRetry.
type manifestListRepro1Catalog struct {
	mu       sync.Mutex
	current  table.Metadata
	location string
}

func newManifestListRepro1Catalog(meta table.Metadata, location string) *manifestListRepro1Catalog {
	return &manifestListRepro1Catalog{current: meta, location: location}
}

func (c *manifestListRepro1Catalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	meta := c.current
	c.mu.Unlock()
	return table.New(
		ident, meta, c.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	), nil
}

func (c *manifestListRepro1Catalog) CommitTable(
	_ context.Context,
	ident table.Identifier,
	reqs []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate requirements against current state (CAS check).
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

// TestBugRepro_ManifestListNotInheritedOnRetry demonstrates that when a
// Transaction.Commit is retried after a catalog conflict, the manifest list
// Avro file written before the first attempt is re-submitted unchanged.  That
// file was built against the stale parent and omits all manifests that a
// concurrent writer added between the first attempt and the retry.  The final
// snapshot therefore contains fewer rows than expected.
//
// Scenario:
//  1. Writer B appends one row to an empty table and commits successfully.
//     The catalog's HEAD now points at Writer B's snapshot.
//  2. Writer A also starts from the empty base (stale).  It appends its own
//     row and attempts to commit.
//  3. Writer A's first CommitTable call fails the CAS check (catalog HEAD ≠
//     nil; requirement says nil) → ErrCommitFailed.
//  4. The OCC retry loop reloads the catalog's current metadata (now
//     Writer B's snapshot), rewrites AssertRefSnapshotID, and re-issues the
//     SAME AddSnapshotUpdate — still pointing at the stale manifest list.
//  5. The retry is accepted by the catalog, but the resulting snapshot's
//     manifest list references only Writer A's new manifest.
//     Writer B's data is silently lost.
func TestBugRepro_ManifestListNotInheritedOnRetry(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	ctx := context.Background()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)

	// CommitNumRetriesKey must be at least 1.  The bug lives in the retry path;
	// with the upstream default of 0, the retry never runs and the test would
	// report "commit failed" instead of demonstrating the data-loss bug.
	props := iceberg.Properties{
		table.PropertyFormatVersion:        "2",
		table.CommitNumRetriesKey:          "1",
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
	}

	metaEmpty, err := table.NewMetadata(
		schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props,
	)
	require.NoError(t, err)

	cat := newManifestListRepro1Catalog(metaEmpty, location)
	fsF := func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

	arrowSc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	makeRow := func() arrow.Table {
		tbl, e := array.TableFromJSON(memory.DefaultAllocator, arrowSc,
			[]string{`[{"id": 1, "val": "x"}]`})
		require.NoError(t, e)
		return tbl
	}
	newTbl := func(base table.Metadata) *table.Table {
		return table.New(
			[]string{"default", "repro1"},
			base,
			location+"/metadata/v1.metadata.json",
			fsF, cat,
		)
	}

	// Step 1: Writer B commits one row from the empty base.
	rowB := makeRow()
	defer rowB.Release()
	txB := newTbl(metaEmpty).NewTransaction()
	require.NoError(t, txB.AppendTable(ctx, rowB, 1, nil))
	_, err = txB.Commit(ctx)
	require.NoError(t, err, "Writer B must commit successfully")

	// Step 2: Writer A starts from the same empty base (stale — no knowledge of
	// Writer B's snapshot).  Its first CommitTable call will fail the CAS check
	// because the catalog HEAD is now Writer B's snapshot ID, but the requirement
	// asserts nil (empty base).  With CommitNumRetriesKey="1" the retry loop runs:
	// it reloads the catalog (gets Writer B's state), rewrites AssertRefSnapshotID,
	// and re-submits the ORIGINAL AddSnapshotUpdate unchanged (BUG).
	rowA := makeRow()
	defer rowA.Release()
	txA := newTbl(metaEmpty).NewTransaction() // stale base
	require.NoError(t, txA.AppendTable(ctx, rowA, 1, nil))
	_, err = txA.Commit(ctx)
	require.NoError(t, err, "Writer A must succeed after OCC retry")

	// Step 3: Inspect the final snapshot's manifest list.
	cat.mu.Lock()
	finalMeta := cat.current
	cat.mu.Unlock()

	finalSnap := finalMeta.CurrentSnapshot()
	require.NotNil(t, finalSnap)

	manifests, err := finalSnap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)

	// The snapshot committed by Writer A must inherit Writer B's manifest from
	// the reloaded parent.  The manifest list should therefore have 2 entries.
	//
	// BUG: on upstream the manifest list was written before the first attempt
	// and is never rebuilt, so it lists only Writer A's own manifest (1 entry).
	// Writer B's data file is absent from the final snapshot.
	require.Len(t, manifests, 2,
		"expected 2 manifests (one per writer); got %d — manifest list not rebuilt on OCC retry",
		len(manifests))
}
