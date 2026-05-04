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
//   Bug: Conflict detection is silently bypassed when the writer starts from an empty table
//
// This file is self-contained.  Drop it into table/ (package table — internal)
// on an unmodified upstream checkout and run:
//
//	go test ./table/ -run TestBugRepro_EmptyTableConflictDetectionBypassed -v
//
// Expected on upstream main (unfixed):
//
//	--- FAIL: TestBugRepro_EmptyTableConflictDetectionBypassed
//	    bug_repro_empty_table_conflict_test.go:NN:
//	        Error Trace: ...
//	        Error:        "[…]" should have 1 item(s), but has 0

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBugRepro_EmptyTableConflictDetectionBypassed shows that when a writer
// loaded the table before any snapshot existed on the target branch (i.e.
// base.SnapshotByName(branch) == nil), newConflictContext returns an empty
// concurrent list regardless of what snapshots the current metadata contains.
//
// Consequence: a RowDelta commit that carries equality-delete files will skip
// the serializable-isolation conflict check entirely.  If another writer
// committed data files between Writer A's load time and its commit time, those
// data files are invisible to the validators — the equality-delete commit is
// accepted even though it may not cover the concurrent data.
//
// Root cause: newConflictContext short-circuits with an empty concurrent list
// when baseHead == nil, reasoning that "there is nothing concurrent to
// validate against".  The correct behaviour is to walk all snapshots from
// currentHead back to the branch origin — those are the snapshots that the
// writer could not have seen.
func TestBugRepro_EmptyTableConflictDetectionBypassed(t *testing.T) {
	// Writer loaded the table before any snapshot was committed on main.
	base := newConflictTestMetadata(t, nil)

	// Meanwhile another writer committed snapshot 42.
	head := int64(42)
	current := newConflictTestMetadata(t, &head)

	ctx, err := newConflictContext(base, current, MainBranch, nil, true)
	require.NoError(t, err)

	// Snapshot 42 was committed while Writer had no view of the branch.
	// It MUST appear in ctx.concurrent so that conflict validators (e.g.
	// RowDelta's serializable check) can inspect the data files it added.
	//
	// BUG: on upstream, ctx.concurrent is empty — snapshot 42 is invisible.
	// Any equality-delete commit from the empty-base writer will be accepted
	// without validating against the concurrent data.
	assert.Len(t, ctx.concurrent, 1,
		"snapshot 42 must appear in concurrent when base has no branch head")
}
