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

// Package poc shows what happens when a schema's field IDs are left unset (0):
// CreateTable puts day(created_at) on the wrong column, Append writes rows with
// a null partition, and the first error is a later query.
//
//	go test -v ./poc/                     # local, no setup
//	go test -v -tags integration ./poc/   # AWS S3 Tables, see aws_test.go
package poc

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestUnsetFieldIDs(t *testing.T) {
	ctx := context.Background()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))

	reproduce(t, cat, "db")
}

// reproduce fails while the bug exists and passes once CreateTable rejects the
// schema. Which column the partition lands on follows Go's map iteration order
// (wrong 7 times in 8), so it retries until it is wrong: 50 tries all landing
// right has a chance of about 1e-45.
func reproduce(t *testing.T, cat catalog.Catalog, namespace string) {
	ctx := context.Background()
	for i := range 50 {
		sc := iceberg.NewSchema(0, // no NestedField.ID set, so both are 0
			iceberg.NestedField{Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
			iceberg.NestedField{Name: "event", Type: iceberg.PrimitiveTypes.String, Required: true},
		)
		spec, err := iceberg.NewPartitionSpecOpts(
			iceberg.AddPartitionFieldByName("created_at", "created_day", iceberg.DayTransform{}, sc, nil))
		require.NoError(t, err)

		ident := catalog.ToIdentifier(namespace, fmt.Sprintf("unset_ids_%d", i))
		tbl, err := cat.CreateTable(ctx, ident, sc, catalog.WithPartitionSpec(&spec))
		if err != nil {
			t.Logf("CreateTable rejected the schema: %v", err)

			return
		}
		t.Cleanup(func() { _ = cat.PurgeTable(context.Background(), ident) })

		got := tbl.Spec()
		col, _ := tbl.Schema().FindColumnName(got.Field(0).FirstSourceID())
		if col == "created_at" {
			continue
		}

		tbl, err = tbl.Append(ctx, oneRow(t, tbl), nil)
		require.NoError(t, err)
		files, err := tbl.Scan().PlanFiles(ctx)
		require.NoError(t, err)
		_, queryErr := tbl.Scan(table.WithRowFilter(
			iceberg.EqualTo(iceberg.Reference("event"), "login"))).ToArrowTable(ctx)

		t.Fatalf("CreateTable and Append succeeded, but the table is partitioned by day(%s), not day(created_at)\n"+
			"partition values written: %v\nquery event = 'login': %v",
			col, files[0].File.Partition(), queryErr)
	}
	t.Log("every table kept day(created_at)")
}

func oneRow(t *testing.T, tbl *table.Table) array.RecordReader {
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc,
		strings.NewReader(`[{"created_at": "2026-11-02T00:00:00Z", "event": "login"}]`))
	require.NoError(t, err)
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	require.NoError(t, err)

	return rdr
}
