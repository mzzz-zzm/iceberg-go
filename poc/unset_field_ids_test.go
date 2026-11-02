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

// Package poc reproduces table creation with a schema whose field IDs were
// never set. NestedField.ID defaults to 0, NewSchema accepts the duplicate
// zeros, and table.NewMetadata rebinds day(created_at) to a random column.
// Neither CreateTable nor Append fails: rows are written with a null partition
// value, and the first error is a query that filters on the column the
// partition moved to.
//
//	go test -v ./poc/                     # local Hadoop catalog in a temp dir
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

func TestUnsetFieldIDsLocal(t *testing.T) {
	ctx := context.Background()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))

	reproduce(t, cat, "db", 20)
}

// unsetIDSchema leaves every NestedField.ID at its zero value.
func unsetIDSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{Name: "user_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{Name: "event", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
}

// rowFilter matches the row written by oneRow, by the column the partition moved to.
var rowFilter = map[string]iceberg.BooleanExpression{
	"user_id": iceberg.EqualTo(iceberg.Reference("user_id"), int64(1)),
	"event":   iceberg.EqualTo(iceberg.Reference("event"), "login"),
}

// reproduce creates tables partitioned by day(created_at). At the first table
// whose partition landed on another column, it writes a row, reads it back
// with a filter on that column, and fails. It passes if CreateTable rejects
// the schema, as Java and iceberg-rust do.
func reproduce(t *testing.T, cat catalog.Catalog, namespace string, tries int) {
	ctx := context.Background()
	bound := map[string]int{}
	for i := range tries {
		// A fresh schema each time: Schema caches its ID-to-name index.
		sc := unsetIDSchema()
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

		created := tbl.Spec()
		col, _ := tbl.Schema().FindColumnName(created.Field(0).FirstSourceID())
		bound[col]++
		if col == "created_at" {
			continue
		}

		tbl, err = tbl.Append(ctx, oneRow(t, tbl), nil)
		require.NoError(t, err, "Append")
		files, err := tbl.Scan().PlanFiles(ctx)
		require.NoError(t, err)
		_, scanErr := tbl.Scan(table.WithRowFilter(rowFilter[col])).ToArrowTable(ctx)

		t.Fatalf("CreateTable and Append succeeded, but day(created_at) is bound to %q (tables so far: %v)\n"+
			"written partition values: %v\nscan filtered on %s: %v",
			col, bound, files[0].File.Partition(), col, scanErr)
	}
	t.Logf("all %d tables kept day(created_at)", tries)
}

func oneRow(t *testing.T, tbl *table.Table) array.RecordReader {
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc,
		strings.NewReader(`[{"created_at": "2026-11-02T00:00:00Z", "user_id": 1, "event": "login"}]`))
	require.NoError(t, err)
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	require.NoError(t, err)

	return rdr
}
