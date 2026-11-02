<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
-->

# Unset field IDs put the partition on the wrong column, with no error

> Draft issue for apache/iceberg-go. Not filed yet.

I forgot to set `ID` on my fields, so they were all 0. `CreateTable` didn't complain, but the table usually came out partitioned by the wrong column:

```go
sc := iceberg.NewSchema(0,
	iceberg.NestedField{Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
	iceberg.NestedField{Name: "event", Type: iceberg.PrimitiveTypes.String, Required: true},
)
spec, _ := iceberg.NewPartitionSpecOpts(
	iceberg.AddPartitionFieldByName("created_at", "created_day", iceberg.DayTransform{}, sc, nil))
tbl, _ := cat.CreateTable(ctx, ident, sc, catalog.WithPartitionSpec(&spec))
// tbl.Spec() is day(event) about 7 times in 8
```

`Append` works too (rows get a null partition). The first error only shows up when you query `event = 'login'`:
`cannot apply day transform for type string`

Repro test that fails every run: https://github.com/mzzz-zzm/iceberg-go/tree/poc/unset-field-ids/poc (`go test -v ./poc/`)

It looks like `checkDuplicateFieldIDs` skips IDs ≤ 0 ([here](https://github.com/apache/iceberg-go/blob/b354505/schema.go#L104-L106), from #879). Then `reassignIDs` [looks up](https://github.com/apache/iceberg-go/blob/b354505/table/metadata.go#L3740) the partition source's name by ID 0. That ID has two names, and [map order](https://github.com/apache/iceberg-go/blob/b354505/schema.go#L1129-L1133) picks one. Every catalog except REST goes through this path.

Java and iceberg-rust reject duplicate field IDs (0 included) when the schema is built. Could `CreateTable` do the same? A single 0 should still be fine (#761).
