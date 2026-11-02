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

//go:build integration

// These tests create, write to and purge up to 10 tables in a real S3 Tables
// bucket, using the default AWS credential chain:
//
//	TEST_S3TABLES_BUCKET_ARN=arn:aws:s3tables:<region>:<account>:bucket/<name> \
//	TEST_S3TABLES_DATABASE=<existing namespace> AWS_REGION=<region> \
//	go test -v -tags integration -run S3Tables ./poc/
package poc

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/require"
)

// Through the Glue catalog, iceberg-go builds the table metadata itself with
// table.NewMetadata, so the bug reaches S3 Tables.
func TestUnsetFieldIDsS3TablesGlue(t *testing.T) {
	region, account, bucket, namespace := s3TablesEnv(t)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	require.NoError(t, err)
	cat, err := glue.NewCatalog(glue.WithAWSConfig(cfg), glue.WithAWSProperties(glue.AWSProperties{
		glue.CatalogIdKey: account + ":s3tablescatalog/" + bucket,
	}))
	require.NoError(t, err)

	reproduce(t, cat, namespace, 10)
}

// Through the Iceberg REST endpoint, iceberg-go sends the IDs unchanged and the
// server assigns them, so this test shows what S3 Tables itself does.
func TestUnsetFieldIDsS3TablesREST(t *testing.T) {
	region, _, _, namespace := s3TablesEnv(t)
	cat, err := rest.NewCatalog(context.Background(), "s3tables",
		"https://s3tables."+region+".amazonaws.com/iceberg",
		rest.WithSigV4RegionSvc(region, "s3tables"),
		rest.WithWarehouseLocation(os.Getenv("TEST_S3TABLES_BUCKET_ARN")))
	require.NoError(t, err)

	reproduce(t, cat, namespace, 10)
}

func s3TablesEnv(t *testing.T) (region, account, bucket, namespace string) {
	arn, namespace := os.Getenv("TEST_S3TABLES_BUCKET_ARN"), os.Getenv("TEST_S3TABLES_DATABASE")
	parts := strings.Split(arn, ":") // arn:aws:s3tables:<region>:<account>:bucket/<name>
	if len(parts) != 6 || namespace == "" {
		t.Skip("set TEST_S3TABLES_BUCKET_ARN and TEST_S3TABLES_DATABASE")
	}

	return parts[3], parts[4], strings.TrimPrefix(parts[5], "bucket/"), namespace
}
