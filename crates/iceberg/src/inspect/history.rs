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

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Type};
use crate::table::Table;

/// History table showing snapshot lineage and rollback detection.
///
/// This table tracks when each snapshot became the current snapshot,
/// enabling detection of rollbacks (snapshots that are no longer ancestors
/// of the current state).
///
/// # Schema
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `made_current_at` | `TimestampTz` | When the snapshot became current (UTC) |
/// | `snapshot_id` | `Long` | The snapshot ID |
/// | `parent_id` | `Long` (nullable) | Parent snapshot ID |
/// | `is_current_ancestor` | `Boolean` | Whether this is an ancestor of current snapshot |
///
/// The `made_current_at` timestamp uses UTC timezone (stored as microseconds since epoch).
pub struct HistoryTable<'a> {
    table: &'a Table,
}

impl<'a> HistoryTable<'a> {
    /// Create a new History table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema for the history table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(
                1,
                "made_current_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            ),
            NestedField::required(2, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "parent_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                4,
                "is_current_ancestor",
                Type::Primitive(PrimitiveType::Boolean),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the history table and returns Arrow record batches.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        // Build ancestor set for is_current_ancestor calculation
        let mut current_ancestors = HashSet::new();
        if let Some(current) = metadata.current_snapshot() {
            let mut snapshot_id = Some(current.snapshot_id());
            while let Some(id) = snapshot_id {
                current_ancestors.insert(id);
                snapshot_id = metadata
                    .snapshot_by_id(id)
                    .and_then(|s| s.parent_snapshot_id());
            }
        }

        // Build arrays from history (snapshot log)
        let mut made_current_at =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut is_current_ancestor = BooleanBuilder::new();

        for entry in metadata.history() {
            made_current_at.append_value(entry.timestamp_ms * 1000);
            snapshot_id.append_value(entry.snapshot_id);

            if let Some(snapshot) = metadata.snapshot_by_id(entry.snapshot_id) {
                parent_id.append_option(snapshot.parent_snapshot_id());
            } else {
                parent_id.append_null();
            }

            is_current_ancestor.append_value(current_ancestors.contains(&entry.snapshot_id));
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(made_current_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(is_current_ancestor.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_history_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  2018-01-04T21:22:35.770+00:00,
                  2019-04-12T20:29:15.770+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3051729675574597004,
                  3055729675574597004,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  null,
                  3051729675574597004,
                ],
                is_current_ancestor: BooleanArray
                [
                  true,
                  true,
                ]"#]],
            &[],
            None,
        );
    }
}
