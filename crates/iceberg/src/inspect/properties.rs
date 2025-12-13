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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::StringBuilder;
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Type};
use crate::table::Table;

/// Properties table showing table properties as key-value pairs.
///
/// This metadata table exposes all table properties, allowing users to
/// query and inspect table configuration via SQL.
///
/// # Schema
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `key` | `String` | Property key |
/// | `value` | `String` | Property value |
///
/// Properties are returned sorted by key for consistent output.
pub struct PropertiesTable<'a> {
    table: &'a Table,
}

impl<'a> PropertiesTable<'a> {
    /// Create a new Properties table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema for the properties table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::String)),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::String)),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the properties table and returns Arrow record batches.
    ///
    /// Properties are sorted by key for consistent, predictable output.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let properties = self.table.metadata().properties();

        // Sort by key for consistent output
        let mut sorted: Vec<_> = properties.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));

        // Build arrays
        let mut key_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();

        for (key, value) in sorted {
            key_builder.append_value(key);
            value_builder.append_value(value);
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
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
    async fn test_properties_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().properties().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "key": Utf8, metadata: {"PARQUET:field_id": "1"} },
                Field { "value": Utf8, metadata: {"PARQUET:field_id": "2"} }"#]],
            expect![[r#"
                key: StringArray
                [
                  "read.split.target.size",
                ],
                value: StringArray
                [
                  "134217728",
                ]"#]],
            &[],
            None,
        );
    }

    #[tokio::test]
    async fn test_properties_table_schema() {
        let table = TableTestFixture::new().table;
        let metadata_table = table.inspect();
        let props_table = metadata_table.properties();
        let schema = props_table.schema();

        assert_eq!(schema.as_struct().fields().len(), 2);

        let key_field = schema.field_by_name("key").unwrap();
        assert_eq!(key_field.name, "key");
        assert!(key_field.required);

        let value_field = schema.field_by_name("value").unwrap();
        assert_eq!(value_field.name, "value");
        assert!(value_field.required);
    }
}
