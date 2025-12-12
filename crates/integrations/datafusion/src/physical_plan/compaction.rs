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

//! Physical plan execution for Iceberg table compaction (rewrite_data_files).
//!
//! This module provides `IcebergCompactionExec` which executes file group rewrites
//! by reading data files, applying position/equality deletes, and writing compacted
//! Parquet files.
//!
//! ## Architecture
//!
//! ```text
//! RewriteDataFilesPlan (from core iceberg)
//!     │
//!     ▼
//! IcebergCompactionExec (this module)
//!     │ For each FileGroup:
//!     │   1. Check cancellation token
//!     │   2. Emit progress event (GroupStarted)
//!     │   3. Read data files with delete application
//!     │   4. Write merged output (TaskWriter)
//!     │   5. Emit progress event (GroupCompleted)
//!     │   6. Yield DataFile JSON
//!     ▼
//! IcebergCompactionCommitExec
//!     │ Collects all DataFile JSON
//!     │ Calls RewriteDataFilesCommitter::commit()
//!     ▼
//! Updated Table Snapshot
//! ```

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};

/// Column name for serialized data files output.
pub const COMPACTION_DATA_FILES_COL: &str = "data_files";

/// Column name for group ID output.
pub const COMPACTION_GROUP_ID_COL: &str = "group_id";

/// Schema for compaction executor output.
///
/// Returns schema with:
/// - `group_id`: UInt32 - which FileGroup this result belongs to
/// - `data_files`: Utf8 - JSON-serialized DataFile entries
pub fn compaction_output_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(COMPACTION_GROUP_ID_COL, DataType::UInt32, false),
        Field::new(COMPACTION_DATA_FILES_COL, DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_output_schema() {
        let schema = compaction_output_schema();
        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name(COMPACTION_DATA_FILES_COL).is_ok());
        assert!(schema.field_with_name(COMPACTION_GROUP_ID_COL).is_ok());
    }
}
