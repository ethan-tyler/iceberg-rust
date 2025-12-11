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

//! DELETE operation support for Iceberg tables in DataFusion.
//!
//! This module provides both programmatic and SQL DELETE operations for Iceberg tables.
//!
//! # SQL DELETE Support
//!
//! SQL DELETE statements are supported via `IcebergTableProvider`:
//!
//! ```sql
//! DELETE FROM catalog.namespace.table WHERE id = 42;
//! DELETE FROM catalog.namespace.table; -- deletes all rows
//! ```
//!
//! # Known Limitations
//!
//! - **V1 tables**: Position deletes require Iceberg format version 2.
//!
//! # Partition Evolution
//!
//! DELETE file serialization tracks partition spec IDs via [`crate::partition_utils`],
//! enabling correct serialization/deserialization across DataFusion executors. However,
//! **full DML support for tables with evolved partition specs is not yet implemented**.
//! Operations that span files from multiple partition specs may fail with errors like
//! "Partition value is not compatible with partition type".
//!
//! # Usage
//!
//! ## Programmatic API
//!
//! Use `IcebergTableProvider::delete()` for programmatic deletes:
//!
//! ```ignore
//! use datafusion::prelude::*;
//! use iceberg_datafusion::IcebergTableProvider;
//!
//! // Create the table provider
//! let provider = IcebergTableProvider::try_new(catalog, table_ident).await?;
//!
//! // Delete rows where id = 42
//! let deleted_count = provider
//!     .delete(&session_state, Some(col("id").eq(lit(42))))
//!     .await?;
//! println!("Deleted {} rows", deleted_count);
//!
//! // Delete all rows (full table delete)
//! let deleted_count = provider.delete(&session_state, None).await?;
//! ```
//!
//! ## Helper Function
//!
//! Use `delete_from_table()` for a standalone delete operation:
//!
//! ```ignore
//! use iceberg_datafusion::delete::delete_from_table;
//! use iceberg::NamespaceIdent;
//!
//! let deleted_count = delete_from_table(
//!     &catalog,
//!     &NamespaceIdent::from_strs(["my_namespace"])?,
//!     "my_table",
//!     &session_state,
//!     Some(col("status").eq(lit("inactive"))),
//! ).await?;
//! ```
//!
//! # Architecture
//!
//! The DELETE operation is implemented as a three-stage execution plan:
//!
//! 1. **IcebergDeleteScanExec**: Scans the table and identifies rows matching the
//!    DELETE predicate, outputting (file_path, position) tuples.
//!
//! 2. **IcebergDeleteWriteExec**: Writes position delete files using the
//!    `PositionDeleteFileWriter`, grouping deletes by their source file.
//!
//! 3. **IcebergDeleteCommitExec**: Commits the delete files to the table using
//!    the `DeleteAction` transaction.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use iceberg::{Catalog, NamespaceIdent};

use crate::error::to_datafusion_error;
use crate::table::IcebergTableProvider;

/// Deletes rows from an Iceberg table that match the given predicate.
///
/// This is a convenience function that creates an `IcebergTableProvider` and
/// calls its `delete()` method. For multiple operations on the same table,
/// it's more efficient to create the provider once and reuse it.
///
/// # Arguments
///
/// * `catalog` - The catalog containing the table
/// * `namespace` - The namespace containing the table
/// * `table_name` - The name of the table to delete from
/// * `session` - The DataFusion session state
/// * `predicate` - Optional filter expression. If `None`, deletes all rows.
///
/// # Returns
///
/// The number of rows deleted.
///
/// # Errors
///
/// Returns an error if:
/// - The table doesn't exist
/// - The delete operation fails
/// - The commit fails (e.g., due to conflicts)
///
/// # Example
///
/// ```ignore
/// use datafusion::prelude::*;
/// use iceberg::{Catalog, NamespaceIdent};
/// use iceberg_datafusion::delete::delete_from_table;
///
/// // Delete all inactive users
/// let deleted = delete_from_table(
///     &catalog,
///     &NamespaceIdent::from_strs(["db"])?,
///     "users",
///     &session_state,
///     Some(col("active").eq(lit(false))),
/// ).await?;
///
/// println!("Deleted {} inactive users", deleted);
/// ```
pub async fn delete_from_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    table_name: impl Into<String>,
    session: &dyn Session,
    predicate: Option<Expr>,
) -> DFResult<u64> {
    let provider = IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name)
        .await
        .map_err(to_datafusion_error)?;
    provider.delete(session, predicate).await
}

/// Parses a SQL WHERE clause string into a DataFusion expression.
///
/// # Deprecation Notice
///
/// **This function is deprecated** and always returns `NotImplemented`.
/// It will be removed in a future release.
///
/// **Use instead**: Construct predicates programmatically using DataFusion's
/// `col()` and `lit()` functions from `datafusion::prelude`:
///
/// ```rust,ignore
/// use datafusion::prelude::{col, lit};
///
/// // Instead of: parse_predicate("id = 42")
/// let predicate = col("id").eq(lit(42));
///
/// // Instead of: parse_predicate("status = 'active' AND age > 18")
/// let predicate = col("status").eq(lit("active")).and(col("age").gt(lit(18)));
/// ```
///
/// # Why Deprecated?
///
/// Proper SQL parsing requires schema context which is not available at parse time.
/// The programmatic approach with `col()` and `lit()` is type-safe and doesn't
/// require schema resolution.
///
/// # Arguments
///
/// * `where_clause` - The WHERE clause without the "WHERE" keyword
///
/// # Returns
///
/// Always returns `Err(DataFusionError::NotImplemented(...))`.
#[deprecated(
    since = "0.5.0",
    note = "Use DataFusion's col() and lit() functions to construct predicates. Example: col(\"id\").eq(lit(42))"
)]
pub fn parse_predicate(where_clause: &str) -> DFResult<Expr> {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    // Parse the WHERE clause as part of a SELECT statement
    let sql = format!("SELECT * FROM dummy WHERE {where_clause}");
    let dialect = GenericDialect {};

    let statements = DFParser::parse_sql_with_dialect(&sql, &dialect)?;
    if statements.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "Failed to parse predicate: no statements".to_string(),
        ));
    }

    // Extract the WHERE expression from the parsed statement
    if let datafusion::sql::parser::Statement::Statement(stmt) = &statements[0]
        && let datafusion::sql::sqlparser::ast::Statement::Query(query) = stmt.as_ref()
        && let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
        && select.selection.is_some()
    {
        // Convert SQL AST to DataFusion Expr
        // Note: This is a simplified conversion; a full implementation
        // would need schema context
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "parse_predicate is not yet fully implemented. \
             Please construct predicates using col() and lit() functions."
                .to_string(),
        ));
    }

    Err(datafusion::error::DataFusionError::Plan(
        "Failed to extract WHERE clause from parsed statement".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_parse_predicate_not_implemented() {
        // This tests that parse_predicate correctly returns NotImplemented
        // Note: parse_predicate is deprecated; use col() and lit() instead
        let result = parse_predicate("id = 42");
        assert!(result.is_err());

        if let Err(datafusion::error::DataFusionError::NotImplemented(msg)) = result {
            assert!(msg.contains("not yet fully implemented"));
        } else {
            panic!("Expected NotImplemented error");
        }
    }
}
