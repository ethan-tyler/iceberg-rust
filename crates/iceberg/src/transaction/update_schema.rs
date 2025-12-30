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

//! Schema evolution action for adding, renaming, and dropping columns.

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::{NestedField, Schema, Type};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum SchemaUpdateOperation {
    AddColumn { name: String, field_type: Type },
    RenameColumn { name: String, new_name: String },
    DropColumn { name: String },
}

/// Transactional schema update action.
///
/// Currently supports top-level ADD, RENAME, and DROP column operations.
pub struct UpdateSchemaAction {
    operations: Vec<SchemaUpdateOperation>,
}

impl UpdateSchemaAction {
    /// Create a new schema update action.
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add an optional column to the schema.
    pub fn add_column(mut self, name: impl Into<String>, field_type: Type) -> Self {
        self.operations.push(SchemaUpdateOperation::AddColumn {
            name: name.into(),
            field_type,
        });
        self
    }

    /// Rename a column in the schema.
    pub fn rename_column(mut self, name: impl Into<String>, new_name: impl Into<String>) -> Self {
        self.operations.push(SchemaUpdateOperation::RenameColumn {
            name: name.into(),
            new_name: new_name.into(),
        });
        self
    }

    /// Drop a column from the schema.
    pub fn drop_column(mut self, name: impl Into<String>) -> Self {
        self.operations
            .push(SchemaUpdateOperation::DropColumn { name: name.into() });
        self
    }

    fn apply_operations(&self, table: &Table) -> Result<Schema> {
        if self.operations.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No schema updates specified",
            ));
        }

        let current_schema = table.metadata().current_schema();
        let mut fields = current_schema.as_struct().fields().to_vec();
        let mut next_field_id = table.metadata().last_column_id();

        for op in &self.operations {
            match op {
                SchemaUpdateOperation::AddColumn { name, field_type } => {
                    ensure_top_level_name(name)?;
                    if find_field_index(&fields, name).is_some() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Column '{name}' already exists"),
                        ));
                    }
                    next_field_id += 1;
                    let field = NestedField::optional(next_field_id, name, field_type.clone());
                    fields.push(Arc::new(field));
                }
                SchemaUpdateOperation::RenameColumn { name, new_name } => {
                    ensure_top_level_name(name)?;
                    ensure_top_level_name(new_name)?;
                    if name == new_name {
                        continue;
                    }
                    let Some(index) = find_field_index(&fields, name) else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Column '{name}' does not exist"),
                        ));
                    };
                    if find_field_index(&fields, new_name).is_some() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Column '{new_name}' already exists"),
                        ));
                    }
                    let mut updated_field = (*fields[index]).clone();
                    updated_field.name = new_name.clone();
                    fields[index] = Arc::new(updated_field);
                }
                SchemaUpdateOperation::DropColumn { name } => {
                    ensure_top_level_name(name)?;
                    let Some(index) = find_field_index(&fields, name) else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Column '{name}' does not exist"),
                        ));
                    };
                    fields.remove(index);
                }
            }
        }

        let schema_clone: Schema = current_schema.as_ref().clone();
        let new_schema = schema_clone.into_builder().replace_fields(fields).build()?;

        if new_schema.is_same_schema(current_schema) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Schema update did not change the schema",
            ));
        }

        Ok(new_schema)
    }
}

fn find_field_index(fields: &[Arc<NestedField>], name: &str) -> Option<usize> {
    fields.iter().position(|field| field.name == name)
}

fn ensure_top_level_name(name: &str) -> Result<()> {
    if name.contains('.') {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Nested field paths are not supported: '{name}'"),
        ));
    }
    Ok(())
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let new_schema = self.apply_operations(table)?;
        let current_schema_id = table.metadata().current_schema_id();
        let last_assigned_field_id = table.metadata().last_column_id();

        let updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch { current_schema_id },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}
