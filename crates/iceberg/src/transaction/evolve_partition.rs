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

//! Partition evolution action for Iceberg transactions.
//!
//! This module provides [`EvolvePartitionAction`] for evolving a table's partition spec
//! as part of a transaction. Partition evolution allows changing how a table is partitioned
//! over time while preserving existing data files under their original partition specs.
//!
//! # Example
//!
//! ```ignore
//! use iceberg::spec::UnboundPartitionSpec;
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! // Create a new partition spec
//! let new_spec = UnboundPartitionSpec::builder()
//!     .add_partition_field(1, "year", Transform::Year)?
//!     .build();
//!
//! // Evolve the partition spec
//! let tx = Transaction::new(&table);
//! let action = tx.evolve_partition_spec()
//!     .add_spec(new_spec)
//!     .set_default();
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::UnboundPartitionSpec;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transactional action that evolves a table's partition spec.
///
/// This action supports:
/// - Adding a new partition spec to the table
/// - Setting the new spec as the default (for new writes)
///
/// After evolution, existing data files retain their original partition spec,
/// while new writes use the updated default spec.
pub struct EvolvePartitionAction {
    spec: Option<UnboundPartitionSpec>,
    set_as_default: bool,
}

impl EvolvePartitionAction {
    /// Creates a new [`EvolvePartitionAction`].
    pub fn new() -> Self {
        Self {
            spec: None,
            set_as_default: false,
        }
    }

    /// Adds a partition spec to the table.
    ///
    /// The spec will be bound to the table's current schema when committed.
    ///
    /// # Arguments
    ///
    /// * `spec` - The unbound partition spec to add
    pub fn add_spec(mut self, spec: UnboundPartitionSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    /// Sets the added partition spec as the default for new writes.
    ///
    /// This should be called after `add_spec()`. If no spec has been added,
    /// this flag is ignored.
    pub fn set_default(mut self) -> Self {
        self.set_as_default = true;
        self
    }
}

impl Default for EvolvePartitionAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for EvolvePartitionAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        // Validate: set_default without add_spec is likely a mistake
        if self.set_as_default && self.spec.is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot set default partition spec without adding a spec first. \
                 Call add_spec() before set_default().",
            ));
        }

        let mut updates = Vec::new();

        if let Some(spec) = &self.spec {
            updates.push(TableUpdate::AddSpec { spec: spec.clone() });

            if self.set_as_default {
                // -1 means "last added spec"
                updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
            }
        }

        Ok(ActionCommit::new(updates, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use as_any::Downcast;

    use super::*;
    use crate::spec::Transform;
    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_table;

    #[test]
    fn test_evolve_partition_add_spec() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let new_spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "x_bucket", Transform::Bucket(16))
            .unwrap()
            .build();

        let tx = tx
            .evolve_partition_spec()
            .add_spec(new_spec.clone())
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<EvolvePartitionAction>()
            .unwrap();
        assert!(action.spec.is_some());
        assert!(!action.set_as_default);
    }

    #[test]
    fn test_evolve_partition_add_and_set_default() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let new_spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "x_bucket", Transform::Bucket(16))
            .unwrap()
            .build();

        let tx = tx
            .evolve_partition_spec()
            .add_spec(new_spec)
            .set_default()
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<EvolvePartitionAction>()
            .unwrap();
        assert!(action.spec.is_some());
        assert!(action.set_as_default);
    }

    #[test]
    fn test_evolve_partition_no_op() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let tx = tx.evolve_partition_spec().apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<EvolvePartitionAction>()
            .unwrap();
        assert!(action.spec.is_none());
    }

    #[tokio::test]
    async fn test_evolve_partition_set_default_without_spec_fails() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // set_default() without add_spec() should fail at commit time
        let tx = tx.evolve_partition_spec().set_default().apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = Arc::clone(&tx.actions[0]);
        let result = action.commit(&table).await;

        match result {
            Ok(_) => panic!("Expected error when set_default() called without add_spec()"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("Cannot set default partition spec without adding a spec"),
                    "Expected error about missing spec, got: {err}",
                );
            }
        }
    }
}
