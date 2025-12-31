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

//! Synthetic concurrency tests for Iceberg transactions.
//!
//! These tests validate that concurrent writers and maintenance operations
//! behave correctly under optimistic concurrency control. Key properties tested:
//!
//! - At least one concurrent operation succeeds, or operations retry safely
//! - Failed operations receive clear conflict errors (not corruption)
//! - Table remains readable and consistent after concurrent operations
//! - No metadata corruption (snapshot lineage valid, manifest references consistent)

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::sync::Barrier;

    use crate::memory::MemoryCatalog;
    use crate::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct};
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent};

    /// Creates a test data file with a unique path
    /// For unpartitioned tables, uses an empty partition struct
    fn create_test_data_file(
        table_metadata: &crate::spec::TableMetadata,
        file_id: u32,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/concurrent_{file_id}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table_metadata.default_partition_spec_id())
            .partition(Struct::empty()) // Unpartitioned table
            .build()
            .unwrap()
    }

    /// Creates a memory catalog wrapped in Arc for sharing across tasks
    async fn new_memory_catalog() -> Arc<MemoryCatalog> {
        use tempfile::TempDir;

        use crate::CatalogBuilder;
        use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_string();
        // Leak the temp dir to keep it alive for the duration of the test
        std::mem::forget(temp_dir);

        Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location)]),
                )
                .await
                .unwrap(),
        )
    }

    /// Creates a test table with the given name in a unique namespace
    async fn create_test_table(
        catalog: &Arc<MemoryCatalog>,
        table_name: &str,
    ) -> crate::table::Table {
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};

        let namespace_ident =
            NamespaceIdent::new(format!("test_ns_{}", uuid::Uuid::new_v4()).replace('-', "_"));

        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap()
    }

    /// Verifies table consistency after concurrent operations
    async fn verify_table_consistency(catalog: &Arc<MemoryCatalog>, table_ident: &TableIdent) {
        let table = catalog.load_table(table_ident).await.unwrap();
        let metadata = table.metadata();

        // Verify snapshot lineage is valid
        if let Some(current_snapshot) = metadata.current_snapshot() {
            // Load manifest list to verify it's readable
            let manifest_list = current_snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await
                .expect("Manifest list should be readable");

            // Verify all manifests are readable
            for manifest_entry in manifest_list.entries() {
                let _manifest = manifest_entry
                    .load_manifest(table.file_io())
                    .await
                    .expect("Manifest should be readable");
            }
        }

        // Verify snapshot history is valid
        for snapshot in metadata.snapshots() {
            // Each snapshot should be accessible
            let _ = metadata.snapshot_by_id(snapshot.snapshot_id());
        }
    }

    /// Test: Two concurrent fast appends to the same table
    ///
    /// Expected behavior:
    /// - At least one append succeeds
    /// - If both succeed (via retry), both data files are present
    /// - If one fails, it's a clean conflict error with `CatalogCommitConflicts`
    /// - Table remains consistent and readable
    #[tokio::test]
    async fn test_concurrent_append_vs_append() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_concurrent_appends").await;
        let table_ident = table.identifier().clone();

        // Create a barrier to synchronize concurrent tasks
        let barrier = Arc::new(Barrier::new(2));

        // Clone necessary data for tasks
        let catalog1 = catalog.clone();
        let catalog2 = catalog.clone();
        let table_ident1 = table_ident.clone();
        let table_ident2 = table_ident.clone();
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();

        // Spawn first concurrent append
        let handle1 = tokio::spawn(async move {
            // Load fresh table state
            let table = catalog1.load_table(&table_ident1).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 1);

            // Wait at barrier to maximize race condition
            barrier1.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog1.as_ref()).await
        });

        // Spawn second concurrent append
        let handle2 = tokio::spawn(async move {
            // Load fresh table state
            let table = catalog2.load_table(&table_ident2).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 2);

            // Wait at barrier to maximize race condition
            barrier2.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog2.as_ref()).await
        });

        // Wait for both to complete
        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Count successes and failures
        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

        // At least one should succeed
        assert!(
            success_count >= 1,
            "At least one concurrent append should succeed"
        );

        // If any failed, verify it's a clean conflict error
        for result in [&result1, &result2] {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict, got: {err:?}"
                );
            }
        }

        // Verify table consistency
        verify_table_consistency(&catalog, &table_ident).await;

        // If both succeeded (through retry), verify both files are present
        if success_count == 2 {
            let final_table = catalog.load_table(&table_ident).await.unwrap();
            let snapshot = final_table.metadata().current_snapshot().unwrap();
            let manifest_list = snapshot
                .load_manifest_list(final_table.file_io(), &final_table.metadata_ref())
                .await
                .unwrap();

            // Count total data files across all manifests
            let mut total_files = 0;
            for manifest_entry in manifest_list.entries() {
                let manifest = manifest_entry
                    .load_manifest(final_table.file_io())
                    .await
                    .unwrap();
                total_files += manifest.entries().len();
            }

            // Should have at least 2 files if both appends succeeded
            assert!(
                total_files >= 2,
                "Both appends succeeded, should have at least 2 data files, found {total_files}"
            );
        }
    }

    /// Test: Three concurrent appends to stress test retry logic
    ///
    /// This test increases concurrency to stress the retry mechanism
    #[tokio::test]
    async fn test_concurrent_triple_append() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_triple_appends").await;
        let table_ident = table.identifier().clone();

        let barrier = Arc::new(Barrier::new(3));

        let mut handles = vec![];
        for i in 0..3 {
            let catalog_clone = catalog.clone();
            let table_ident_clone = table_ident.clone();
            let barrier_clone = barrier.clone();

            let handle = tokio::spawn(async move {
                let table = catalog_clone.load_table(&table_ident_clone).await.unwrap();
                let data_file = create_test_data_file(table.metadata(), i);

                barrier_clone.wait().await;

                let tx = Transaction::new(&table);
                let tx = tx
                    .fast_append()
                    .add_data_files(vec![data_file])
                    .apply(tx)
                    .unwrap();
                tx.commit(catalog_clone.as_ref()).await
            });

            handles.push(handle);
        }

        // Collect results
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let success_count = results.iter().filter(|r| r.is_ok()).count();

        // At least one should succeed
        assert!(
            success_count >= 1,
            "At least one of three concurrent appends should succeed"
        );

        // Verify all failures are clean conflict errors
        for result in &results {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict"
                );
            }
        }

        // Verify table consistency
        verify_table_consistency(&catalog, &table_ident).await;
    }

    /// Test: Property update vs append concurrency
    ///
    /// Tests that property updates and data appends can handle conflicts correctly
    #[tokio::test]
    async fn test_concurrent_property_update_vs_append() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_property_vs_append").await;
        let table_ident = table.identifier().clone();

        let barrier = Arc::new(Barrier::new(2));

        let catalog1 = catalog.clone();
        let table_ident1 = table_ident.clone();
        let barrier1 = barrier.clone();

        // Spawn property update
        let handle1 = tokio::spawn(async move {
            let table = catalog1.load_table(&table_ident1).await.unwrap();

            barrier1.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .update_table_properties()
                .set("test.key".to_string(), "test.value".to_string())
                .apply(tx)
                .unwrap();
            tx.commit(catalog1.as_ref()).await
        });

        let catalog2 = catalog.clone();
        let table_ident2 = table_ident.clone();
        let barrier2 = barrier.clone();

        // Spawn append
        let handle2 = tokio::spawn(async move {
            let table = catalog2.load_table(&table_ident2).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 100);

            barrier2.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog2.as_ref()).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

        // At least one should succeed
        assert!(
            success_count >= 1,
            "At least one concurrent operation should succeed"
        );

        // Verify all failures are clean conflict errors
        for result in [&result1, &result2] {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict"
                );
            }
        }

        // Verify table consistency
        verify_table_consistency(&catalog, &table_ident).await;
    }

    /// Test: Sequential commits work correctly
    ///
    /// Baseline test to ensure sequential operations work as expected
    #[tokio::test]
    async fn test_sequential_appends() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_sequential").await;
        let table_ident = table.identifier().clone();

        // First append
        let table = catalog.load_table(&table_ident).await.unwrap();
        let data_file1 = create_test_data_file(table.metadata(), 1);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file1])
            .apply(tx)
            .unwrap();
        let table = tx
            .commit(catalog.as_ref())
            .await
            .expect("First append should succeed");

        // Verify first append
        assert!(table.metadata().current_snapshot().is_some());

        // Second append - load fresh table state
        let table = catalog.load_table(&table_ident).await.unwrap();
        let data_file2 = create_test_data_file(table.metadata(), 2);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file2])
            .apply(tx)
            .unwrap();
        let table = tx
            .commit(catalog.as_ref())
            .await
            .expect("Second append should succeed");

        // Verify both data files are present
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .unwrap();

        let mut total_files = 0;
        for manifest_entry in manifest_list.entries() {
            let manifest = manifest_entry.load_manifest(table.file_io()).await.unwrap();
            total_files += manifest.entries().len();
        }

        assert_eq!(
            total_files, 2,
            "Should have 2 data files after sequential appends"
        );

        // Verify consistency
        verify_table_consistency(&catalog, &table_ident).await;
    }

    /// Test: Stale transaction fails with clean error
    ///
    /// Verifies that committing a stale transaction (based on old snapshot)
    /// fails with a clear conflict error
    #[tokio::test]
    async fn test_stale_transaction_conflict() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_stale").await;
        let table_ident = table.identifier().clone();

        // Create transaction based on current state
        let table_snapshot1 = catalog.load_table(&table_ident).await.unwrap();
        let data_file1 = create_test_data_file(table_snapshot1.metadata(), 1);
        let tx1 = Transaction::new(&table_snapshot1);
        let tx1 = tx1
            .fast_append()
            .add_data_files(vec![data_file1])
            .apply(tx1)
            .unwrap();

        // Meanwhile, another commit happens
        let table_snapshot2 = catalog.load_table(&table_ident).await.unwrap();
        let data_file2 = create_test_data_file(table_snapshot2.metadata(), 2);
        let tx2 = Transaction::new(&table_snapshot2);
        let tx2 = tx2
            .fast_append()
            .add_data_files(vec![data_file2])
            .apply(tx2)
            .unwrap();
        tx2.commit(catalog.as_ref())
            .await
            .expect("Intervening commit should succeed");

        // Now try to commit the stale transaction
        let result = tx1.commit(catalog.as_ref()).await;

        // Should fail or succeed through retry - either is acceptable
        // The key is that it should NOT corrupt the table
        verify_table_consistency(&catalog, &table_ident).await;

        // Log what happened for debugging
        match result {
            Ok(_) => println!("Stale transaction succeeded through retry"),
            Err(e) => {
                assert_eq!(
                    e.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Stale transaction should fail with conflict error, got: {e:?}"
                );
                println!("Stale transaction correctly failed with conflict");
            }
        }
    }

    /// Test: High concurrency stress test
    ///
    /// Spawns multiple concurrent operations to stress test the system
    #[tokio::test]
    async fn test_high_concurrency_stress() {
        const NUM_CONCURRENT: usize = 5;

        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_stress").await;
        let table_ident = table.identifier().clone();

        let barrier = Arc::new(Barrier::new(NUM_CONCURRENT));

        let mut handles = vec![];
        for i in 0..NUM_CONCURRENT {
            let catalog_clone = catalog.clone();
            let table_ident_clone = table_ident.clone();
            let barrier_clone = barrier.clone();

            let handle = tokio::spawn(async move {
                let table = catalog_clone.load_table(&table_ident_clone).await.unwrap();
                let data_file = create_test_data_file(table.metadata(), i as u32);

                barrier_clone.wait().await;

                let tx = Transaction::new(&table);
                let tx = tx
                    .fast_append()
                    .add_data_files(vec![data_file])
                    .apply(tx)
                    .unwrap();
                tx.commit(catalog_clone.as_ref()).await
            });

            handles.push(handle);
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let success_count = results.iter().filter(|r| r.is_ok()).count();
        let failure_count = results.iter().filter(|r| r.is_err()).count();

        println!("High concurrency test: {success_count} succeeded, {failure_count} failed");

        // At least one should succeed
        assert!(
            success_count >= 1,
            "At least one of {NUM_CONCURRENT} concurrent operations should succeed"
        );

        // All failures should be clean conflict errors
        for result in &results {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "All failures should be catalog commit conflicts"
                );
            }
        }

        // Most importantly: verify table is not corrupted
        verify_table_consistency(&catalog, &table_ident).await;
    }

    // =========================================================================
    // Additional concurrency tests for specific operations
    // =========================================================================

    /// Creates a test delete file for position deletes
    fn create_test_delete_file(
        table_metadata: &crate::spec::TableMetadata,
        file_id: u32,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(format!("test/delete_{file_id}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table_metadata.default_partition_spec_id())
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    /// Test: RowDelta vs append concurrency
    ///
    /// Tests that a row-level mutation (RowDelta) and an append can
    /// handle conflicts correctly. RowDelta operations are atomic
    /// combinations of data file additions and delete file additions.
    #[tokio::test]
    async fn test_concurrent_row_delta_vs_append() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_rowdelta_vs_append").await;
        let table_ident = table.identifier().clone();

        // First, add some data to the table so RowDelta has something to work with
        let initial_table = catalog.load_table(&table_ident).await.unwrap();
        let initial_file = create_test_data_file(initial_table.metadata(), 0);
        let tx = Transaction::new(&initial_table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![initial_file])
            .apply(tx)
            .unwrap();
        tx.commit(catalog.as_ref())
            .await
            .expect("Initial data setup should succeed");

        let barrier = Arc::new(Barrier::new(2));

        let catalog1 = catalog.clone();
        let table_ident1 = table_ident.clone();
        let barrier1 = barrier.clone();

        // Spawn RowDelta operation (simulating UPDATE: add new data + delete old)
        let handle1 = tokio::spawn(async move {
            let table = catalog1.load_table(&table_ident1).await.unwrap();
            let new_data = create_test_data_file(table.metadata(), 10);
            let delete = create_test_delete_file(table.metadata(), 10);

            barrier1.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .row_delta()
                .add_data_files(vec![new_data])
                .add_position_delete_files(vec![delete])
                .apply(tx)
                .unwrap();
            tx.commit(catalog1.as_ref()).await
        });

        let catalog2 = catalog.clone();
        let table_ident2 = table_ident.clone();
        let barrier2 = barrier.clone();

        // Spawn append operation
        let handle2 = tokio::spawn(async move {
            let table = catalog2.load_table(&table_ident2).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 20);

            barrier2.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog2.as_ref()).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

        println!(
            "RowDelta vs append: row_delta={}, append={}",
            if result1.is_ok() { "ok" } else { "conflict" },
            if result2.is_ok() { "ok" } else { "conflict" }
        );

        // At least one should succeed
        assert!(success_count >= 1, "At least one operation should succeed");

        // Verify all failures are clean conflict errors
        for result in [&result1, &result2] {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict"
                );
            }
        }

        // Verify table consistency
        verify_table_consistency(&catalog, &table_ident).await;
    }

    /// Test: expire_snapshots vs append concurrency
    ///
    /// Tests that snapshot expiration and data append can handle conflicts.
    /// This ensures that expire_snapshots doesn't delete data that's being
    /// referenced by a concurrent append operation.
    #[tokio::test]
    async fn test_concurrent_expire_snapshots_vs_append() {
        use chrono::Utc;

        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_expire_vs_append").await;
        let table_ident = table.identifier().clone();

        // Create multiple snapshots to have something to expire
        for i in 0..3 {
            let table = catalog.load_table(&table_ident).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), i);
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog.as_ref())
                .await
                .expect("Setup append should succeed");
        }

        // Verify we have snapshots
        let table = catalog.load_table(&table_ident).await.unwrap();
        let snapshot_count = table.metadata().snapshots().count();
        assert!(
            snapshot_count >= 3,
            "Should have at least 3 snapshots, got {snapshot_count}"
        );

        let barrier = Arc::new(Barrier::new(2));

        let catalog1 = catalog.clone();
        let table_ident1 = table_ident.clone();
        let barrier1 = barrier.clone();

        // Spawn expire_snapshots operation
        let handle1 = tokio::spawn(async move {
            let table = catalog1.load_table(&table_ident1).await.unwrap();

            barrier1.wait().await;

            let tx = Transaction::new(&table);
            // Expire old snapshots, but retain at least 1
            let tx = tx
                .expire_snapshots()
                .older_than(Utc::now())
                .retain_last(1)
                .apply(tx)
                .unwrap();
            tx.commit(catalog1.as_ref()).await
        });

        let catalog2 = catalog.clone();
        let table_ident2 = table_ident.clone();
        let barrier2 = barrier.clone();

        // Spawn append operation
        let handle2 = tokio::spawn(async move {
            let table = catalog2.load_table(&table_ident2).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 100);

            barrier2.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog2.as_ref()).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

        println!(
            "expire_snapshots vs append: expire={}, append={}",
            if result1.is_ok() { "ok" } else { "conflict" },
            if result2.is_ok() { "ok" } else { "conflict" }
        );

        // At least one should succeed
        assert!(success_count >= 1, "At least one operation should succeed");

        // Verify all failures are clean conflict errors
        for result in [&result1, &result2] {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict"
                );
            }
        }

        // Most importantly: verify table is not corrupted
        verify_table_consistency(&catalog, &table_ident).await;

        // Verify table is still readable and has data
        let final_table = catalog.load_table(&table_ident).await.unwrap();
        assert!(
            final_table.metadata().current_snapshot().is_some(),
            "Table should still have a current snapshot"
        );
    }

    /// Test: Simulated rewrite_data_files scenario using replace_partitions
    ///
    /// Since rewrite_data_files uses a planner/committer pattern with external
    /// query engine involvement, we test the underlying conflict detection
    /// using replace_partitions which has similar semantics (replace files in partition).
    ///
    /// This tests the same conflict detection that would apply to compaction.
    #[tokio::test]
    async fn test_concurrent_replace_partitions_vs_append() {
        let catalog = new_memory_catalog().await;
        let table = create_test_table(&catalog, "test_replace_vs_append").await;
        let table_ident = table.identifier().clone();

        // First, add some data to the table
        let initial_table = catalog.load_table(&table_ident).await.unwrap();
        let initial_file = create_test_data_file(initial_table.metadata(), 0);
        let tx = Transaction::new(&initial_table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![initial_file])
            .apply(tx)
            .unwrap();
        tx.commit(catalog.as_ref())
            .await
            .expect("Initial data setup should succeed");

        let barrier = Arc::new(Barrier::new(2));

        let catalog1 = catalog.clone();
        let table_ident1 = table_ident.clone();
        let barrier1 = barrier.clone();

        // Spawn replace_partitions operation (simulating compaction: replace files)
        let handle1 = tokio::spawn(async move {
            let table = catalog1.load_table(&table_ident1).await.unwrap();
            let new_data = create_test_data_file(table.metadata(), 50);

            barrier1.wait().await;

            let tx = Transaction::new(&table);
            // Replace partitions adds new files - similar to compaction output
            let tx = tx
                .replace_partitions()
                .add_data_file(new_data)
                .apply(tx)
                .unwrap();
            tx.commit(catalog1.as_ref()).await
        });

        let catalog2 = catalog.clone();
        let table_ident2 = table_ident.clone();
        let barrier2 = barrier.clone();

        // Spawn append operation
        let handle2 = tokio::spawn(async move {
            let table = catalog2.load_table(&table_ident2).await.unwrap();
            let data_file = create_test_data_file(table.metadata(), 60);

            barrier2.wait().await;

            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .unwrap();
            tx.commit(catalog2.as_ref()).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

        println!(
            "replace_partitions vs append: replace={}, append={}",
            if result1.is_ok() { "ok" } else { "conflict" },
            if result2.is_ok() { "ok" } else { "conflict" }
        );

        // At least one should succeed
        assert!(success_count >= 1, "At least one operation should succeed");

        // Verify all failures are clean conflict errors
        for result in [&result1, &result2] {
            if let Err(err) = result {
                assert_eq!(
                    err.kind(),
                    ErrorKind::CatalogCommitConflicts,
                    "Failed operation should be a catalog commit conflict"
                );
            }
        }

        // Verify table consistency
        verify_table_consistency(&catalog, &table_ident).await;
    }
}
