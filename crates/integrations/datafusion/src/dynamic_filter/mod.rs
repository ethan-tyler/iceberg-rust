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

//! Dynamic Partition Pruning for Iceberg table scans.
//!
//! This module enables runtime filter pushdown from DataFusion hash joins to Iceberg
//! table scans, allowing partition and file pruning based on runtime join build-side values.
//!
//! # How It Works
//!
//! 1. DataFusion's hash join builds a filter (bloom filter or IN-list) from the build side
//! 2. During post-optimization, this filter is pushed down as [`DynamicFilterPhysicalExpr`]
//! 3. [`IcebergTableScan`] receives the filter via [`handle_child_pushdown_result`]
//! 4. The [`DynamicFilterHandler`] converts it to an Iceberg [`Predicate`]
//! 5. Iceberg's scan planner uses the predicate for partition/file pruning
//!
//! # Supported Filter Types
//!
//! - Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
//! - IN lists (up to 1000 items to prevent pathological cases)
//! - IS NULL / IS NOT NULL
//! - Logical AND/OR (partial conversion supported - unconvertible parts are dropped)
//! - NOT expressions
//!
//! # Supported Data Types
//!
//! - Boolean, Int, Long, Float, Double
//! - String, Binary, Fixed, UUID
//! - Date, Time, Timestamp, TimestampTz (with timezone awareness)
//! - Decimal (with precision/scale matching)
//!
//! # Limitations
//!
//! - Only primitive column types (nested types not supported)
//! - Only filterable columns (partition columns or columns with statistics)
//! - Single dynamic filter per scan (first valid one is accepted)
//! - IN-list limited to 1000 items
//!
//! # Thread Safety
//!
//! The [`DynamicFilterHandler`] is thread-safe and handles concurrent access via:
//! - `AtomicU64` for generation tracking with proper Acquire/Release ordering
//! - `RwLock` for cached predicate with double-check locking pattern
//!
//! # Performance Characteristics
//!
//! - Cached predicates avoid repeated conversion
//! - Task caching avoids re-planning when dynamic filter hasn't changed
//! - Generation-based invalidation ensures correctness under concurrent updates
//!
//! [`DynamicFilterPhysicalExpr`]: datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr
//! [`IcebergTableScan`]: crate::physical_plan::scan::IcebergTableScan
//! [`handle_child_pushdown_result`]: datafusion::physical_plan::ExecutionPlan::handle_child_pushdown_result
//! [`DynamicFilterHandler`]: handler::DynamicFilterHandler
//! [`Predicate`]: iceberg::expr::Predicate

pub(crate) mod handler;
