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

//! Expression conversion utilities for the Iceberg DataFusion integration.
//!
//! This module converts DataFusion physical expressions ([`PhysicalExpr`]) into Iceberg
//! [`Predicate`]s so they can be applied for file / manifest pruning.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use iceberg::Result;
use iceberg::expr::Predicate;
use iceberg::spec::Schema;

mod converter;
mod literal;

/// Converts a DataFusion physical expression into an Iceberg [`Predicate`].
///
/// Returns `Predicate::AlwaysTrue` when the expression cannot be safely converted.
pub fn convert_physical_expr_to_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Predicate> {
    converter::convert_physical_expr_to_predicate(expr, schema)
}
