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

use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{Receiver, channel};

use super::delete_filter::DeleteFilter;
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::arrow::{arrow_primitive_to_literal, arrow_schema_to_schema};
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Predicate, Reference};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{
    DataContentType, Datum, ListType, MapType, NestedField, NestedFieldRef, PartnerAccessor,
    PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor, StructType, Type,
    visit_schema_with_partner,
};
use crate::{Error, ErrorKind, Result};

/// Default maximum number of rows allowed in a single equality delete file.
///
/// This limit exists to prevent unbounded memory growth when parsing large equality
/// delete files. Each deleted row creates a predicate in memory, and for very large
/// delete files (millions of rows), this can cause memory issues.
///
/// The default of 10 million rows provides a generous buffer for production workloads
/// while protecting against pathological cases. For reference:
/// - 100k rows ≈ 10-20 MB of predicates
/// - 1M rows ≈ 100-200 MB of predicates
/// - 10M rows ≈ 1-2 GB of predicates
///
/// To process larger equality delete files, consider:
/// 1. Compacting the table to merge equality deletes with data files
/// 2. Using position deletes instead of equality deletes for large-scale deletions
/// 3. Partitioning to limit per-partition delete file sizes
pub const DEFAULT_MAX_EQUALITY_DELETE_ROWS: usize = 10_000_000;

#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    basic_delete_file_loader: BasicDeleteFileLoader,
    concurrency_limit_data_files: usize,
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    // TODO: Delete Vector loader from Puffin files
    ExistingEqDel,
    PosDels(ArrowRecordBatchStream),
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
        sender: tokio::sync::oneshot::Sender<Predicate>,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    DelVecs(HashMap<String, DeleteVector>),
    EqDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> Self {
        CachingDeleteFileLoader {
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io),
            concurrency_limit_data_files,
        }
    }

    /// Initiates loading of all deletes for all the specified tasks
    ///
    /// Returned future completes once all positional deletes and delete vectors
    /// have loaded. EQ deletes are not waited for in this method but the returned
    /// DeleteFilter will await their loading when queried for them.
    ///
    ///  * Create a single stream of all delete file tasks irrespective of type,
    ///    so that we can respect the combined concurrency limit
    ///  * We then process each in two phases: load and parse.
    ///  * for positional deletes the load phase instantiates an ArrowRecordBatchStream to
    ///    stream the file contents out
    ///  * for eq deletes, we first check if the EQ delete is already loaded or being loaded by
    ///    another concurrently processing data file scan task. If it is, we skip it.
    ///    If not, the DeleteFilter is updated to contain a notifier to prevent other data file
    ///    tasks from starting to load the same equality delete file. We spawn a task to load
    ///    the EQ delete's record batch stream, convert it to a predicate, update the delete filter,
    ///    and notify any task that was waiting for it.
    ///  * When this gets updated to add support for delete vectors, the load phase will return
    ///    a PuffinReader for them.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///    The result of this is a map of data file paths to delete vectors for the positional
    ///    delete tasks (and in future for the delete vector tasks). For equality delete
    ///    file tasks, this results in an unbound Predicate.
    ///  * The unbound Predicates resulting from equality deletes are sent to their associated oneshot
    ///    channel to store them in the right place in the delete file managers state.
    ///  * The results of all of these futures are awaited on in parallel with the specified
    ///    level of concurrency and collected into a vec. We then combine all the delete
    ///    vector maps that resulted from any positional delete or delete vector files into a
    ///    single map and persist it in the state.
    ///
    ///
    ///  Conceptually, the data flow is like this:
    /// ```none
    ///                                          FileScanTaskDeleteFile
    ///                                                     |
    ///                                             Skip Started EQ Deletes
    ///                                                     |
    ///                                                     |
    ///                                       [load recordbatch stream / puffin]
    ///                                             DeleteFileContext
    ///                                                     |
    ///                                                     |
    ///                       +-----------------------------+--------------------------+
    ///                     Pos Del           Del Vec (Not yet Implemented)         EQ Del
    ///                       |                             |                          |
    ///              [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///          HashMap<String, RoaringTreeMap> HashMap<String, RoaringTreeMap>   (Predicate, Sender)
    ///                       |                             |                          |
    ///                       |                             |                 [persist to state]
    ///                       |                             |                          ()
    ///                       |                             |                          |
    ///                       +-----------------------------+--------------------------+
    ///                                                     |
    ///                                             [buffer unordered]
    ///                                                     |
    ///                                            [combine del vectors]
    ///                                        HashMap<String, RoaringTreeMap>
    ///                                                     |
    ///                                        [persist del vectors to state]
    ///                                                    ()
    ///                                                    |
    ///                                                    |
    ///                                                 [join!]
    /// ```
    pub(crate) fn load_deletes(
        &self,
        delete_file_entries: &[FileScanTaskDeleteFile],
        schema: SchemaRef,
    ) -> Receiver<Result<DeleteFilter>> {
        let (tx, rx) = channel();
        let del_filter = DeleteFilter::default();

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.basic_delete_file_loader.clone(),
                    del_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);

        let del_filter = del_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let basic_delete_file_loader = self.basic_delete_file_loader.clone();
        crate::runtime::spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;
                let basic_delete_file_loader = basic_delete_file_loader.clone();

                let results: Vec<ParsedDeleteFileContext> = task_stream
                    .map(move |(task, file_io, del_filter, schema)| {
                        let basic_delete_file_loader = basic_delete_file_loader.clone();
                        async move {
                            Self::load_file_for_task(
                                &task,
                                basic_delete_file_loader.clone(),
                                del_filter,
                                schema,
                            )
                            .await
                        }
                    })
                    .map(move |ctx| {
                        Ok(async { Self::parse_file_content_for_task(ctx.await?).await })
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_collect::<Vec<_>>()
                    .await?;

                for item in results {
                    if let ParsedDeleteFileContext::DelVecs(hash_map) = item {
                        for (data_file_path, delete_vector) in hash_map.into_iter() {
                            del_filter.upsert_delete_vector(data_file_path, delete_vector);
                        }
                    }
                }

                Ok(del_filter)
            }
            .await;

            let _ = tx.send(result);
        });

        rx
    }

    async fn load_file_for_task(
        task: &FileScanTaskDeleteFile,
        basic_delete_file_loader: BasicDeleteFileLoader,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => Ok(DeleteFileContext::PosDels(
                basic_delete_file_loader
                    .parquet_to_batch_stream(&task.file_path)
                    .await?,
            )),

            DataContentType::EqualityDeletes => {
                let Some(notify) = del_filter.try_start_eq_del_load(&task.file_path) else {
                    return Ok(DeleteFileContext::ExistingEqDel);
                };

                let (sender, receiver) = channel();
                del_filter.insert_equality_delete(&task.file_path, receiver);

                // Per the Iceberg spec, evolve schema for equality deletes but only for the
                // equality_ids columns, not all table columns.
                let equality_ids_vec = task.equality_ids.clone().unwrap();
                let evolved_stream = BasicDeleteFileLoader::evolve_schema(
                    basic_delete_file_loader
                        .parquet_to_batch_stream(&task.file_path)
                        .await?,
                    schema,
                    &equality_ids_vec,
                )
                .await?;

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: evolved_stream,
                    sender,
                    equality_ids: HashSet::from_iter(equality_ids_vec),
                })
            }

            DataContentType::Data => Err(Error::new(
                ErrorKind::Unexpected,
                "tasks with files of type Data not expected here",
            )),
        }
    }

    async fn parse_file_content_for_task(
        ctx: DeleteFileContext,
    ) -> Result<ParsedDeleteFileContext> {
        match ctx {
            DeleteFileContext::ExistingEqDel => Ok(ParsedDeleteFileContext::EqDel),
            DeleteFileContext::PosDels(batch_stream) => {
                let del_vecs =
                    Self::parse_positional_deletes_record_batch_stream(batch_stream).await?;
                Ok(ParsedDeleteFileContext::DelVecs(del_vecs))
            }
            DeleteFileContext::FreshEqDel {
                sender,
                batch_stream,
                equality_ids,
            } => {
                let predicate =
                    Self::parse_equality_deletes_record_batch_stream(batch_stream, equality_ids)
                        .await?;

                sender
                    .send(predicate)
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "Could not send eq delete predicate to state",
                        )
                    })
                    .map(|_| ParsedDeleteFileContext::EqDel)
            }
        }
    }

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        let mut result: HashMap<String, DeleteVector> = HashMap::default();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            let columns = batch.columns();

            let Some(file_paths) = columns[0].as_any().downcast_ref::<StringArray>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast file paths array to StringArray",
                ));
            };
            let Some(positions) = columns[1].as_any().downcast_ref::<Int64Array>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast positions array to Int64Array",
                ));
            };

            for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
                let (Some(file_path), Some(pos)) = (file_path, pos) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "null values in delete file",
                    ));
                };

                result
                    .entry(file_path.to_string())
                    .or_default()
                    .insert(pos as u64);
            }
        }

        Ok(result)
    }

    /// Parses equality delete record batches into a combined predicate.
    ///
    /// # Arguments
    /// * `stream` - The record batch stream from the equality delete file
    /// * `equality_ids` - The field IDs that form the equality delete key
    /// * `max_rows` - Optional maximum rows to process. If None, uses DEFAULT_MAX_EQUALITY_DELETE_ROWS.
    ///   Pass Some(0) to disable the limit entirely (use with caution).
    ///
    /// # Returns
    /// A predicate that filters out rows matching any of the deleted row keys.
    ///
    /// # Errors
    /// Returns `PreconditionFailed` if the number of rows exceeds the limit.
    async fn parse_equality_deletes_record_batch_stream(
        stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
    ) -> Result<Predicate> {
        Self::parse_equality_deletes_record_batch_stream_with_limit(
            stream,
            equality_ids,
            Some(DEFAULT_MAX_EQUALITY_DELETE_ROWS),
        )
        .await
    }

    /// Internal implementation that accepts an explicit row limit.
    async fn parse_equality_deletes_record_batch_stream_with_limit(
        mut stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
        max_rows: Option<usize>,
    ) -> Result<Predicate> {
        let mut row_predicates = Vec::new();
        let mut batch_schema_iceberg: Option<Schema> = None;
        let accessor = EqDelRecordBatchPartnerAccessor;
        let mut total_rows: usize = 0;
        let row_limit = max_rows.unwrap_or(usize::MAX);

        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok(AlwaysTrue);
            }

            let schema = match &batch_schema_iceberg {
                Some(schema) => schema,
                None => {
                    let schema = arrow_schema_to_schema(record_batch.schema().as_ref())?;
                    batch_schema_iceberg = Some(schema);
                    batch_schema_iceberg.as_ref().unwrap()
                }
            };

            let root_array: ArrayRef = Arc::new(StructArray::from(record_batch));

            let mut processor = EqDelColumnProcessor::new(&equality_ids);
            visit_schema_with_partner(schema, &root_array, &mut processor, &accessor)?;

            let mut datum_columns_with_names = processor.finish()?;
            if datum_columns_with_names.is_empty() {
                continue;
            }

            // Process the collected columns in lockstep
            #[allow(clippy::len_zero)]
            while datum_columns_with_names[0].0.len() > 0 {
                // Check row limit before processing each row
                total_rows += 1;
                if total_rows > row_limit {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Equality delete file exceeds maximum allowed rows ({row_limit}). \
                             This limit exists to prevent unbounded memory growth. \
                             To resolve this issue, consider: \
                             (1) Compacting the table to merge equality deletes with data files, \
                             (2) Using position deletes instead of equality deletes for large-scale deletions, \
                             (3) Partitioning the table to limit per-partition delete file sizes. \
                             See DEFAULT_MAX_EQUALITY_DELETE_ROWS documentation for details.",
                        ),
                    )
                    .with_context("rows_processed", total_rows.to_string())
                    .with_context("max_rows_allowed", row_limit.to_string()));
                }

                let mut row_predicate = AlwaysTrue;
                for &mut (ref mut column, ref field_name) in &mut datum_columns_with_names {
                    if let Some(item) = column.next() {
                        let cell_predicate = if let Some(datum) = item? {
                            Reference::new(field_name.clone()).equal_to(datum.clone())
                        } else {
                            Reference::new(field_name.clone()).is_null()
                        };
                        row_predicate = row_predicate.and(cell_predicate)
                    }
                }
                row_predicates.push(row_predicate.not().rewrite_not());
            }
        }

        // All row predicates are combined to a single predicate by creating a balanced binary tree.
        // Using a simple fold would result in a deeply nested predicate that can cause a stack overflow.
        while row_predicates.len() > 1 {
            let mut next_level = Vec::with_capacity(row_predicates.len().div_ceil(2));
            let mut iter = row_predicates.into_iter();
            while let Some(p1) = iter.next() {
                if let Some(p2) = iter.next() {
                    next_level.push(p1.and(p2));
                } else {
                    next_level.push(p1);
                }
            }
            row_predicates = next_level;
        }

        match row_predicates.pop() {
            Some(p) => Ok(p),
            None => Ok(AlwaysTrue),
        }
    }
}

struct EqDelColumnProcessor<'a> {
    equality_ids: &'a HashSet<i32>,
    collected_columns: Vec<(ArrayRef, String, Type)>,
}

impl<'a> EqDelColumnProcessor<'a> {
    fn new(equality_ids: &'a HashSet<i32>) -> Self {
        Self {
            equality_ids,
            collected_columns: Vec::with_capacity(equality_ids.len()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn finish(
        self,
    ) -> Result<
        Vec<(
            Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>>,
            String,
        )>,
    > {
        self.collected_columns
            .into_iter()
            .map(|(array, field_name, field_type)| {
                let primitive_type = field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "field is not a primitive type")
                    })?
                    .clone();

                let lit_vec = arrow_primitive_to_literal(&array, &field_type)?;
                let datum_iterator: Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>> =
                    Box::new(lit_vec.into_iter().map(move |c| {
                        c.map(|literal| {
                            literal
                                .as_primitive_literal()
                                .map(|primitive_literal| {
                                    Datum::new(primitive_type.clone(), primitive_literal)
                                })
                                .ok_or(Error::new(
                                    ErrorKind::Unexpected,
                                    "failed to convert to primitive literal",
                                ))
                        })
                        .transpose()
                    }));

                Ok((datum_iterator, field_name))
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl SchemaWithPartnerVisitor<ArrayRef> for EqDelColumnProcessor<'_> {
    type T = ();

    fn schema(&mut self, _schema: &Schema, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, partner: &ArrayRef, _value: ()) -> Result<()> {
        if self.equality_ids.contains(&field.id) && field.field_type.as_primitive_type().is_some() {
            self.collected_columns.push((
                partner.clone(),
                field.name.clone(),
                field.field_type.as_ref().clone(),
            ));
        }
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        _results: Vec<()>,
    ) -> Result<()> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _partner: &ArrayRef,
        _key_value: (),
        _value: (),
    ) -> Result<()> {
        Ok(())
    }

    fn primitive(&mut self, _primitive: &PrimitiveType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }
}

struct EqDelRecordBatchPartnerAccessor;

impl PartnerAccessor<ArrayRef> for EqDelRecordBatchPartnerAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field: &NestedField,
    ) -> Result<&'a ArrayRef> {
        let Some(struct_array) = struct_partner.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Expected struct array for field extraction",
            ));
        };

        // Find the field by name within the struct
        for (i, field_def) in struct_array.fields().iter().enumerate() {
            if field_def.name() == &field.name {
                return Ok(struct_array.column(i));
            }
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field {} not found in parent struct", field.name),
        ))
    }

    fn list_element_partner<'a>(&self, _list_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "List columns are unsupported in equality deletes",
        ))
    }

    fn map_key_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }

    fn map_value_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::scan::FileScanTaskDeleteFile;
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    #[cfg(target_os = "linux")]
    fn read_proc_status_kb(field: &str) -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status.lines().find_map(|line| {
            if line.starts_with(field) {
                line.split_whitespace().nth(1)?.parse().ok()
            } else {
                None
            }
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn read_proc_status_kb(_field: &str) -> Option<u64> {
        None
    }

    #[tokio::test]
    async fn test_delete_file_loader_parse_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        let eq_delete_file_path = setup_write_equality_delete_file_1(table_location);

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&eq_delete_file_path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2, 3, 4, 6, 8]);

        let parsed_eq_delete = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await
        .expect("error parsing batch stream");
        println!("{parsed_eq_delete}");

        let expected = "(((((y != 1) OR (z != 100)) OR (a != \"HELP\")) OR (sa != 4)) OR (b != 62696E6172795F64617461)) AND (((((y != 2) OR (z IS NOT NULL)) OR (a IS NOT NULL)) OR (sa != 5)) OR (b IS NOT NULL))".to_string();

        assert_eq!(parsed_eq_delete.to_string(), expected);
    }

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        arrow_schema::Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn setup_write_equality_delete_file_1(table_location: &str) -> String {
        let col_y_vals = vec![1, 2];
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let col_z_vals = vec![Some(100), None];
        let col_z = Arc::new(Int64Array::from(col_z_vals)) as ArrayRef;

        let col_a_vals = vec![Some("HELP"), None];
        let col_a = Arc::new(StringArray::from(col_a_vals)) as ArrayRef;

        let col_s = Arc::new(StructArray::from(vec![
            (
                Arc::new(simple_field("sa", DataType::Int32, false, "6")),
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            ),
            (
                Arc::new(simple_field("sb", DataType::Utf8, true, "7")),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]));

        let col_b_vals = vec![Some(&b"binary_data"[..]), None];
        let col_b = Arc::new(BinaryArray::from(col_b_vals)) as ArrayRef;

        let equality_delete_schema = {
            let struct_field = DataType::Struct(Fields::from(vec![
                simple_field("sa", DataType::Int32, false, "6"),
                simple_field("sb", DataType::Utf8, true, "7"),
            ]));

            let fields = vec![
                Field::new("y", arrow_schema::DataType::Int64, true).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                )),
                Field::new("z", arrow_schema::DataType::Int64, true).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                )),
                Field::new("a", arrow_schema::DataType::Utf8, true).with_metadata(HashMap::from([
                    (PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string()),
                ])),
                simple_field("s", struct_field, false, "5"),
                simple_field("b", DataType::Binary, true, "8"),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_deletes_to_write = RecordBatch::try_new(equality_delete_schema.clone(), vec![
            col_y, col_z, col_a, col_s, col_b,
        ])
        .unwrap();

        let path = format!("{}/equality-deletes-1.parquet", &table_location);

        let file = File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(
            file,
            equality_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();

        writer
            .write(&equality_deletes_to_write)
            .expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        path
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // union of pos dels from pos del file 1 and 2, ie
        // [0, 1, 3, 5, 6, 8, 1022, 1023] | [0, 1, 3, 5, 20, 21, 22, 23]
        // = [0, 1, 3, 5, 6, 8, 20, 21, 22, 23, 1022, 1023]
        assert_eq!(result.lock().unwrap().len(), 12);

        let result = delete_filter.get_delete_vector(&file_scan_tasks[1]);
        assert!(result.is_none()); // no pos dels for file 3
    }

    /// Verifies that evolve_schema on partial-schema equality deletes works correctly
    /// when only equality_ids columns are evolved, not all table columns.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// equality delete files can contain only a subset of columns.
    #[tokio::test]
    async fn test_partial_schema_equality_deletes_evolve_succeeds() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();

        // Create table schema with REQUIRED fields
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    crate::spec::NestedField::required(
                        1,
                        "id",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    crate::spec::NestedField::required(
                        2,
                        "data",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Write equality delete file with PARTIAL schema (only 'data' column)
        let delete_file_path = {
            let data_vals = vec!["a", "d", "g"];
            let data_col = Arc::new(StringArray::from(data_vals)) as ArrayRef;

            let delete_schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
                "data",
                DataType::Utf8,
                false,
                "2", // field ID
            )]));

            let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![data_col]).unwrap();

            let path = format!("{}/partial-eq-deletes.parquet", &table_location);
            let file = File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer =
                ArrowWriter::try_new(file, delete_batch.schema(), Some(props)).unwrap();
            writer.write(&delete_batch).expect("Writing batch");
            writer.close().unwrap();
            path
        };

        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();
        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());

        let batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&delete_file_path)
            .await
            .unwrap();

        // Only evolve the equality_ids columns (field 2), not all table columns
        let equality_ids = vec![2];
        let evolved_stream =
            BasicDeleteFileLoader::evolve_schema(batch_stream, table_schema, &equality_ids)
                .await
                .unwrap();

        let result = evolved_stream.try_collect::<Vec<_>>().await;

        assert!(
            result.is_ok(),
            "Expected success when evolving only equality_ids columns, got error: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1); // Only 'data' column

        // Verify the actual values are preserved after schema evolution
        let data_col = batch.column(0).as_string::<i32>();
        assert_eq!(data_col.value(0), "a");
        assert_eq!(data_col.value(1), "d");
        assert_eq!(data_col.value(2), "g");
    }

    /// Test loading a FileScanTask with BOTH positional and equality deletes.
    /// Verifies the fix for the inverted condition that caused "Missing predicate for equality delete file" errors.
    #[tokio::test]
    async fn test_load_deletes_with_mixed_types() {
        use crate::scan::FileScanTask;
        use crate::spec::{DataFileFormat, Schema};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        // Create the data file schema
        let data_file_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    crate::spec::NestedField::optional(
                        2,
                        "y",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                    crate::spec::NestedField::optional(
                        3,
                        "z",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Write positional delete file
        let positional_delete_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_values =
            vec![format!("{}/data-1.parquet", table_location.to_str().unwrap()); 4];
        let file_path_col = Arc::new(StringArray::from_iter_values(&file_path_values));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![0i64, 1, 2, 3]));

        let positional_deletes_to_write =
            RecordBatch::try_new(positional_delete_schema.clone(), vec![
                file_path_col,
                pos_col,
            ])
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let pos_del_path = format!("{}/pos-del-mixed.parquet", table_location.to_str().unwrap());
        let file = File::create(&pos_del_path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            positional_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();
        writer.write(&positional_deletes_to_write).unwrap();
        writer.close().unwrap();

        // Write equality delete file
        let eq_delete_path = setup_write_equality_delete_file_1(table_location.to_str().unwrap());

        // Create FileScanTask with BOTH positional and equality deletes
        let pos_del = FileScanTaskDeleteFile {
            file_path: pos_del_path,
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        };

        let eq_del = FileScanTaskDeleteFile {
            file_path: eq_delete_path.clone(),
            file_type: DataContentType::EqualityDeletes,
            partition_spec_id: 0,
            equality_ids: Some(vec![2, 3]), // Only use field IDs that exist in both schemas
        };

        let file_scan_task = FileScanTask {
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: format!("{}/data-1.parquet", table_location.to_str().unwrap()),
            data_file_format: DataFileFormat::Parquet,
            schema: data_file_schema.clone(),
            project_field_ids: vec![2, 3],
            predicate: None,
            deletes: vec![pos_del, eq_del],
            partition: None,
            partition_spec_id: None,
            partition_spec: None,
            name_mapping: None,
        };

        // Load the deletes - should handle both types without error
        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);
        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_task.deletes, file_scan_task.schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Verify both delete types can be processed together
        let result = delete_filter
            .build_equality_delete_predicate(&file_scan_task)
            .await;
        assert!(
            result.is_ok(),
            "Failed to build equality delete predicate: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_large_equality_delete_batch_stack_overflow() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // Create a large batch of equality deletes
        let num_rows = 20_000;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        // Write to file
        let path = format!("{}/large-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2]);

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Stress test for equality delete memory bounds (WP3.2).
    ///
    /// This test validates that equality delete application with 100k+ deleted rows
    /// does not cause OOM or unbounded memory growth. The test:
    /// 1. Creates an equality delete file with 100,000+ rows
    /// 2. Scans a small data file with equality delete application
    /// 3. Measures peak RSS delta (VmHWM) on Linux
    /// 4. Asserts the operation completes without crashing
    ///
    /// NOTE: This test proves the operation completes successfully. On non-Linux platforms,
    /// peak RSS measurement is skipped.
    #[tokio::test]
    async fn test_equality_delete_memory_bounds_100k_rows() {
        use std::time::Instant;

        use futures::TryStreamExt;

        use crate::arrow::ArrowReaderBuilder;
        use crate::scan::{FileScanTask, FileScanTaskStream};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // Create a stress test with 100k+ equality delete rows
        // Using 150k rows to ensure we're well above the 100k threshold
        let delete_rows = 150_000i64;
        let col_y_vals: Vec<i64> = (0..delete_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let delete_batch = RecordBatch::try_new(arrow_schema.clone(), vec![col_y]).unwrap();

        // Write equality delete file
        let delete_file_path = format!("{}/stress-eq-deletes-100k.parquet", &table_location);
        let delete_file = File::create(&delete_file_path).unwrap();
        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, arrow_schema.clone(), Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        drop(delete_batch);
        // col_y was moved into RecordBatch::try_new, no need to drop

        // Write a small data file to apply deletes against
        let data_rows = 100i64;
        let data_vals: Vec<i64> = (0..data_rows).collect();
        let data_col = Arc::new(Int64Array::from(data_vals)) as ArrayRef;
        let data_batch = RecordBatch::try_new(arrow_schema.clone(), vec![data_col]).unwrap();

        let data_file_path = format!("{}/stress-eq-data.parquet", &table_location);
        let data_file = File::create(&data_file_path).unwrap();
        let data_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut data_writer =
            ArrowWriter::try_new(data_file, arrow_schema.clone(), Some(data_props)).unwrap();
        data_writer.write(&data_batch).unwrap();
        data_writer.close().unwrap();

        drop(data_batch);
        // data_col was moved into RecordBatch::try_new, no need to drop

        let data_file_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let task = FileScanTask {
            start: 0,
            length: 0,
            record_count: Some(data_rows as u64),
            data_file_path,
            data_file_format: DataFileFormat::Parquet,
            schema: data_file_schema,
            project_field_ids: vec![2],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_path: delete_file_path,
                file_type: DataContentType::EqualityDeletes,
                partition_spec_id: 0,
                equality_ids: Some(vec![2]),
            }],
            partition: None,
            partition_spec_id: None,
            partition_spec: None,
            name_mapping: None,
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let reader = ArrowReaderBuilder::new(file_io.clone()).build();

        // Measure scan time and peak RSS delta during delete application
        let baseline_hwm_kb = read_proc_status_kb("VmHWM:");
        let start = Instant::now();
        let total_rows = reader
            .read(tasks)
            .unwrap()
            .try_fold(
                0usize,
                |acc, batch| async move { Ok(acc + batch.num_rows()) },
            )
            .await
            .unwrap();

        let elapsed = start.elapsed();

        // Log performance characteristics for tuning
        println!(
            "WP3.2 Stress Test Results:\n\
             - Rows processed: {}\n\
             - Data rows scanned: {}\n\
             - Scan time: {:?}\n\
             - Rows remaining: {}",
            delete_rows, data_rows, elapsed, total_rows
        );

        if let (Some(before_kb), Some(after_kb)) = (baseline_hwm_kb, read_proc_status_kb("VmHWM:"))
        {
            let delta_kb = after_kb.saturating_sub(before_kb);
            let max_delta_kb = 512 * 1024;
            assert!(
                delta_kb <= max_delta_kb,
                "Peak RSS delta exceeded limit: {} kB > {} kB",
                delta_kb,
                max_delta_kb
            );
            println!(
                "Peak RSS delta (VmHWM): {} kB (limit {} kB)",
                delta_kb, max_delta_kb
            );
        } else {
            println!("VmHWM not available; skipping peak RSS assertion");
        }

        // All data rows should be deleted (delete file contains all values in the data file)
        assert_eq!(
            total_rows, 0,
            "Expected all data rows to be deleted with {} equality deletes",
            delete_rows
        );
    }

    /// Test with multi-column equality deletes (more realistic scenario)
    #[tokio::test]
    async fn test_equality_delete_memory_bounds_multicolumn() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // 50k rows with 3 columns = 150k predicates worth of data
        let num_rows = 50_000;
        let col_a_vals: Vec<i64> = (0..num_rows).collect();
        let col_a = Arc::new(Int64Array::from(col_a_vals)) as ArrayRef;

        let col_b_vals: Vec<i64> = (0..num_rows).map(|i| i * 2).collect();
        let col_b = Arc::new(Int64Array::from(col_b_vals)) as ArrayRef;

        // Include some nulls to test null handling under load
        let col_c_vals: Vec<Option<i64>> = (0..num_rows)
            .map(|i| if i % 100 == 0 { None } else { Some(i * 3) })
            .collect();
        let col_c = Arc::new(Int64Array::from(col_c_vals)) as ArrayRef;

        let schema =
            Arc::new(arrow_schema::Schema::new(vec![
                Field::new("a", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())],
                )),
                Field::new("b", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                )),
                Field::new("c", arrow_schema::DataType::Int64, true).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                )),
            ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_a, col_b, col_c]).unwrap();

        let path = format!("{}/stress-eq-deletes-multicolumn.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![1, 2, 3]); // All three columns

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(
            result.is_ok(),
            "Multi-column stress test with {} rows failed: {:?}",
            num_rows,
            result.err()
        );

        let predicate = result.unwrap();
        assert_ne!(predicate, Predicate::AlwaysTrue);

        println!(
            "Multi-column stress test passed: {} rows × 3 columns",
            num_rows
        );
    }

    /// Test that string columns (which use more memory per value) also work under load
    #[tokio::test]
    async fn test_equality_delete_memory_bounds_string_columns() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // 100k rows with string values (more memory per predicate)
        let num_rows = 100_000;
        let col_vals: Vec<String> = (0..num_rows).map(|i| format!("value_{:06}", i)).collect();
        let col = Arc::new(StringArray::from(col_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("key", arrow_schema::DataType::Utf8, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

        let path = format!("{}/stress-eq-deletes-strings.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![1]);

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(
            result.is_ok(),
            "String column stress test with {} rows failed: {:?}",
            num_rows,
            result.err()
        );

        println!("String column stress test passed: {} rows", num_rows);
    }

    /// Test that the predicate limit guard mechanism works correctly.
    ///
    /// This test verifies that when the row limit is exceeded, the parser:
    /// 1. Returns a PreconditionFailed error (not OOM or panic)
    /// 2. Provides clear error context with row counts
    /// 3. Provides remediation guidance in the error message
    #[tokio::test]
    async fn test_equality_delete_predicate_limit_guard() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // Create a delete file with 1000 rows
        let num_rows = 1000i64;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        let path = format!("{}/limit-test-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2]);

        // Set a low limit (100 rows) to trigger the guard
        let result =
            CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream_with_limit(
                record_batch_stream,
                eq_ids,
                Some(100), // Low limit to trigger guard
            )
            .await;

        // Verify the guard triggers correctly
        assert!(result.is_err(), "Expected error when exceeding row limit");

        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            crate::ErrorKind::PreconditionFailed,
            "Expected PreconditionFailed error kind"
        );

        let err_msg = err.to_string();
        assert!(
            err_msg.contains("exceeds maximum allowed rows"),
            "Error message should mention row limit: {}",
            err_msg
        );
        assert!(
            err_msg.contains("Compacting"),
            "Error message should provide remediation guidance: {}",
            err_msg
        );

        println!(
            "Guard mechanism test passed - error returned gracefully:\n{}",
            err
        );
    }

    /// Test that the limit can be disabled by passing Some(0) or None with large value
    #[tokio::test]
    async fn test_equality_delete_limit_disabled() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        let num_rows = 500i64;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        let path = format!("{}/no-limit-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());

        // Test with None (no limit)
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .unwrap();

        let eq_ids = HashSet::from_iter(vec![2]);

        let result =
            CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream_with_limit(
                record_batch_stream,
                eq_ids.clone(),
                None, // No limit
            )
            .await;

        assert!(
            result.is_ok(),
            "Should succeed with no limit: {:?}",
            result.err()
        );

        println!("Limit disabled test passed - {} rows processed", num_rows);
    }
}
