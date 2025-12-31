# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Spark validation script for cross-engine interoperability tests.

This script is invoked via docker exec from Rust integration tests to validate
that tables modified by Rust can be read correctly by Spark.

Usage:
    python3 validate.py <table_name> <validation_type> [options]

Examples:
    python3 validate.py test_table count
    python3 validate.py test_table full
    python3 validate.py test_table metadata
    python3 validate.py test_table query "SELECT count(*) FROM {table}"
"""

import sys
import json
from pyspark.sql import SparkSession


def get_spark():
    """Get or create SparkSession configured for Iceberg REST catalog."""
    return (
        SparkSession.builder.appName("IcebergValidation")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.catalog.rest.write.delete.mode", "merge-on-read")
        .config("spark.sql.catalog.rest.write.update.mode", "merge-on-read")
        .config("spark.sql.catalog.rest.write.merge.mode", "merge-on-read")
        .getOrCreate()
    )


def table_ref(table_name):
    """Return a Spark table identifier, defaulting to rest.default namespace."""
    if "." in table_name:
        return table_name
    return f"rest.default.{table_name}"


def validate_count(spark, table_name):
    """Return row count for the table."""
    df = spark.table(table_ref(table_name))
    return {"count": df.count()}


def validate_full(spark, table_name):
    """Return full validation including count, checksum, and column info."""
    df = spark.table(table_ref(table_name))
    count = df.count()

    # Calculate checksum using hash of all columns
    checksum_result = df.selectExpr("sum(hash(*))").collect()
    checksum = checksum_result[0][0] if checksum_result else None

    # Get column names
    columns = df.columns

    # Get min/max for numeric columns (if any)
    bounds = {}
    for col_name in columns:
        col_type = str(df.schema[col_name].dataType)
        if "Int" in col_type or "Long" in col_type or "Double" in col_type:
            try:
                min_max = df.selectExpr(
                    f"min({col_name})", f"max({col_name})"
                ).collect()[0]
                bounds[col_name] = {"min": min_max[0], "max": min_max[1]}
            except Exception:
                pass

    return {
        "count": count,
        "checksum": checksum,
        "columns": columns,
        "bounds": bounds if bounds else None,
    }


def validate_metadata(spark, table_name):
    """Return metadata table information for validation."""
    try:
        snapshots = spark.table(f"{table_ref(table_name)}.snapshots")
        snapshot_count = snapshots.count()

        # Get current snapshot info
        current_snapshot = None
        if snapshot_count > 0:
            latest = snapshots.orderBy("committed_at", ascending=False).first()
            if latest:
                current_snapshot = {
                    "snapshot_id": latest["snapshot_id"],
                    "operation": latest["operation"]
                    if "operation" in latest.asDict()
                    else None,
                }
    except Exception as e:
        return {"error": f"Failed to read snapshots: {str(e)}"}

    try:
        files = spark.table(f"{table_ref(table_name)}.files")
        file_count = files.count()
    except Exception as e:
        return {"error": f"Failed to read files: {str(e)}"}

    try:
        manifests = spark.table(f"{table_ref(table_name)}.manifests")
        manifest_count = manifests.count()
    except Exception as e:
        return {"error": f"Failed to read manifests: {str(e)}"}

    return {
        "snapshot_count": snapshot_count,
        "file_count": file_count,
        "manifest_count": manifest_count,
        "current_snapshot": current_snapshot,
    }


def validate_query(spark, table_name, query_template):
    """Execute a custom query and return results."""
    # Replace {table} placeholder with actual table reference
    query = query_template.replace("{table}", table_ref(table_name))

    try:
        result = spark.sql(query).collect()
        # Convert to serializable format
        rows = []
        for row in result:
            rows.append(row.asDict())
        return {"rows": rows, "count": len(rows)}
    except Exception as e:
        return {"error": f"Query failed: {str(e)}"}


def validate_distinct_count(spark, table_name, column):
    """Return count of distinct values for a column."""
    df = spark.table(table_ref(table_name))
    distinct_count = df.select(column).distinct().count()
    return {"distinct_count": distinct_count, "column": column}


def validate_snapshot_summary(spark, table_name, snapshot_id=None):
    """
    Extract detailed snapshot summary fields for semantic parity comparison.

    Returns all summary fields from a specific snapshot (or current if not specified).
    This enables field-by-field comparison between Rust and Spark produced metadata.
    """
    try:
        snapshots_df = spark.table(f"{table_ref(table_name)}.snapshots")

        if snapshot_id:
            snapshot_row = snapshots_df.filter(f"snapshot_id = {snapshot_id}").first()
        else:
            # Get current/latest snapshot
            snapshot_row = snapshots_df.orderBy("committed_at", ascending=False).first()

        if not snapshot_row:
            return {"error": "No snapshot found"}

        row_dict = snapshot_row.asDict(recursive=True)

        # Extract core fields
        result = {
            "snapshot_id": row_dict.get("snapshot_id"),
            "parent_id": row_dict.get("parent_id"),
            "operation": row_dict.get("operation"),
            "committed_at": str(row_dict.get("committed_at"))
            if row_dict.get("committed_at")
            else None,
        }

        # Extract summary map - this contains the key fields for parity comparison
        summary = row_dict.get("summary")
        if summary:
            # summary is a map/dict in Spark
            summary_dict = {}
            if isinstance(summary, dict):
                summary_dict = summary
            else:
                # Handle Row type
                try:
                    summary_dict = (
                        summary.asDict()
                        if hasattr(summary, "asDict")
                        else dict(summary)
                    )
                except:
                    summary_dict = {}

            result["summary"] = {
                # Core counts
                "added-data-files": summary_dict.get("added-data-files"),
                "deleted-data-files": summary_dict.get("deleted-data-files"),
                "added-records": summary_dict.get("added-records"),
                "deleted-records": summary_dict.get("deleted-records"),
                "added-files-size": summary_dict.get("added-files-size"),
                "removed-files-size": summary_dict.get("removed-files-size"),
                # Delete file counts
                "added-delete-files": summary_dict.get("added-delete-files"),
                "removed-delete-files": summary_dict.get("removed-delete-files"),
                "added-position-deletes": summary_dict.get("added-position-deletes"),
                "removed-position-deletes": summary_dict.get(
                    "removed-position-deletes"
                ),
                "added-equality-deletes": summary_dict.get("added-equality-deletes"),
                "removed-equality-deletes": summary_dict.get(
                    "removed-equality-deletes"
                ),
                # Totals
                "total-data-files": summary_dict.get("total-data-files"),
                "total-delete-files": summary_dict.get("total-delete-files"),
                "total-records": summary_dict.get("total-records"),
                "total-files-size": summary_dict.get("total-files-size"),
                "total-position-deletes": summary_dict.get("total-position-deletes"),
                "total-equality-deletes": summary_dict.get("total-equality-deletes"),
                # Partition info
                "changed-partition-count": summary_dict.get("changed-partition-count"),
                # Raw summary for any additional fields
                "raw": summary_dict,
            }
        else:
            result["summary"] = None

        return result
    except Exception as e:
        return {"error": f"Failed to extract snapshot summary: {str(e)}"}


def validate_manifest_entries(spark, table_name, limit=100):
    """
    Extract manifest entry details for structure comparison.

    Returns manifest entry fields to verify ordering, required fields, and partition serialization.
    """
    try:
        # Read manifest entries via the entries metadata table
        entries_df = spark.table(f"{table_ref(table_name)}.entries")

        entries = []
        for row in entries_df.limit(limit).collect():
            row_dict = row.asDict(recursive=True)
            entry = {
                "status": row_dict.get("status"),
                "snapshot_id": row_dict.get("snapshot_id"),
                "sequence_number": row_dict.get("sequence_number"),
                "file_sequence_number": row_dict.get("file_sequence_number"),
            }

            # Optional manifest context when available
            if "manifest_path" in row_dict:
                entry["manifest_path"] = row_dict.get("manifest_path")
            if "manifest_pos" in row_dict:
                entry["manifest_pos"] = row_dict.get("manifest_pos")
            if "partition_spec_id" in row_dict:
                entry["partition_spec_id"] = row_dict.get("partition_spec_id")
            if "spec_id" in row_dict:
                entry["spec_id"] = row_dict.get("spec_id")

            # Extract data_file fields
            data_file = row_dict.get("data_file")
            if data_file:
                entry["data_file"] = {
                    "content": data_file.get("content"),
                    "file_path": data_file.get("file_path"),
                    "file_format": data_file.get("file_format"),
                    "partition": data_file.get("partition"),
                    "record_count": data_file.get("record_count"),
                    "file_size_in_bytes": data_file.get("file_size_in_bytes"),
                }
            entries.append(entry)

        return {
            "entry_count": entries_df.count(),
            "entries": entries,
        }
    except Exception as e:
        return {"error": f"Failed to extract manifest entries: {str(e)}"}


def validate_execute_sql(spark, table_name, sql_template):
    """
    Execute one or more SQL statements, then return snapshot summary.

    SQL statements can be separated by ';' and may use {table} placeholder.
    """
    try:
        table = table_ref(table_name)
        sql_text = sql_template.replace("{table}", table)
        statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
        for statement in statements:
            spark.sql(statement)

        return validate_snapshot_summary(spark, table_name)
    except Exception as e:
        return {"error": f"SQL execution failed: {str(e)}"}


def validate_spark_dml(spark, table_name, dml_type, predicate=None, update_values=None):
    """
    Execute a DML operation via Spark and return the resulting snapshot summary.

    This allows direct comparison: run same DML via Rust, then via Spark,
    and compare the resulting metadata structures.

    Args:
        table_name: Table to operate on
        dml_type: "delete", "update", or "insert"
        predicate: WHERE clause for delete/update
        update_values: SET clause values for update (e.g., "status='done'")
    """
    try:
        tbl = table_ref(table_name)

        if dml_type == "delete":
            if predicate:
                spark.sql(f"DELETE FROM {tbl} WHERE {predicate}")
            else:
                spark.sql(f"DELETE FROM {tbl}")
        elif dml_type == "update" and update_values:
            if predicate:
                spark.sql(f"UPDATE {tbl} SET {update_values} WHERE {predicate}")
            else:
                spark.sql(f"UPDATE {tbl} SET {update_values}")
        else:
            return {"error": f"Unsupported DML type or missing arguments: {dml_type}"}

        # Get the snapshot summary for the just-committed operation
        return validate_snapshot_summary(spark, table_name)
    except Exception as e:
        return {"error": f"DML execution failed: {str(e)}"}


VALIDATORS = {
    "count": validate_count,
    "full": validate_full,
    "metadata": validate_metadata,
    "query": validate_query,
    "distinct": validate_distinct_count,
    "snapshot_summary": validate_snapshot_summary,
    "manifest_entries": validate_manifest_entries,
    "spark_dml": validate_spark_dml,
    "execute_sql": validate_execute_sql,
}


def main():
    if len(sys.argv) < 3:
        print(
            json.dumps(
                {
                    "error": "Usage: validate.py <table_name> <validation_type> [options]",
                    "available_types": list(VALIDATORS.keys()),
                }
            )
        )
        sys.exit(1)

    table_name = sys.argv[1]
    validation_type = sys.argv[2]

    spark = None

    try:
        spark = get_spark()
    except Exception as e:
        print(json.dumps({"error": f"Failed to initialize Spark: {str(e)}"}))
        sys.exit(1)

    try:
        if validation_type not in VALIDATORS:
            result = {
                "error": f"Unknown validation type: {validation_type}",
                "available_types": list(VALIDATORS.keys()),
            }
        elif validation_type == "query":
            if len(sys.argv) < 4:
                result = {
                    "error": "Query validation requires a query template as third argument"
                }
            else:
                query_template = sys.argv[3]
                result = validate_query(spark, table_name, query_template)
        elif validation_type == "distinct":
            if len(sys.argv) < 4:
                result = {
                    "error": "Distinct validation requires a column name as third argument"
                }
            else:
                column = sys.argv[3]
                result = validate_distinct_count(spark, table_name, column)
        elif validation_type == "snapshot_summary":
            # Optional: snapshot_id as third argument
            snapshot_id = int(sys.argv[3]) if len(sys.argv) > 3 else None
            result = validate_snapshot_summary(spark, table_name, snapshot_id)
        elif validation_type == "manifest_entries":
            # Optional: limit as third argument
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100
            result = validate_manifest_entries(spark, table_name, limit)
        elif validation_type == "spark_dml":
            # Required: dml_type, optional: predicate, update_values
            if len(sys.argv) < 4:
                result = {
                    "error": "spark_dml requires at least dml_type (delete/update) as third argument"
                }
            else:
                dml_type = sys.argv[3]
                predicate = sys.argv[4] if len(sys.argv) > 4 else None
                update_values = sys.argv[5] if len(sys.argv) > 5 else None
                result = validate_spark_dml(
                    spark, table_name, dml_type, predicate, update_values
                )
        elif validation_type == "execute_sql":
            if len(sys.argv) < 4:
                result = {
                    "error": "execute_sql requires a SQL statement as third argument"
                }
            else:
                sql_template = sys.argv[3]
                result = validate_execute_sql(spark, table_name, sql_template)
        else:
            result = VALIDATORS[validation_type](spark, table_name)

        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
