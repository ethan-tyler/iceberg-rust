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
        SparkSession.builder
        .appName("IcebergValidation")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
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
                min_max = df.selectExpr(f"min({col_name})", f"max({col_name})").collect()[0]
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
                    "operation": latest["operation"] if "operation" in latest.asDict() else None,
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


VALIDATORS = {
    "count": validate_count,
    "full": validate_full,
    "metadata": validate_metadata,
    "query": validate_query,
    "distinct": validate_distinct_count,
}


def main():
    if len(sys.argv) < 3:
        print(json.dumps({
            "error": "Usage: validate.py <table_name> <validation_type> [options]",
            "available_types": list(VALIDATORS.keys()),
        }))
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
                result = {"error": "Query validation requires a query template as third argument"}
            else:
                query_template = sys.argv[3]
                result = validate_query(spark, table_name, query_template)
        elif validation_type == "distinct":
            if len(sys.argv) < 4:
                result = {"error": "Distinct validation requires a column name as third argument"}
            else:
                column = sys.argv[3]
                result = validate_distinct_count(spark, table_name, column)
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
