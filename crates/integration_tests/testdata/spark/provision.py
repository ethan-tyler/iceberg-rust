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

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

# The configuration is important, otherwise we get many small
# parquet files with a single row. When a positional delete
# hits the Parquet file with one row, the parquet file gets
# dropped instead of having a merge-on-read delete file.
spark = (
    SparkSession.builder.config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()
)

spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS rest.default""")

spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(
    f"DELETE FROM rest.default.test_positional_merge_on_read_deletes WHERE number = 9"
)

spark.sql(
    f"""
  CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_double_deletes (
    dt     date,
    number integer,
    letter string
  )
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
  );
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_double_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

#  Creates two positional deletes that should be merged
spark.sql(
    f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE number = 9"
)
spark.sql(
    f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE letter == 'f'"
)

#  Create a table, and do some renaming
spark.sql(
    "CREATE OR REPLACE TABLE rest.default.test_rename_column (lang string) USING iceberg"
)
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Python')")
spark.sql("ALTER TABLE rest.default.test_rename_column RENAME COLUMN lang TO language")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Java')")

#  Create a table, and do some evolution
spark.sql(
    "CREATE OR REPLACE TABLE rest.default.test_promote_column (foo int) USING iceberg"
)
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (19)")
spark.sql("ALTER TABLE rest.default.test_promote_column ALTER COLUMN foo TYPE bigint")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (25)")

#  Create a table, and do some evolution on a partition column
spark.sql(
    "CREATE OR REPLACE TABLE rest.default.test_promote_partition_column (foo int, bar float, baz decimal(4, 2)) USING iceberg PARTITIONED BY (foo)"
)
spark.sql(
    "INSERT INTO rest.default.test_promote_partition_column VALUES (19, 19.25, 19.25)"
)
spark.sql(
    "ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN foo TYPE bigint"
)
spark.sql(
    "ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN bar TYPE double"
)
spark.sql(
    "ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN baz TYPE decimal(6, 2)"
)
spark.sql(
    "INSERT INTO rest.default.test_promote_partition_column VALUES (25, 22.25, 22.25)"
)

#  Create a table with various types
spark.sql("""
CREATE OR REPLACE TABLE rest.default.types_test USING ICEBERG AS
SELECT
    CAST(s % 2 = 1 AS BOOLEAN) AS cboolean,
    CAST(s % 256 - 128 AS TINYINT) AS ctinyint,
    CAST(s AS SMALLINT) AS csmallint,
    CAST(s AS INT) AS cint,
    CAST(s AS BIGINT) AS cbigint,
    CAST(s AS FLOAT) AS cfloat,
    CAST(s AS DOUBLE) AS cdouble,
    CAST(s / 100.0 AS DECIMAL(8, 2)) AS cdecimal,
    CAST(DATE('1970-01-01') + s AS DATE) AS cdate,
    CAST(from_unixtime(s) AS TIMESTAMP_NTZ) AS ctimestamp_ntz,
    CAST(from_unixtime(s) AS TIMESTAMP) AS ctimestamp,
    CAST(s AS STRING) AS cstring,
    CAST(s AS BINARY) AS cbinary
FROM (
    SELECT EXPLODE(SEQUENCE(0, 1000)) AS s
);
""")

# =============================================================================
# Partition Evolution Cross-Engine Tests
# =============================================================================
# These tables are created with partition evolution to test iceberg-rust's
# ability to perform DML operations (DELETE, UPDATE, MERGE) on tables with
# multiple partition specs. See docs/partition-evolution/DESIGN.md

# Table for testing DELETE on evolved partitions
# Spec v0: unpartitioned, Spec v1: partitioned by category
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_partition_evolution_delete (
    id     integer,
    category string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Insert data under spec v0 (unpartitioned)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_delete
VALUES
    (1, 'electronics', 100),
    (2, 'electronics', 200),
    (3, 'books', 300);
""")

# Evolve to partition by category
spark.sql(
    "ALTER TABLE rest.default.test_partition_evolution_delete ADD PARTITION FIELD category"
)

# Insert data under spec v1 (partitioned by category)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_delete
VALUES
    (4, 'books', 400),
    (5, 'electronics', 500);
""")

# Table for testing UPDATE on evolved partitions
# Spec v0: unpartitioned, Spec v1: partitioned by region
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_partition_evolution_update (
    id     integer,
    region string,
    status string
)
USING iceberg
TBLPROPERTIES (
    'write.update.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Insert data under spec v0 (unpartitioned)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_update
VALUES
    (1, 'US', 'pending'),
    (2, 'US', 'pending'),
    (3, 'EU', 'pending');
""")

# Evolve to partition by region
spark.sql(
    "ALTER TABLE rest.default.test_partition_evolution_update ADD PARTITION FIELD region"
)

# Insert data under spec v1 (partitioned by region)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_update
VALUES
    (4, 'EU', 'pending'),
    (5, 'APAC', 'pending');
""")

# Table for testing MERGE on evolved partitions
# Spec v0: unpartitioned, Spec v1: partitioned by region
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_partition_evolution_merge (
    id     integer,
    region string,
    amount integer
)
USING iceberg
TBLPROPERTIES (
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Insert data under spec v0 (unpartitioned)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_merge
VALUES
    (1, 'US', 100),
    (2, 'US', 200),
    (3, 'EU', 300);
""")

# Evolve to partition by region
spark.sql(
    "ALTER TABLE rest.default.test_partition_evolution_merge ADD PARTITION FIELD region"
)

# Insert data under spec v1 (partitioned by region)
spark.sql("""
INSERT INTO rest.default.test_partition_evolution_merge
VALUES
    (4, 'EU', 400),
    (5, 'APAC', 500);
""")

# =============================================================================
# WP1 Cross-Engine Interop Tests (Unpartitioned Tables)
# =============================================================================
# These tables are unpartitioned to avoid known blockers with partitioned DML.

# Table for testing DELETE with NULL semantics
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_delete_null_semantics (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Insert data including NULLs
spark.sql("""
INSERT INTO rest.default.test_delete_null_semantics
VALUES
    (1, 'alpha', 100),
    (2, 'beta', NULL),
    (3, NULL, 300),
    (4, 'delta', 400),
    (5, NULL, NULL),
    (6, 'zeta', 600);
""")

# Table for testing compaction (binpack)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_compaction (
    id     integer,
    data   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert data in multiple small commits to create multiple small files
spark.sql("INSERT INTO rest.default.test_compaction VALUES (1, 'row1')")
spark.sql("INSERT INTO rest.default.test_compaction VALUES (2, 'row2')")
spark.sql("INSERT INTO rest.default.test_compaction VALUES (3, 'row3')")
spark.sql("INSERT INTO rest.default.test_compaction VALUES (4, 'row4')")
spark.sql("INSERT INTO rest.default.test_compaction VALUES (5, 'row5')")

# Table for testing expire snapshots
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_expire_snapshots (
    id     integer,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Create multiple snapshots by doing multiple inserts
spark.sql("INSERT INTO rest.default.test_expire_snapshots VALUES (1, 100)")
spark.sql("INSERT INTO rest.default.test_expire_snapshots VALUES (2, 200)")
spark.sql("INSERT INTO rest.default.test_expire_snapshots VALUES (3, 300)")
spark.sql("INSERT INTO rest.default.test_expire_snapshots VALUES (4, 400)")

# Table for testing rewrite manifests
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_rewrite_manifests (
    id     integer,
    data   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert data in multiple commits to create multiple manifests
spark.sql("INSERT INTO rest.default.test_rewrite_manifests VALUES (1, 'one')")
spark.sql("INSERT INTO rest.default.test_rewrite_manifests VALUES (2, 'two')")
spark.sql("INSERT INTO rest.default.test_rewrite_manifests VALUES (3, 'three')")

# Table for testing expire snapshots with file deletion (WP2)
# Creates multiple snapshots with separate data files that can be safely orphaned
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_expire_snapshots_file_deletion (
    id     integer,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Create multiple snapshots by doing multiple inserts (same as test_expire_snapshots)
spark.sql(
    "INSERT INTO rest.default.test_expire_snapshots_file_deletion VALUES (1, 100)"
)
spark.sql(
    "INSERT INTO rest.default.test_expire_snapshots_file_deletion VALUES (2, 200)"
)
spark.sql(
    "INSERT INTO rest.default.test_expire_snapshots_file_deletion VALUES (3, 300)"
)
spark.sql(
    "INSERT INTO rest.default.test_expire_snapshots_file_deletion VALUES (4, 400)"
)

# Table for testing remove orphan files
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_remove_orphan_files (
    id     integer,
    data   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

spark.sql("INSERT INTO rest.default.test_remove_orphan_files VALUES (1, 'alpha')")

# =============================================================================
# WP1.5 Semantic Parity Validation Tables
# =============================================================================
# These tables are created in PAIRS (rust_ and spark_ variants) to enable
# field-by-field comparison of metadata produced by identical operations.
# See docs/parity/semantic-parity-findings.md

# Pair 1: DELETE semantic parity test
# Table for Rust DELETE operation
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_delete_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Table for Spark DELETE operation (identical initial state)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_delete_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

# Insert identical data into both tables
for table in ["parity_delete_rust", "parity_delete_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, 'beta', 200),
        (3, 'gamma', 300),
        (4, 'delta', 400),
        (5, 'epsilon', 500);
    """)

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_delete_empty_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_delete_empty_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_delete_empty_rust", "parity_delete_empty_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, 'beta', 200),
        (3, 'gamma', 300),
        (4, 'delta', 400),
        (5, 'epsilon', 500);
    """)

# Pair 2: Compaction semantic parity test (multiple small files)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_compact_rust (
    id     integer,
    data   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_compact_spark (
    id     integer,
    data   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert data in multiple commits to create multiple small files
for table in ["parity_compact_rust", "parity_compact_spark"]:
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (1, 'row1')")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (2, 'row2')")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (3, 'row3')")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (4, 'row4')")

# Pair 3: Expire snapshots semantic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_expire_rust (
    id     integer,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_expire_spark (
    id     integer,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Create multiple snapshots by doing multiple inserts
for table in ["parity_expire_rust", "parity_expire_spark"]:
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (1, 100)")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (2, 200)")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (3, 300)")

# Pair 4: Edge case - NULL value handling
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_null_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_null_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_null_rust", "parity_null_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, NULL, 200),
        (3, 'gamma', NULL),
        (4, NULL, NULL);
    """)

# Pair 5: UPDATE semantic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_update_rust (
    id     integer,
    status string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.update.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_update_spark (
    id     integer,
    status string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.update.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_update_rust", "parity_update_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'pending', 10),
        (2, 'pending', 20),
        (3, 'pending', 30),
        (4, 'pending', 40);
    """)

# Pair 6: MERGE semantic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_merge_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_merge_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_merge_rust", "parity_merge_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, 'beta', 200),
        (3, 'gamma', 300),
        (4, 'delta', 400);
    """)

# Pair 7: Boundary values semantic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_boundary_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_boundary_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_boundary_rust", "parity_boundary_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (-2147483648, 'min', -1),
        (2147483647, 'max', 1),
        (0, '', 0);
    """)

# Pair 8: Empty partition delete semantic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_partition_rust (
    id       integer,
    category string,
    value    integer
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_partition_spark (
    id       integer,
    category string,
    value    integer
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_partition_rust", "parity_partition_spark"]:
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (1, 'A', 10)")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (2, 'B', 20)")
    spark.sql(f"INSERT INTO rest.default.{table} VALUES (3, NULL, 30)")

# Pair 9: Three-valued logic parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_three_valued_rust (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_three_valued_spark (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_three_valued_rust", "parity_three_valued_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, NULL, 200),
        (3, 'gamma', NULL),
        (4, NULL, NULL);
    """)

# Pair 10: Zero-length binary parity test
spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_binary_rust (
    id      integer,
    payload binary
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

spark.sql("""
CREATE OR REPLACE TABLE rest.default.parity_binary_spark (
    id      integer,
    payload binary
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'format-version'='2'
);
""")

for table in ["parity_binary_rust", "parity_binary_spark"]:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, CAST('' AS BINARY)),
        (2, CAST('abc' AS BINARY));
    """)

# Pair 11: Schema error rejection parity tests
schema_tables = [
    "parity_schema_rust",
    "parity_schema_spark",
    "parity_schema_add_existing_rust",
    "parity_schema_add_existing_spark",
    "parity_schema_rename_existing_rust",
    "parity_schema_rename_existing_spark",
    "parity_schema_drop_missing_rust",
    "parity_schema_drop_missing_spark",
    "parity_schema_rename_missing_rust",
    "parity_schema_rename_missing_spark",
    "parity_schema_incompatible_rust",
    "parity_schema_incompatible_spark",
]

for table in schema_tables:
    spark.sql(f"""
    CREATE OR REPLACE TABLE rest.default.{table} (
        id     integer,
        name   string,
        value  integer
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2'
    );
    """)

for table in schema_tables:
    spark.sql(f"""
    INSERT INTO rest.default.{table}
    VALUES
        (1, 'alpha', 100),
        (2, 'beta', 200);
    """)

# =============================================================================
# WP3.1 Equality Delete Interop Tests (Spark -> Rust)
# =============================================================================
# These tables test Rust's ability to read equality delete files written by Spark.
# Since Spark doesn't write equality deletes by default (it uses positional deletes),
# we write parquet files directly with PyArrow and register them via Iceberg API.

import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os


def create_equality_delete_file(
    spark, table_name, delete_schema_fields, delete_data, partition_spec_id=0
):
    """
    Create an equality delete file for a table using PyArrow + Iceberg Java API.

    Args:
        spark: SparkSession
        table_name: Full table name (e.g., 'rest.default.test_table')
        delete_schema_fields: List of (field_id, name, type_str) tuples for delete columns
        delete_data: List of tuples containing delete values
        partition_spec_id: Partition spec ID (default 0 for unpartitioned)
    """
    from py4j.java_gateway import java_import
    import uuid as py_uuid

    jvm = spark._jvm
    java_import(jvm, "org.apache.iceberg.*")
    java_import(jvm, "org.apache.iceberg.catalog.*")
    java_import(jvm, "org.apache.iceberg.spark.*")
    java_import(jvm, "org.apache.iceberg.io.*")
    java_import(jvm, "org.apache.iceberg.data.*")

    # Get the Spark catalog and load the table
    spark_catalog = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergCatalog(
        spark._jsparkSession, "rest"
    )
    table_identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(
        table_name.replace("rest.", "")
    )
    table = spark_catalog.loadTable(table_identifier)

    # Build PyArrow schema with Iceberg field IDs
    pa_fields = []
    equality_ids = []
    for field_id, name, type_str in delete_schema_fields:
        if type_str == "int":
            pa_type = pa.int32()
        elif type_str == "long":
            pa_type = pa.int64()
        elif type_str == "string":
            pa_type = pa.string()
        else:
            raise ValueError(f"Unsupported type: {type_str}")

        # Add PARQUET:field_id metadata for Iceberg
        field = pa.field(
            name, pa_type, metadata={b"PARQUET:field_id": str(field_id).encode()}
        )
        pa_fields.append(field)
        equality_ids.append(field_id)

    pa_schema = pa.schema(pa_fields)

    # Create PyArrow table from delete data
    columns = {field.name: [] for field in pa_fields}
    for row in delete_data:
        for i, (field_id, name, type_str) in enumerate(delete_schema_fields):
            columns[name].append(row[i])

    # Build arrays
    arrays = []
    for field_id, name, type_str in delete_schema_fields:
        if type_str == "int":
            arrays.append(pa.array(columns[name], type=pa.int32()))
        elif type_str == "long":
            arrays.append(pa.array(columns[name], type=pa.int64()))
        elif type_str == "string":
            arrays.append(pa.array(columns[name], type=pa.string()))

    delete_table = pa.table(
        dict(zip([f[1] for f in delete_schema_fields], arrays)), schema=pa_schema
    )

    # Write to temporary local file first
    temp_file = f"/tmp/equality-delete-{py_uuid.uuid4()}.parquet"
    pq.write_table(delete_table, temp_file)

    # Get file size
    file_size = os.path.getsize(temp_file)

    # Upload to S3 via Iceberg's FileIO
    delete_file_path = (
        f"{table.location()}/data/equality-delete-{py_uuid.uuid4()}.parquet"
    )
    output_file = table.io().newOutputFile(delete_file_path)

    # Read temp file and write to output
    with open(temp_file, "rb") as f:
        content = f.read()

    output_stream = output_file.create()
    output_stream.write(content)
    output_stream.close()

    os.remove(temp_file)

    # Create the DeleteFile metadata
    FileFormat = jvm.org.apache.iceberg.FileFormat
    partition_spec = table.specs().get(partition_spec_id)
    partition_type = partition_spec.partitionType()
    partition_data = jvm.org.apache.iceberg.PartitionData(partition_type)

    # Use FileMetadata.deleteFileBuilder for creating delete files
    # Build the equality field IDs as a Java int array
    gateway = spark.sparkContext._gateway
    int_array = gateway.new_array(gateway.jvm.int, len(equality_ids))
    for i, eid in enumerate(equality_ids):
        int_array[i] = eid

    # Create delete file using FileMetadata.Builder pattern
    delete_file_meta = (
        jvm.org.apache.iceberg.FileMetadata.deleteFileBuilder(partition_spec)
        .ofEqualityDeletes(int_array)
        .withPartition(partition_data)
        .withPath(delete_file_path)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(file_size)
        .withRecordCount(len(delete_data))
        .build()
    )

    # Commit the delete file using RowDelta
    row_delta = table.newRowDelta()
    row_delta.addDeletes(delete_file_meta)
    row_delta.commit()

    print(f"Created equality delete file: {delete_file_path}")
    return delete_file_path


# Test 1: Basic equality delete on integer column
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_equality_delete_int (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert test data: 10 rows
spark.sql("""
INSERT INTO rest.default.test_equality_delete_int
VALUES
    (1, 'alpha', 100),
    (2, 'beta', 200),
    (3, 'gamma', 300),
    (4, 'delta', 400),
    (5, 'epsilon', 500),
    (6, 'zeta', 600),
    (7, 'eta', 700),
    (8, 'theta', 800),
    (9, 'iota', 900),
    (10, 'kappa', 1000);
""")

# Create equality delete for rows where id IN (3, 5, 7)
# This tests basic integer column equality delete
create_equality_delete_file(
    spark,
    "rest.default.test_equality_delete_int",
    delete_schema_fields=[(1, "id", "int")],
    delete_data=[(3,), (5,), (7,)],
)
# Expected remaining rows: 1, 2, 4, 6, 8, 9, 10 (count = 7)


# Test 2: String key equality delete
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_equality_delete_string (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert test data
spark.sql("""
INSERT INTO rest.default.test_equality_delete_string
VALUES
    (1, 'apple', 10),
    (2, 'banana', 20),
    (3, 'cherry', 30),
    (4, 'date', 40),
    (5, 'elderberry', 50),
    (6, 'fig', 60);
""")

# Create equality delete for rows where name IN ('banana', 'date', 'fig')
# This tests string column equality delete
create_equality_delete_file(
    spark,
    "rest.default.test_equality_delete_string",
    delete_schema_fields=[(2, "name", "string")],
    delete_data=[("banana",), ("date",), ("fig",)],
)
# Expected remaining rows: apple, cherry, elderberry (count = 3)


# Test 3: NULL handling in equality delete key columns
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_equality_delete_null (
    id     integer,
    name   string,
    value  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert test data including NULLs
spark.sql("""
INSERT INTO rest.default.test_equality_delete_null
VALUES
    (1, 'alpha', 100),
    (2, NULL, 200),
    (3, 'gamma', 300),
    (4, NULL, 400),
    (5, 'epsilon', NULL),
    (6, NULL, NULL);
""")

# Create equality delete for rows where name IS NULL
# This tests NULL handling - should delete rows 2, 4, 6
# One NULL entry will match all rows where the column is NULL
create_equality_delete_file(
    spark,
    "rest.default.test_equality_delete_null",
    delete_schema_fields=[(2, "name", "string")],
    delete_data=[(None,)],  # Single NULL deletion matches all NULL rows (2, 4, 6)
)
# Note: Per Iceberg spec, NULL = NULL for equality deletes
# Expected remaining rows: 1, 3, 5 (count = 3)


# Test 4: Multi-column equality key
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_equality_delete_multi (
    id       integer,
    category string,
    region   string,
    value    integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert test data with various combinations
spark.sql("""
INSERT INTO rest.default.test_equality_delete_multi
VALUES
    (1, 'electronics', 'US', 100),
    (2, 'electronics', 'EU', 200),
    (3, 'books', 'US', 300),
    (4, 'books', 'EU', 400),
    (5, 'electronics', 'US', 500),
    (6, 'clothing', 'APAC', 600),
    (7, 'books', 'US', 700),
    (8, 'electronics', 'EU', 800);
""")

# Create equality delete for specific (category, region) combinations:
# - ('electronics', 'US') -> deletes rows 1, 5
# - ('books', 'EU') -> deletes row 4
# This tests multi-column equality key
create_equality_delete_file(
    spark,
    "rest.default.test_equality_delete_multi",
    delete_schema_fields=[(2, "category", "string"), (3, "region", "string")],
    delete_data=[
        ("electronics", "US"),
        ("books", "EU"),
    ],
)
# Expected remaining rows: 2, 3, 6, 7, 8 (count = 5)


print("WP3.1 Equality delete test tables created successfully!")

# =============================================================================
# WP4 INSERT OVERWRITE Interop Tests
# =============================================================================
# These tables test Rust's INSERT OVERWRITE semantics and Spark's ability to
# read the results correctly.

# Table for testing Dynamic Overwrite (ReplacePartitions)
# Partitioned by category - Rust will replace partitions based on files added
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_dynamic_overwrite (
    id       integer,
    category string,
    value    integer
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data into multiple partitions
spark.sql("""
INSERT INTO rest.default.test_dynamic_overwrite
VALUES
    (1, 'electronics', 100),
    (2, 'electronics', 200),
    (3, 'books', 300),
    (4, 'books', 400),
    (5, 'clothing', 500),
    (6, 'clothing', 600);
""")
# Expected: 6 rows across 3 partitions (electronics, books, clothing)
# Rust will replace only touched partitions - e.g., replace electronics partition
# After Rust replaces electronics: (new_rows_for_electronics) + (3,4 books) + (5,6 clothing)

# Table for testing Static Overwrite (filter-based)
# Partitioned by region - Rust will delete files matching filter and add new files
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_static_overwrite (
    id     integer,
    region string,
    amount integer
)
USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data
spark.sql("""
INSERT INTO rest.default.test_static_overwrite
VALUES
    (1, 'US', 100),
    (2, 'US', 200),
    (3, 'EU', 300),
    (4, 'EU', 400),
    (5, 'APAC', 500);
""")
# Expected: 5 rows across 3 partitions (US, EU, APAC)
# Rust will overwrite where region = 'US' with new files
# After static overwrite: (new_US_rows) + (3,4 EU) + (5 APAC)

# Table for testing Dynamic Overwrite on unpartitioned table (full table replace)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_dynamic_overwrite_unpartitioned (
    id    integer,
    name  string,
    value integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data
spark.sql("""
INSERT INTO rest.default.test_dynamic_overwrite_unpartitioned
VALUES
    (1, 'alpha', 100),
    (2, 'beta', 200),
    (3, 'gamma', 300);
""")
# For unpartitioned tables, dynamic overwrite replaces ALL data
# After Rust replaces: only the new rows will exist

# Table for testing Static Overwrite with empty result (delete-only)
# This tests the case where we overwrite a partition with no new files (effectively deleting it)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_static_overwrite_empty (
    id       integer,
    category string,
    value    integer
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version'='2'
);
""")

spark.sql("""
INSERT INTO rest.default.test_static_overwrite_empty
VALUES
    (1, 'keep', 100),
    (2, 'keep', 200),
    (3, 'delete', 300),
    (4, 'delete', 400);
""")
# Static overwrite with filter category='delete' and no new files
# After: rows 1,2 (keep) should remain, rows 3,4 (delete) should be gone

print("WP4 INSERT OVERWRITE test tables created successfully!")

# =============================================================================
# WP6.1 Schema Evolution Interop Tests (Spark DDL -> Rust Reads)
# =============================================================================
# These tables test Rust's ability to correctly read tables after Spark performs
# schema evolution operations: ADD COLUMN, DROP COLUMN, RENAME COLUMN.
# The key test is field ID semantics - Iceberg uses field IDs (not names) for
# column resolution.

# Test 1: ADD COLUMN - Rust reads table after Spark adds columns
# Tests: new columns are NULL for old data, populated for new data
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_add_column (
    id     integer,
    name   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data with original schema (snapshot 1)
spark.sql("""
INSERT INTO rest.default.test_schema_add_column
VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie');
""")

# Add columns - Spark assigns new field IDs
spark.sql("ALTER TABLE rest.default.test_schema_add_column ADD COLUMN age integer")
spark.sql("ALTER TABLE rest.default.test_schema_add_column ADD COLUMN email string")

# Insert data with new schema (snapshot 2)
spark.sql("""
INSERT INTO rest.default.test_schema_add_column
VALUES
    (4, 'Diana', 25, 'diana@example.com'),
    (5, 'Eve', 30, 'eve@example.com');
""")
# Expected: 5 rows total
# Rows 1-3: id, name populated; age=NULL, email=NULL
# Rows 4-5: all columns populated


# Test 2: DROP COLUMN - Rust reads table after Spark drops a column
# Tests: dropped column is not visible, field ID tracking still correct
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_drop_column (
    id          integer,
    name        string,
    to_drop     string,
    value       integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data with original schema (snapshot 1)
spark.sql("""
INSERT INTO rest.default.test_schema_drop_column
VALUES
    (1, 'Alice', 'will_be_dropped', 100),
    (2, 'Bob', 'will_be_dropped', 200),
    (3, 'Charlie', 'will_be_dropped', 300);
""")

# Drop column - Spark marks field as deleted in schema
spark.sql("ALTER TABLE rest.default.test_schema_drop_column DROP COLUMN to_drop")

# Insert data with new schema (snapshot 2)
spark.sql("""
INSERT INTO rest.default.test_schema_drop_column
VALUES
    (4, 'Diana', 400),
    (5, 'Eve', 500);
""")
# Expected: 5 rows total
# Schema should only show: id, name, value (no to_drop column)
# All rows should have id, name, value correctly populated


# Test 3: ADD COLUMN with DEFAULT - tests default value handling
# Note: Iceberg default values are part of spec v2+
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_add_column_default (
    id     integer,
    name   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data
spark.sql("""
INSERT INTO rest.default.test_schema_add_column_default
VALUES
    (1, 'Alpha'),
    (2, 'Beta');
""")

# Add column with default value (Spark may not support defaults yet)
default_supported = True
try:
    spark.sql(
        "ALTER TABLE rest.default.test_schema_add_column_default ADD COLUMN status string DEFAULT 'active'"
    )
except Exception as exc:
    print(f"Default column values not supported; falling back. Error: {exc}")
    spark.sql(
        "ALTER TABLE rest.default.test_schema_add_column_default ADD COLUMN status string"
    )
    default_supported = False

# Insert new data (status will have default or be explicitly provided)
if default_supported:
    spark.sql("""
    INSERT INTO rest.default.test_schema_add_column_default (id, name)
    VALUES
        (3, 'Gamma'),
        (4, 'Delta');
    """)
else:
    spark.sql("""
    INSERT INTO rest.default.test_schema_add_column_default
    VALUES
        (3, 'Gamma', 'active'),
        (4, 'Delta', 'active');
    """)

# Insert with explicit status
spark.sql("""
INSERT INTO rest.default.test_schema_add_column_default
VALUES
    (5, 'Epsilon', 'inactive');
""")
# Expected: 5 rows
# Rows 1-2: status=NULL (pre-evolution data)
# Rows 3-4: status='active' (default applied)
# Row 5: status='inactive' (explicit value)


# Test 4: Nested struct type evolution - add field to struct
# Tests complex type schema evolution
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_nested_evolution (
    id       integer,
    info     struct<name:string, city:string>
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data with original struct schema
spark.sql("""
INSERT INTO rest.default.test_schema_nested_evolution
VALUES
    (1, named_struct('name', 'Alice', 'city', 'NYC')),
    (2, named_struct('name', 'Bob', 'city', 'LA'));
""")

# Add new field to struct - Spark supports this via ADD COLUMNS
spark.sql(
    "ALTER TABLE rest.default.test_schema_nested_evolution ADD COLUMN info.country string"
)

# Insert data with evolved struct
spark.sql("""
INSERT INTO rest.default.test_schema_nested_evolution
VALUES
    (3, named_struct('name', 'Charlie', 'city', 'London', 'country', 'UK')),
    (4, named_struct('name', 'Diana', 'city', 'Paris', 'country', 'France'));
""")
# Expected: 4 rows
# Rows 1-2: info.country=NULL
# Rows 3-4: info.country populated


# Test 5: RENAME COLUMN then ADD COLUMN (combined evolution)
# Tests correct field ID tracking through multiple schema changes
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_rename_then_add (
    id         integer,
    old_name   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert initial data
spark.sql("""
INSERT INTO rest.default.test_schema_rename_then_add
VALUES
    (1, 'Alpha'),
    (2, 'Beta');
""")

# Rename column
spark.sql(
    "ALTER TABLE rest.default.test_schema_rename_then_add RENAME COLUMN old_name TO new_name"
)

# Add another column
spark.sql(
    "ALTER TABLE rest.default.test_schema_rename_then_add ADD COLUMN category string"
)

# Insert more data
spark.sql("""
INSERT INTO rest.default.test_schema_rename_then_add
VALUES
    (3, 'Gamma', 'A'),
    (4, 'Delta', 'B');
""")
# Expected: 4 rows
# Column old_name is now new_name (same field ID)
# Rows 1-2: category=NULL
# Rows 3-4: category populated


# Test 6: Multiple type promotions in sequence
# Tests int -> long -> string NOT allowed (validates rejection)
# Tests valid: int -> long (allowed)
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_type_promotion_chain (
    id     integer,
    count  integer
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Insert data as int
spark.sql("""
INSERT INTO rest.default.test_schema_type_promotion_chain
VALUES
    (1, 100),
    (2, 200);
""")

# Promote to bigint
spark.sql(
    "ALTER TABLE rest.default.test_schema_type_promotion_chain ALTER COLUMN count TYPE bigint"
)

# Insert data as bigint
spark.sql("""
INSERT INTO rest.default.test_schema_type_promotion_chain
VALUES
    (3, 3000000000),
    (4, 4000000000);
""")
# Expected: 4 rows
# All count values correctly promoted to bigint
# Row 3,4 have values > INT_MAX


# Test 7: Historical snapshot reads - test reading prior schema
# This table has multiple snapshots with different schemas
spark.sql("""
CREATE OR REPLACE TABLE rest.default.test_schema_historical (
    id     integer,
    name   string
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2'
);
""")

# Snapshot 1: original schema
spark.sql("""
INSERT INTO rest.default.test_schema_historical
VALUES
    (1, 'First');
""")

# Add column and create snapshot 2
spark.sql("ALTER TABLE rest.default.test_schema_historical ADD COLUMN version integer")
spark.sql("""
INSERT INTO rest.default.test_schema_historical
VALUES
    (2, 'Second', 2);
""")

# Rename column and create snapshot 3
spark.sql("ALTER TABLE rest.default.test_schema_historical RENAME COLUMN name TO label")
spark.sql("""
INSERT INTO rest.default.test_schema_historical
VALUES
    (3, 'Third', 3);
""")

# Record snapshot IDs for historical reads
historical_snapshots = spark.sql("""
SELECT snapshot_id, committed_at
FROM rest.default.test_schema_historical.snapshots
ORDER BY committed_at
""").collect()

print(
    f"test_schema_historical snapshots: {[(s.snapshot_id, str(s.committed_at)) for s in historical_snapshots]}"
)
# Rust should be able to read each snapshot with correct schema

print("WP6.1 Schema evolution test tables created successfully!")
