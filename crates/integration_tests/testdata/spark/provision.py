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
    SparkSession
        .builder
        .config("spark.sql.shuffle.partitions", "1")
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

spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_deletes WHERE number = 9")

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
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE number = 9")
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE letter == 'f'")

#  Create a table, and do some renaming
spark.sql("CREATE OR REPLACE TABLE rest.default.test_rename_column (lang string) USING iceberg")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Python')")
spark.sql("ALTER TABLE rest.default.test_rename_column RENAME COLUMN lang TO language")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Java')")

#  Create a table, and do some evolution
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_column (foo int) USING iceberg")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (19)")
spark.sql("ALTER TABLE rest.default.test_promote_column ALTER COLUMN foo TYPE bigint")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (25)")

#  Create a table, and do some evolution on a partition column
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_partition_column (foo int, bar float, baz decimal(4, 2)) USING iceberg PARTITIONED BY (foo)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (19, 19.25, 19.25)")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN foo TYPE bigint")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN bar TYPE double")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN baz TYPE decimal(6, 2)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (25, 22.25, 22.25)")

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
spark.sql("ALTER TABLE rest.default.test_partition_evolution_delete ADD PARTITION FIELD category")

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
spark.sql("ALTER TABLE rest.default.test_partition_evolution_update ADD PARTITION FIELD region")

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
spark.sql("ALTER TABLE rest.default.test_partition_evolution_merge ADD PARTITION FIELD region")

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
