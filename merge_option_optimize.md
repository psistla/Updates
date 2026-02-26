```
"""
Delta Merge - SCD Type 2 with TOV2 Metadata
- Dynamic key column (passed per call)
- Hash computed at runtime (not stored in target)
- Handles 100+ columns efficiently
- tov2_updated column optional
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, to_date,
    md5, concat_ws, coalesce
)
from delta.tables import DeltaTable
import logging
import time

logger = logging.getLogger(__name__)

TOV2_COLUMNS = ["tov2_eff_date", "tov2_end_date", "tov2_updated", "tov2_flag"]


def delta_merge_scd2_tov2(
    spark,
    source_df: DataFrame,
    target_path: str,
    key_column: str,
    partition_filter: str = None,
) -> dict:
    """
    SCD Type 2 merge with runtime hash computation (no hash stored in target).
    
    Args:
        spark: Active SparkSession
        source_df: Full source dataset
        target_path: Delta table path
        key_column: Primary key column name (e.g., "position_number", "employee_id")
        partition_filter: Scope for soft-deletes (e.g., "target.region = 'US'")
    
    Behavior:
        INSERT (new key): tov2_eff_date=today, tov2_end_date=9999-12-31, tov2_flag=1
        UPDATE (changed): Close old (tov2_flag=0), Insert new (tov2_flag=1)
        UNCHANGED: No action
        SOFT DELETE: tov2_end_date=today, tov2_flag=0
    
    Returns:
        dict: {inserted, closed, unchanged, duration_seconds}
    """
    start_time = time.time()
    
    if key_column not in source_df.columns:
        raise ValueError(f"Source missing required key column: {key_column}")
    
    source_df = source_df.filter(col(key_column).isNotNull())
    
    # Clean source: remove TOV2 columns if present
    source_columns = [c for c in source_df.columns if c not in TOV2_COLUMNS]
    source_df_clean = source_df.select(*source_columns)
    
    # Compute hash on non-key columns
    hash_columns = sorted([c for c in source_columns if c != key_column])
    source_with_hash = source_df_clean.withColumn(
        "_src_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in hash_columns]))
    )
    
    # Load target and compute hash on active records
    target_table = DeltaTable.forPath(spark, target_path)
    target_df = target_table.toDF()
    target_columns = set(target_df.columns)
    
    has_tov2_updated = "tov2_updated" in target_columns
    
    # Hash target active records (same columns as source)
    target_hash_columns = sorted([c for c in target_columns if c not in TOV2_COLUMNS and c != key_column])
    target_active = target_df.filter("tov2_flag = 1").withColumn(
        "_tgt_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in target_hash_columns]))
    )
    
    # ============================================================
    # CLASSIFY ROWS
    # ============================================================
    
    # Join source and target on key
    joined = (
        source_with_hash.alias("src")
        .join(
            target_active.select(key_column, "_tgt_hash").alias("tgt"),
            on=key_column,
            how="full_outer"
        )
    )
    
    # New rows: in source, not in target
    new_rows = joined.filter(col("_tgt_hash").isNull()).select(
        *[col(f"src.{c}") for c in source_columns]
    )
    
    # Changed rows: in both, hash differs
    changed_rows = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") != col("_tgt_hash"))
    ).select(*[col(f"src.{c}") for c in source_columns])
    
    # Unchanged rows: hash matches (no action needed)
    unchanged_count = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") == col("_tgt_hash"))
    ).count()
    
    # Deleted keys: in target, not in source
    deleted_keys = joined.filter(col("_src_hash").isNull()).select(col(f"tgt.{key_column}"))
    
    # Also get keys that changed (need to close old version)
    changed_keys = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") != col("_tgt_hash"))
    ).select(col(f"src.{key_column}"))
    
    # Keys to close = changed + deleted
    keys_to_close = changed_keys.union(deleted_keys)
    
    # ============================================================
    # STEP 1: Close old versions (changed + deleted)
    # ============================================================
    
    close_count = keys_to_close.count()
    
    if close_count > 0:
        close_set = {
            "tov2_end_date": current_date(),
            "tov2_flag": lit(0),
        }
        if has_tov2_updated:
            close_set["tov2_updated"] = current_timestamp()
        
        close_condition = f"target.{key_column} = source.{key_column} AND target.tov2_flag = 1"
        if partition_filter:
            close_condition = f"({close_condition}) AND {partition_filter}"
        
        (
            target_table.alias("target")
            .merge(keys_to_close.alias("source"), close_condition)
            .whenMatchedUpdate(set=close_set)
            .execute()
        )
    
    # ============================================================
    # STEP 2: Insert new rows + new versions of changed rows
    # ============================================================
    
    rows_to_insert = new_rows.union(changed_rows)
    insert_count = rows_to_insert.count()
    
    if insert_count > 0:
        # Add TOV2 columns
        insert_df = rows_to_insert
        insert_df = insert_df.withColumn("tov2_eff_date", current_date())
        insert_df = insert_df.withColumn("tov2_end_date", to_date(lit("9999-12-31")))
        insert_df = insert_df.withColumn("tov2_flag", lit(1))
        if has_tov2_updated:
            insert_df = insert_df.withColumn("tov2_updated", current_timestamp())
        
        # Match target column order
        target_col_order = [c for c in target_df.columns]
        insert_df = insert_df.select(*target_col_order)
        
        insert_df.write.format("delta").mode("append").save(target_path)
    
    duration = round(time.time() - start_time, 2)
    
    result = {
        "inserted": insert_count,
        "closed": close_count,
        "unchanged": unchanged_count,
        "source_rows": source_df.count(),
        "duration_seconds": duration,
    }
    
    logger.info(f"SCD2 Merge complete: {result}")
    return result


# ============================================================
# USAGE EXAMPLES
# ============================================================

# Example 1: Position table with position_number as key
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_positions,
    target_path="Tables/position_master",
    key_column="position_number",
)

# Example 2: Employee table with employee_id as key
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_employees,
    target_path="Tables/employee_dim",
    key_column="employee_id",
)

# Example 3: Product table with sku as key, partition-scoped
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_products,
    target_path="Tables/product_catalog",
    key_column="sku",
    partition_filter="target.category = 'Electronics'",
)

# Example 4: Large table with 100+ columns
df_large = spark.read.parquet("Files/landing/large_dimension.parquet")
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_large,
    target_path="Tables/large_dimension",
    key_column="dim_key",
)

print(f"Inserted: {result['inserted']}, Closed: {result['closed']}, Unchanged: {result['unchanged']}")

```

# Delta Merge SCD Type 2 with TOV2 Metadata

## Overview

Production-grade PySpark Delta Lake merge implementation for Microsoft Fabric environments featuring:

- **SCD Type 2 pattern** with full history tracking
- **Runtime hash-based change detection** (no hash column in target)
- **Dynamic key column** support (passed per invocation)
- **Automatic TOV2 metadata management** with optional `tov2_updated` column
- **Handles 100+ column tables** efficiently

---

## Operation Behavior

| Scenario | Source | Target | Action | TOV2 Result |
|----------|--------|--------|--------|-------------|
| **New Row** | Exists | Missing | INSERT | `tov2_eff_date=today`, `tov2_end_date=9999-12-31`, `tov2_flag=1` |
| **Changed Row** | Exists | Exists (hash differs) | Close old + INSERT new | Old: `tov2_end_date=today`, `tov2_flag=0`<br>New: `tov2_eff_date=today`, `tov2_end_date=9999-12-31`, `tov2_flag=1` |
| **Unchanged Row** | Exists | Exists (hash matches) | No action | Preserved |
| **Soft Delete** | Missing | Exists | UPDATE (close) | `tov2_end_date=today`, `tov2_flag=0` |

---

## TOV2 Metadata Columns

| Column | Data Type | Insert Value | Update Value | Soft Delete Value | Required |
|--------|-----------|--------------|--------------|-------------------|----------|
| `tov2_eff_date` | DATE | `current_date()` | `current_date()` | Preserved | Yes |
| `tov2_end_date` | DATE | `9999-12-31` | `9999-12-31` | `current_date()` | Yes |
| `tov2_updated` | TIMESTAMP | `current_timestamp()` | `current_timestamp()` | `current_timestamp()` | **Optional** |
| `tov2_flag` | INT | `1` | `1` | `0` | Yes |

---

## Function Signature

```python
def delta_merge_scd2_tov2(
    spark,
    source_df: DataFrame,
    target_path: str,
    key_column: str,
    partition_filter: str = None,
) -> dict:
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `spark` | SparkSession | Yes | Active Spark session |
| `source_df` | DataFrame | Yes | Full source dataset (records missing from source will be soft-deleted) |
| `target_path` | str | Yes | Delta table path (`Tables/xxx` or `abfss://...`) |
| `key_column` | str | Yes | Primary key column name for merge matching |
| `partition_filter` | str | No | Predicate to scope soft-deletes (e.g., `"target.region = 'US'"`) |

### Returns

```python
{
    "inserted": int,        # New rows + new versions of changed rows
    "closed": int,          # Old versions closed + soft deletes
    "unchanged": int,       # Rows where hash matched (no action)
    "source_rows": int,     # Total source row count
    "duration_seconds": float
}
```

---

## Complete Implementation

```python
"""
Delta Merge - SCD Type 2 with TOV2 Metadata
- Dynamic key column (passed per call)
- Hash computed at runtime (not stored in target)
- Handles 100+ columns efficiently
- tov2_updated column optional
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, to_date,
    md5, concat_ws, coalesce
)
from delta.tables import DeltaTable
import logging
import time

logger = logging.getLogger(__name__)

TOV2_COLUMNS = ["tov2_eff_date", "tov2_end_date", "tov2_updated", "tov2_flag"]


def delta_merge_scd2_tov2(
    spark,
    source_df: DataFrame,
    target_path: str,
    key_column: str,
    partition_filter: str = None,
) -> dict:
    """
    SCD Type 2 merge with runtime hash computation (no hash stored in target).
    
    Args:
        spark: Active SparkSession
        source_df: Full source dataset
        target_path: Delta table path
        key_column: Primary key column name (e.g., "position_number", "employee_id")
        partition_filter: Scope for soft-deletes (e.g., "target.region = 'US'")
    
    Behavior:
        INSERT (new key): tov2_eff_date=today, tov2_end_date=9999-12-31, tov2_flag=1
        UPDATE (changed): Close old (tov2_flag=0), Insert new (tov2_flag=1)
        UNCHANGED: No action
        SOFT DELETE: tov2_end_date=today, tov2_flag=0
    
    Returns:
        dict: {inserted, closed, unchanged, duration_seconds}
    """
    start_time = time.time()
    
    if key_column not in source_df.columns:
        raise ValueError(f"Source missing required key column: {key_column}")
    
    source_df = source_df.filter(col(key_column).isNotNull())
    
    # Clean source: remove TOV2 columns if present
    source_columns = [c for c in source_df.columns if c not in TOV2_COLUMNS]
    source_df_clean = source_df.select(*source_columns)
    
    # Compute hash on non-key columns
    hash_columns = sorted([c for c in source_columns if c != key_column])
    source_with_hash = source_df_clean.withColumn(
        "_src_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in hash_columns]))
    )
    
    # Load target and compute hash on active records
    target_table = DeltaTable.forPath(spark, target_path)
    target_df = target_table.toDF()
    target_columns = set(target_df.columns)
    
    has_tov2_updated = "tov2_updated" in target_columns
    
    # Hash target active records (same columns as source)
    target_hash_columns = sorted([c for c in target_columns if c not in TOV2_COLUMNS and c != key_column])
    target_active = target_df.filter("tov2_flag = 1").withColumn(
        "_tgt_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in target_hash_columns]))
    )
    
    # ============================================================
    # CLASSIFY ROWS
    # ============================================================
    
    # Join source and target on key
    joined = (
        source_with_hash.alias("src")
        .join(
            target_active.select(key_column, "_tgt_hash").alias("tgt"),
            on=key_column,
            how="full_outer"
        )
    )
    
    # New rows: in source, not in target
    new_rows = joined.filter(col("_tgt_hash").isNull()).select(
        *[col(f"src.{c}") for c in source_columns]
    )
    
    # Changed rows: in both, hash differs
    changed_rows = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") != col("_tgt_hash"))
    ).select(*[col(f"src.{c}") for c in source_columns])
    
    # Unchanged rows: hash matches (no action needed)
    unchanged_count = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") == col("_tgt_hash"))
    ).count()
    
    # Deleted keys: in target, not in source
    deleted_keys = joined.filter(col("_src_hash").isNull()).select(col(f"tgt.{key_column}"))
    
    # Also get keys that changed (need to close old version)
    changed_keys = joined.filter(
        col("_tgt_hash").isNotNull() & 
        col("_src_hash").isNotNull() & 
        (col("_src_hash") != col("_tgt_hash"))
    ).select(col(f"src.{key_column}"))
    
    # Keys to close = changed + deleted
    keys_to_close = changed_keys.union(deleted_keys)
    
    # ============================================================
    # STEP 1: Close old versions (changed + deleted)
    # ============================================================
    
    close_count = keys_to_close.count()
    
    if close_count > 0:
        close_set = {
            "tov2_end_date": current_date(),
            "tov2_flag": lit(0),
        }
        if has_tov2_updated:
            close_set["tov2_updated"] = current_timestamp()
        
        close_condition = f"target.{key_column} = source.{key_column} AND target.tov2_flag = 1"
        if partition_filter:
            close_condition = f"({close_condition}) AND {partition_filter}"
        
        (
            target_table.alias("target")
            .merge(keys_to_close.alias("source"), close_condition)
            .whenMatchedUpdate(set=close_set)
            .execute()
        )
    
    # ============================================================
    # STEP 2: Insert new rows + new versions of changed rows
    # ============================================================
    
    rows_to_insert = new_rows.union(changed_rows)
    insert_count = rows_to_insert.count()
    
    if insert_count > 0:
        # Add TOV2 columns
        insert_df = rows_to_insert
        insert_df = insert_df.withColumn("tov2_eff_date", current_date())
        insert_df = insert_df.withColumn("tov2_end_date", to_date(lit("9999-12-31")))
        insert_df = insert_df.withColumn("tov2_flag", lit(1))
        if has_tov2_updated:
            insert_df = insert_df.withColumn("tov2_updated", current_timestamp())
        
        # Match target column order
        target_col_order = [c for c in target_df.columns]
        insert_df = insert_df.select(*target_col_order)
        
        insert_df.write.format("delta").mode("append").save(target_path)
    
    duration = round(time.time() - start_time, 2)
    
    result = {
        "inserted": insert_count,
        "closed": close_count,
        "unchanged": unchanged_count,
        "source_rows": source_df.count(),
        "duration_seconds": duration,
    }
    
    logger.info(f"SCD2 Merge complete: {result}")
    return result
```

---

## Usage Examples

### Basic Usage

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Sample source data
source_data = [
    ("POS001", "Engineering", "Senior Engineer", 155000.00, "US"),
    ("POS002", "Marketing", "Marketing Manager", 120000.00, "US"),
    ("POS004", "Engineering", "Staff Engineer", 180000.00, "US"),
]

schema = StructType([
    StructField("position_number", StringType(), False),
    StructField("department", StringType(), True),
    StructField("title", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("region", StringType(), True),
])

df_positions = spark.createDataFrame(source_data, schema)

# Execute merge
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_positions,
    target_path="Tables/position_master",
    key_column="position_number",
)

print(f"Inserted: {result['inserted']}, Closed: {result['closed']}, Unchanged: {result['unchanged']}")
```

### Different Key Columns

```python
# Employee table
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_employees,
    target_path="Tables/employee_dim",
    key_column="employee_id",
)

# Product catalog
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_products,
    target_path="Tables/product_catalog",
    key_column="sku",
)

# Customer dimension
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_customers,
    target_path="Tables/customer_dim",
    key_column="customer_key",
)
```

### Partition-Scoped Merge

```python
# Only soft-delete within US region
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_us_positions,
    target_path="Tables/position_master",
    key_column="position_number",
    partition_filter="target.region = 'US'",
)

# Multiple partition values
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_positions,
    target_path="Tables/position_master",
    key_column="position_number",
    partition_filter="target.region IN ('US', 'CA', 'UK')",
)
```

### From External Sources

```python
# From Parquet file
df_from_parquet = spark.read.parquet("Files/landing/positions_daily.parquet")
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_from_parquet,
    target_path="Tables/position_master",
    key_column="position_number",
)

# From another Delta table
df_staging = spark.read.format("delta").load("Tables/position_staging")
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_staging,
    target_path="Tables/position_master",
    key_column="position_number",
)

# From CSV with schema inference
df_csv = spark.read.option("header", True).option("inferSchema", True).csv("Files/landing/positions.csv")
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=df_csv,
    target_path="Tables/position_master",
    key_column="position_number",
)
```

---

## Target Table Requirements

### Required Schema

```sql
CREATE TABLE position_master (
    position_number STRING NOT NULL,
    -- ... business columns (any number) ...
    tov2_eff_date DATE NOT NULL,
    tov2_end_date DATE NOT NULL,
    tov2_flag INT NOT NULL
)
USING DELTA;
```

### Optional tov2_updated Column

```sql
CREATE TABLE position_master (
    position_number STRING NOT NULL,
    -- ... business columns ...
    tov2_eff_date DATE NOT NULL,
    tov2_end_date DATE NOT NULL,
    tov2_updated TIMESTAMP,  -- Optional: auto-detected
    tov2_flag INT NOT NULL
)
USING DELTA;
```

### Sample Table with 100+ Columns

```sql
CREATE TABLE large_dimension (
    dim_key STRING NOT NULL,
    col_001 STRING,
    col_002 STRING,
    -- ... col_003 to col_099 ...
    col_100 STRING,
    tov2_eff_date DATE NOT NULL,
    tov2_end_date DATE NOT NULL,
    tov2_flag INT NOT NULL
)
USING DELTA;
```

---

## Verification Queries

### Current Active Records

```sql
SELECT * 
FROM position_master 
WHERE tov2_flag = 1 
ORDER BY position_number;
```

### Historical Versions for Specific Key

```sql
SELECT 
    position_number, 
    salary, 
    tov2_eff_date, 
    tov2_end_date, 
    tov2_flag
FROM position_master 
WHERE position_number = 'POS001'
ORDER BY tov2_eff_date;
```

### Soft-Deleted Records

```sql
SELECT 
    position_number, 
    tov2_eff_date, 
    tov2_end_date, 
    tov2_flag 
FROM position_master 
WHERE tov2_flag = 0 
  AND tov2_end_date = current_date()
ORDER BY position_number;
```

### Merge History

```sql
DESCRIBE HISTORY position_master LIMIT 10;
```

### Record Counts by Status

```sql
SELECT 
    tov2_flag,
    COUNT(*) as record_count
FROM position_master
GROUP BY tov2_flag;
```

---

## Expected Table State Example

**Before Merge:**

| position_number | salary | tov2_eff_date | tov2_end_date | tov2_flag |
|-----------------|--------|---------------|---------------|-----------|
| POS001 | 150000 | 2024-01-01 | 9999-12-31 | 1 |
| POS002 | 120000 | 2024-01-01 | 9999-12-31 | 1 |
| POS003 | 95000 | 2024-01-01 | 9999-12-31 | 1 |

**Source Data:**

| position_number | salary |
|-----------------|--------|
| POS001 | 155000 | *(changed)* |
| POS002 | 120000 | *(unchanged)* |
| POS004 | 180000 | *(new)* |

**After Merge:**

| position_number | salary | tov2_eff_date | tov2_end_date | tov2_flag | Notes |
|-----------------|--------|---------------|---------------|-----------|-------|
| POS001 | 150000 | 2024-01-01 | **2025-02-26** | **0** | Old version closed |
| POS001 | **155000** | **2025-02-26** | 9999-12-31 | **1** | New version inserted |
| POS002 | 120000 | 2024-01-01 | 9999-12-31 | 1 | Unchanged |
| POS003 | 95000 | 2024-01-01 | **2025-02-26** | **0** | Soft deleted |
| POS004 | 180000 | **2025-02-26** | 9999-12-31 | **1** | New insert |

---

## Performance Optimization

### Recommended Settings by Scale

| Source Rows | Target Rows | SKU | Recommendations |
|-------------|-------------|-----|-----------------|
| <1M | <10M | F32+ | Default settings |
| 1-10M | 10-100M | F64+ | Add partition filter, cache source |
| 10-50M | 100-500M | F64+ | Repartition, broadcast threshold |
| 50M+ | 500M+ | F128+ | Partition filter mandatory, batch processing |

### Optimization Code

```python
# Cache source to avoid recomputation (3 passes over data)
source_df.cache()

# Repartition for better parallelism on large datasets
source_df = source_df.repartition(200, key_column)

# Increase broadcast threshold for smaller source tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Execute merge
result = delta_merge_scd2_tov2(
    spark=spark,
    source_df=source_df,
    target_path="Tables/large_dimension",
    key_column="dim_key",
    partition_filter="target.region = 'US'",
)

# Cleanup
source_df.unpersist()
```

### Post-Merge Optimization

```python
# Optimize table after large merge
spark.sql("""
    OPTIMIZE position_master
    ZORDER BY (position_number)
""")

# Vacuum old files (default 7-day retention)
spark.sql("VACUUM position_master")
```

---

## Critical Gotchas

| Issue | Impact | Mitigation |
|-------|--------|------------|
| Source must be **full dataset** | Missing records are soft-deleted | Use `partition_filter` for incremental loads |
| Null key values | Never match, always insert | Pre-filter: `source_df.filter(col(key_column).isNotNull())` |
| Two-phase operation | Non-atomic (close → insert) | Wrap in try/except, use Delta RESTORE on failure |
| Column order mismatch | Insert fails | Code handles via `select(*target_col_order)` |
| F32/F48 memory limits | OOM on 50M+ source rows | Repartition: `source_df.repartition(200)` |
| Concurrent merges | `ConcurrentModificationException` | Use notebook scheduler or pipeline sequencing |
| Hash column collision | ~1 in 2^128 probability | Acceptable for most use cases |

---

## Monitoring & Observability

### Audit Logging Extension

```python
def delta_merge_scd2_tov2_with_audit(
    spark,
    source_df: DataFrame,
    target_path: str,
    key_column: str,
    partition_filter: str = None,
    audit_table: str = "audit.merge_log",
) -> dict:
    """Wrapper with audit logging."""
    
    result = delta_merge_scd2_tov2(
        spark=spark,
        source_df=source_df,
        target_path=target_path,
        key_column=key_column,
        partition_filter=partition_filter,
    )
    
    # Log to audit table
    spark.sql(f"""
        INSERT INTO {audit_table} VALUES (
            '{target_path}',
            '{key_column}',
            {result['inserted']},
            {result['closed']},
            {result['unchanged']},
            {result['source_rows']},
            {result['duration_seconds']},
            current_timestamp()
        )
    """)
    
    return result
```

### Audit Table Schema

```sql
CREATE TABLE audit.merge_log (
    target_path STRING,
    key_column STRING,
    inserted_count BIGINT,
    closed_count BIGINT,
    unchanged_count BIGINT,
    source_rows BIGINT,
    duration_seconds DOUBLE,
    executed_at TIMESTAMP
)
USING DELTA;
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-02-26 | Initial release |

---

## Requirements

- **Fabric Runtime:** 1.2+ (Delta Lake 2.4+)
- **Python:** 3.10+
- **Libraries:** pyspark, delta-spark (included in Fabric)

---
