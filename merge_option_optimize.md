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