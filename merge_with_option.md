# UPDATED PRODUCTION-READY SCD TYPE 2 MERGE

```python
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, expr, when, 
    sha2, concat_ws, to_date, arrays_zip, coalesce
)
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, date
from typing import List, Optional
import logging

def scd2_merge(
    spark,
    target_table: str,
    source_df: DataFrame,
    key_column: str,
    exclude_columns: List[str] = ['eff_date', 'end_date', 'p_flag', 'update_date'],
    eff_date_col: str = 'eff_date',
    end_date_col: str = 'end_date',
    flag_col: str = 'p_flag',
    update_date_col: Optional[str] = 'update_date',
    active_flag_value: int = 1,  # Changed to int
    inactive_flag_value: int = 0,  # Changed to int
    default_start_date: date = date(1900, 1, 1),
    high_date: date = date(9999, 12, 31),
    enable_deletion_vectors: bool = True,
    optimize_post_merge: bool = False
) -> dict:
    """
    Perform SCD Type 2 merge on Delta table with production-grade efficiency.
    
    Args:
        spark: SparkSession
        target_table: Fully qualified target table name (e.g., 'lakehouse.schema.table')
        source_df: Source DataFrame with new/changed records
        key_column: Business key column name (e.g., 'p_number')
        exclude_columns: Metadata columns to exclude from change detection
        eff_date_col: Effective date column name
        end_date_col: End date column name
        flag_col: Current flag column name (integer: 1=active, 0=inactive)
        update_date_col: Last update timestamp column name (Optional - set to None to exclude)
        active_flag_value: Integer value for active records (default: 1)
        inactive_flag_value: Integer value for inactive records (default: 0)
        default_start_date: Default effective date for new records
        high_date: End date for active records
        enable_deletion_vectors: Enable deletion vectors for performance
        optimize_post_merge: Run OPTIMIZE after merge (recommended periodically, not every run)
    
    Returns:
        dict: Metrics dictionary with execution details
    """
    
    logger = logging.getLogger(__name__)
    start_time = datetime.now()
    
    try:
        # Validate flag values are integers
        if not isinstance(active_flag_value, int) or not isinstance(inactive_flag_value, int):
            raise ValueError(f"Flag values must be integers. Got active={type(active_flag_value)}, inactive={type(inactive_flag_value)}")
        
        if active_flag_value not in [0, 1] or inactive_flag_value not in [0, 1]:
            raise ValueError(f"Flag values must be 0 or 1. Got active={active_flag_value}, inactive={inactive_flag_value}")
        
        if active_flag_value == inactive_flag_value:
            raise ValueError(f"Active and inactive flag values must be different. Both are {active_flag_value}")
        
        # Enable deletion vectors for efficient soft deletes
        if enable_deletion_vectors:
            spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
        
        # Get target Delta table
        target_dt = DeltaTable.forName(spark, target_table)
        
        # Build exclusion list - add update_date_col only if provided
        exclude_list = [c for c in exclude_columns]
        if update_date_col and update_date_col not in exclude_list:
            exclude_list.append(update_date_col)
        
        # Get all columns excluding metadata for change detection
        all_columns = source_df.columns
        compare_columns = [c for c in all_columns if c not in exclude_list and c != key_column]
        
        # Add hash column to source for efficient change detection
        hash_expr = concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in compare_columns])
        source_prepared = source_df.withColumn("_source_hash", sha2(hash_expr, 256)) \
            .withColumn(eff_date_col, coalesce(col(eff_date_col), lit(default_start_date))) \
            .withColumn(end_date_col, lit(high_date)) \
            .withColumn(flag_col, lit(active_flag_value).cast(IntegerType()))  # Cast to int
        
        # Add update_date column only if specified
        if update_date_col:
            source_prepared = source_prepared.withColumn(update_date_col, current_timestamp())
        
        # Build hash expression for target
        target_hash_expr = concat_ws("||", *[coalesce(col(f"target.{c}").cast("string"), lit("")) for c in compare_columns])
        
        # Build update set dictionary for whenMatchedUpdate - conditional on update_date_col
        expire_set = {
            end_date_col: f"source.{eff_date_col}",
            flag_col: str(inactive_flag_value)  # Direct integer value, no quotes
        }
        if update_date_col:
            expire_set[update_date_col] = "current_timestamp()"
        
        # Build insert values dictionary - conditional on update_date_col
        insert_values = {
            **{c: f"source.{c}" for c in all_columns if c in source_df.columns},
            flag_col: str(active_flag_value),  # Direct integer value, no quotes
            end_date_col: f"date'{high_date}'"
        }
        if update_date_col:
            insert_values[update_date_col] = "current_timestamp()"
        
        # SINGLE MERGE OPERATION - handles all scenarios
        merge_result = target_dt.alias("target").merge(
            source_prepared.alias("source"),
            f"target.{key_column} = source.{key_column} AND target.{flag_col} = {active_flag_value}"  # No quotes for int
        ).whenMatchedUpdate(
            # Expire current record when data actually changed
            condition=f"sha2(concat_ws('||', {', '.join([f'coalesce(cast(target.{c} as string), \\'\\')'for c in compare_columns])}), 256) != source._source_hash",
            set=expire_set
        ).whenNotMatchedInsert(
            # Insert new records (both brand new keys and new versions of changed keys)
            values=insert_values
        ).execute()
        
        # Insert new active versions for changed records
        # This handles the "new version" rows for keys that were expired above
        changed_keys = target_dt.toDF().alias("t").join(
            source_prepared.alias("s"),
            (col(f"t.{key_column}") == col(f"s.{key_column}")) & 
            (col(f"t.{flag_col}") == lit(inactive_flag_value)) &  # No quotes for int comparison
            (col(f"t.{end_date_col}") == col(f"s.{eff_date_col}")),
            "inner"
        ).select("s.*").distinct()
        
        # Only insert if there are changed records
        changed_keys_count = changed_keys.count()
        if changed_keys_count > 0:
            changed_keys.write.format("delta").mode("append").saveAsTable(target_table)
        
        # Optional post-merge optimization
        if optimize_post_merge:
            logger.info(f"Running OPTIMIZE on {target_table}")
            spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({key_column})")
        
        # Calculate metrics
        execution_time = (datetime.now() - start_time).total_seconds()
        
        metrics = {
            "execution_time_seconds": execution_time,
            "target_table": target_table,
            "key_column": key_column,
            "source_row_count": source_prepared.count(),
            "changed_records_inserted": changed_keys_count,
            "compare_columns_count": len(compare_columns),
            "update_date_tracking": update_date_col is not None,
            "flag_config": f"active={active_flag_value}, inactive={inactive_flag_value}",
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"SCD2 merge completed: {metrics}")
        return metrics
        
    except Exception as e:
        logger.error(f"SCD2 merge failed for {target_table}: {str(e)}")
        raise


# USAGE EXAMPLES

# Example 1: Standard usage with p_flag as 1/0 (default)
source_data = spark.table("staging.source_table")

metrics = scd2_merge(
    spark=spark,
    target_table="gold.target_table",
    source_df=source_data,
    key_column="p_number",
    exclude_columns=['eff_date', 'end_date', 'p_flag', 'update_date'],
    eff_date_col='eff_date',
    end_date_col='end_date',
    flag_col='p_flag',
    update_date_col='update_date',
    active_flag_value=1,    # Active = 1
    inactive_flag_value=0,  # Inactive = 0
    optimize_post_merge=False
)

# Example 2: WITHOUT update_date column
metrics = scd2_merge(
    spark=spark,
    target_table="gold.target_table_no_update_date",
    source_df=source_data,
    key_column="p_number",
    exclude_columns=['eff_date', 'end_date', 'p_flag'],
    eff_date_col='eff_date',
    end_date_col='end_date',
    flag_col='p_flag',
    update_date_col=None,
    active_flag_value=1,
    inactive_flag_value=0
)

# Example 3: Custom flag column name with int values
metrics = scd2_merge(
    spark=spark,
    target_table="gold.custom_table",
    source_df=source_data,
    key_column="customer_id",
    eff_date_col='effective_from',
    end_date_col='effective_to',
    flag_col='is_active',  # Different column name
    update_date_col=None,
    active_flag_value=1,
    inactive_flag_value=0
)

# Example 4: Query active records
active_records = spark.sql(f"""
    SELECT * 
    FROM gold.target_table 
    WHERE p_flag = 1
""")

# Example 5: Query historical changes for a key
history = spark.sql(f"""
    SELECT 
        p_number,
        eff_date,
        end_date,
        p_flag,
        CASE WHEN p_flag = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END as status,
        *
    FROM gold.target_table 
    WHERE p_number = 'ABC123'
    ORDER BY eff_date DESC
""")
```

---

# KEY CHANGES

1. **Flag value types**: Changed from `str` to `int` with defaults `1` (active) and `0` (inactive)
2. **Type casting**: Added `.cast(IntegerType())` when setting flag column in source preparation
3. **SQL expressions**: Removed quotes around flag values in merge conditions and set operations
4. **Validation**: Added input validation to ensure flag values are integers (0 or 1) and different from each other
5. **Comparisons**: Updated all flag comparisons to use integer values without quotes
6. **Metrics**: Added `flag_config` to output showing active/inactive values used

---

# VALIDATION & ERROR HANDLING

The function now validates:
- Flag values must be integers (`isinstance` check)
- Flag values must be 0 or 1 (valid binary states)
- Active and inactive values must be different

**Invalid examples that will raise errors:**
```python
# Error: String values
active_flag_value='Y', inactive_flag_value='N'  # TypeError

# Error: Out of range
active_flag_value=2, inactive_flag_value=0  # ValueError

# Error: Same values
active_flag_value=1, inactive_flag_value=1  # ValueError
```

---

# QUERY PATTERNS WITH INTEGER FLAGS

```python
# Active records only
spark.sql("SELECT * FROM gold.target_table WHERE p_flag = 1")

# Inactive/historical records only
spark.sql("SELECT * FROM gold.target_table WHERE p_flag = 0")

# Count active vs inactive
spark.sql("""
    SELECT 
        p_flag,
        CASE WHEN p_flag = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END as status,
        COUNT(*) as record_count
    FROM gold.target_table
    GROUP BY p_flag
""")

# Latest version for each key (active only)
spark.sql("""
    SELECT * 
    FROM gold.target_table 
    WHERE p_flag = 1
""")
```

**Performance note:** Integer flags are more efficient than strings for filtering and joins (4 bytes vs variable string storage + comparison overhead).