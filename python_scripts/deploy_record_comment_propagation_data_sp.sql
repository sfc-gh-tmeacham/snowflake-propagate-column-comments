CREATE OR REPLACE PROCEDURE RECORD_COMMENT_PROPAGATION_DATA_SP(P_DATABASE_NAME VARCHAR, P_SCHEMA_NAME VARCHAR, P_TABLE_NAME VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.12
  PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
  HANDLER = 'record_comment_propagation_data'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, concat, when, row_number, count, replace, regexp_replace, current_timestamp, min
from snowflake.snowpark.types import StringType, StructType, StructField, IntegerType, TimestampType
from snowflake.snowpark.window import Window
import logging
from snowflake import telemetry
from functools import reduce

# Get a logger for this module
logger = logging.getLogger("record_comment_propagation_data")

def record_comment_propagation_data(session: snowpark.Session, database_name: str, schema_name: str, table_name: str) -> str:
    """
    Orchestrates the comment propagation process by finding un-commented columns
    and recording their potential comments in a single operation.
    """
    try:
        # Validate that input parameters are not None.
        if not all([database_name, schema_name, table_name]):
            err_msg = 'ERROR in record_comment_propagation_data: Input parameters cannot be None.'
            logger.critical(err_msg)
            return err_msg

        telemetry.set_span_attribute("target_database", database_name)
        telemetry.set_span_attribute("target_schema", schema_name)
        telemetry.set_span_attribute("target_table", table_name)

        # Use the DataFrame API to check if the table exists
        try:
            session.table(f'"{database_name}"."{schema_name}"."{table_name}"').limit(1).collect()
        except Exception:
            err_msg = f"ERROR: Table {database_name}.{schema_name}.{table_name} not found or not accessible."
            logger.critical(err_msg)
            return err_msg

        run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]
        telemetry.set_span_attribute("run_id", run_id)
        telemetry.add_event('Procedure Started', {'target_table_fqn': f'{database_name}.{schema_name}.{table_name}', 'run_id': run_id})

        # This helper function safely quotes an identifier by handling existing quotes.
        def safe_quote(c: col):
            return concat(lit('"'), replace(regexp_replace(c, '^"|"$', ''), lit('"'), lit('""')), lit('"'))

        # Step 1: Find all columns in the target table that do not have a comment.
        telemetry.add_event('Step 1: Find uncommented columns - Started')
        
        columns_df = session.read.table(f'"{database_name}".INFORMATION_SCHEMA.COLUMNS')
        
        uncommented_columns_df = columns_df.filter(
            (col("TABLE_SCHEMA") == schema_name) &
            (col("TABLE_NAME") == table_name) &
            ((col("COMMENT").is_null()) | (col("COMMENT") == ''))
        ).select(
            col("TABLE_CATALOG").alias("source_database_name"),
            col("TABLE_SCHEMA").alias("source_schema_name"),
            col("TABLE_NAME").alias("source_table_name"),
            col("COLUMN_NAME").alias("source_column_name"),
            concat(
                safe_quote(col("TABLE_CATALOG")), lit('.'),
                safe_quote(col("TABLE_SCHEMA")), lit('.'),
                safe_quote(col("TABLE_NAME")), lit('.'),
                safe_quote(col("COLUMN_NAME"))
            ).alias("source_column_fqn")
        ).cache_result()
        
        uncommented_column_count = uncommented_columns_df.count()
        telemetry.add_event('Step 1: Find uncommented columns - Finished', {'uncommented_column_count': uncommented_column_count})

        # Step 2 & 3: Discover lineage for each uncommented column.
        telemetry.add_event('Step 2: Discover lineage - Started')
        
        uncommented_columns_list = uncommented_columns_df.select("source_column_fqn").collect()
        
        lineage_dfs = []
        for row in uncommented_columns_list:
            column_fqn = row["SOURCE_COLUMN_FQN"]
            lineage_df = session.table_function("SNOWFLAKE.CORE.GET_LINEAGE", lit(column_fqn), lit('COLUMN'), lit('UPSTREAM')).select(
                lit(column_fqn).alias("source_column_fqn"),
                col("SOURCE_OBJECT_DATABASE").alias("TARGET_OBJECT_DATABASE"),
                col("SOURCE_OBJECT_SCHEMA").alias("TARGET_OBJECT_SCHEMA"),
                col("SOURCE_OBJECT_NAME").alias("TARGET_OBJECT_NAME"),
                col("SOURCE_COLUMN_NAME").alias("TARGET_COLUMN_NAME"),
                col("DISTANCE")
            )
            lineage_dfs.append(lineage_df)

        if not lineage_dfs:
            lineage_schema = StructType([
                StructField("source_column_fqn", StringType()),
                StructField("TARGET_OBJECT_DATABASE", StringType()),
                StructField("TARGET_OBJECT_SCHEMA", StringType()),
                StructField("TARGET_OBJECT_NAME", StringType()),
                StructField("TARGET_COLUMN_NAME", StringType()),
                StructField("DISTANCE", IntegerType())
            ])
            temp_lineage_df = session.create_dataframe([], schema=lineage_schema)
        else:
            temp_lineage_df = reduce(lambda df1, df2: df1.unionAll(df2), lineage_dfs)

        lineage_path_count = temp_lineage_df.count()
        telemetry.add_event('Step 2: Discover lineage - Finished', {'lineage_path_count': lineage_path_count})

        # Accurately count parents at the closest distance before considering comments.
        closest_distance_df = temp_lineage_df.groupBy("source_column_fqn").agg(min("DISTANCE").alias("min_distance"))
        
        parent_count_df = temp_lineage_df.join(
            closest_distance_df,
            "source_column_fqn"
        ).filter(
            col("DISTANCE") == col("min_distance")
        ).groupBy("source_column_fqn").agg(count("*").alias("parents_at_closest_distance"))

        # Step 4 & 5: Gather comments from all unique upstream databases and tables.
        telemetry.add_event('Step 5: Gather comments - Started')

        upstream_dbs = temp_lineage_df.select("TARGET_OBJECT_DATABASE").distinct().filter(col("TARGET_OBJECT_DATABASE").is_not_null()).collect()
        
        comment_dfs = []
        for row in upstream_dbs:
            db_name = row['TARGET_OBJECT_DATABASE']
            info_schema_cols = session.read.table(f'"{db_name}".INFORMATION_SCHEMA.COLUMNS')
            relevant_cols_for_db = temp_lineage_df.filter(col("TARGET_OBJECT_DATABASE") == db_name).select(
                col("TARGET_OBJECT_SCHEMA"), col("TARGET_OBJECT_NAME"), col("TARGET_COLUMN_NAME")
            ).distinct()
            comments_df = info_schema_cols.join(
                relevant_cols_for_db,
                (info_schema_cols.table_schema == relevant_cols_for_db.TARGET_OBJECT_SCHEMA) &
                (info_schema_cols.table_name == relevant_cols_for_db.TARGET_OBJECT_NAME) &
                (info_schema_cols.column_name == relevant_cols_for_db.TARGET_COLUMN_NAME)
            ).filter(col("comment").is_not_null() & (col("comment") != '')).select(
                "table_catalog", "table_schema", "table_name", "column_name", "comment"
            )
            comment_dfs.append(comments_df)

        if not comment_dfs:
            all_upstream_comments_df = session.create_dataframe([], schema=StructType([StructField("table_catalog", StringType()), StructField("table_schema", StringType()), StructField("table_name", StringType()), StructField("column_name", StringType()), StructField("comment", StringType())]))
        else:
            all_upstream_comments_df = reduce(lambda df1, df2: df1.unionAll(df2), comment_dfs)

        total_comments_found = all_upstream_comments_df.count()
        telemetry.add_event('Step 5: Gather comments - Finished', {'total_comments_found': total_comments_found})

        # Step 6: Join lineage with comments and apply ranking logic to find the best comment.
        telemetry.add_event('Step 6: Stage results - Started')
        
        lineage_with_comments = temp_lineage_df.join(
            all_upstream_comments_df,
            (temp_lineage_df.TARGET_OBJECT_DATABASE == all_upstream_comments_df.table_catalog) &
            (temp_lineage_df.TARGET_OBJECT_SCHEMA == all_upstream_comments_df.table_schema) &
            (temp_lineage_df.TARGET_OBJECT_NAME == all_upstream_comments_df.table_name) &
            (temp_lineage_df.TARGET_COLUMN_NAME == all_upstream_comments_df.column_name),
            "left"
        ).select(
            temp_lineage_df.source_column_fqn,
            all_upstream_comments_df.table_catalog.alias("target_database_name"),
            all_upstream_comments_df.table_schema.alias("target_schema_name"),
            all_upstream_comments_df.table_name.alias("target_table_name"),
            all_upstream_comments_df.column_name.alias("target_column_name"),
            all_upstream_comments_df.comment.alias("target_comment"),
            temp_lineage_df.DISTANCE.alias("lineage_distance")
        ).withColumn(
             "target_column_fqn",
             concat(
                safe_quote(col("target_database_name")), lit('.'),
                safe_quote(col("target_schema_name")), lit('.'),
                safe_quote(col("target_table_name")), lit('.'),
                safe_quote(col("target_column_name"))
            )
        )

        window = Window.partitionBy("source_column_fqn").orderBy("lineage_distance")
        ranked_lineage = lineage_with_comments.filter(
            col("target_comment").is_not_null() & (col("target_comment") != '')
        ).withColumn(
            "rn", row_number().over(window)
        ).withColumn(
            "comments_at_this_distance", count(lit(1)).over(Window.partitionBy("source_column_fqn", "lineage_distance"))
        )

        comment_propagation_logic = uncommented_columns_df.join(
            ranked_lineage.filter(col("rn") == 1), "source_column_fqn", "left"
        ).join(
            parent_count_df, "source_column_fqn", "left"
        ).withColumn(
            "status",
            when(col("target_comment").is_null(), lit("NO_COMMENT_FOUND"))
            .when(col("parents_at_closest_distance") > 1, lit("MULTIPLE_PARENTS_FOUND"))
            .when(col("comments_at_this_distance") > 1, lit("MULTIPLE_COMMENTS_AT_SAME_DISTANCE"))
            .otherwise(lit("COMMENT_FOUND"))
        )

        final_results_df = comment_propagation_logic.select(
            lit(run_id).alias("RUN_ID"),
            "source_database_name", "source_schema_name", "source_table_name", "source_column_name", "source_column_fqn",
            "target_database_name", "target_schema_name", "target_table_name", "target_column_name", "target_column_fqn",
            when(col("status") == "MULTIPLE_PARENTS_FOUND", lit(None).cast(StringType())).otherwise(col("target_comment")).alias("target_comment"),
            "lineage_distance", "status",
            current_timestamp().alias("RECORD_TIMESTAMP"),
            lit(None).cast(StringType()).alias("APPLICATION_STATUS"),
            lit(None).cast(TimestampType()).alias("APPLICATION_TIMESTAMP")
        )

        final_results_df.write.mode("append").save_as_table(table_name="COMMENT_PROPAGATION_STAGING")
        rows_inserted = final_results_df.count()

        actionable_comments = final_results_df.filter(col("status") == 'COMMENT_FOUND').count()
        telemetry.add_event('Step 6: Stage results - Finished', {'rows_inserted': rows_inserted, 'actionable_comments': actionable_comments})

        success_msg = f"Success: Found {rows_inserted} uncommented columns. Results are under RUN_ID: {run_id}"
        logger.info(success_msg)
        return success_msg

    except Exception as e:
        err_msg = f"An unexpected error occurred in record_comment_propagation_data: {e}"
        logger.critical(err_msg, exc_info=True)
        return err_msg
$$;
