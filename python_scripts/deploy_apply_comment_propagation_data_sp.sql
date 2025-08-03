CREATE OR REPLACE PROCEDURE APPLY_COMMENT_PROPAGATION_DATA_SP(P_RUN_ID VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.12
  PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
  HANDLER = 'apply_comment_propagation_data'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
import time
import logging
from snowflake import telemetry

logger = logging.getLogger("apply_comment_propagation_data")

def apply_comment_propagation_data(session: snowpark.Session, run_id: str) -> str:
    """
    Applies the column comments recorded in the COMMENT_PROPAGATION_STAGING table for a given RUN_ID.
    """
    try:
        # Validate that the RUN_ID is not NULL.
        if not run_id:
            err_msg = 'ERROR in apply_comment_propagation_data: Input RUN_ID cannot be None.'
            logger.critical(err_msg)
            return err_msg

        logger.info(f'Starting APPLY_COMMENT_PROPAGATION_DATA for RUN_ID: {run_id}')

        staging_table = session.table("COMMENT_PROPAGATION_STAGING")
        
        # Check if the RUN_ID exists to provide a better error message.
        run_id_exists = staging_table.filter(col("RUN_ID") == run_id).count()
        if run_id_exists == 0:
            err_msg = f'ERROR: RUN_ID {run_id} not found in COMMENT_PROPAGATION_STAGING.'
            logger.critical(err_msg)
            return err_msg

        # Filter for comments that are ready to be applied for this run.
        comments_to_apply_df = staging_table.filter(
            (col("RUN_ID") == run_id) &
            (col("STATUS") == 'COMMENT_FOUND') &
            (col("APPLICATION_STATUS").is_null())
        ).cache_result()

        if comments_to_apply_df.count() == 0:
            success_msg = f'No comments to apply for RUN_ID: {run_id}'
            logger.info(success_msg)
            return success_msg
        
        # Group by table to apply comments in batches, which is more efficient.
        comments_grouped_by_table = comments_to_apply_df.select(
            "SOURCE_DATABASE_NAME", "SOURCE_SCHEMA_NAME", "SOURCE_TABLE_NAME"
        ).distinct().collect()

        total_comments_applied = 0
        total_comments_skipped = 0
        application_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        for table_row in comments_grouped_by_table:
            db = table_row["SOURCE_DATABASE_NAME"]
            schema = table_row["SOURCE_SCHEMA_NAME"]
            table = table_row["SOURCE_TABLE_NAME"]
            
            table_fqn = f'"{db}"."{schema}"."{table}"'
            
            comments_for_this_table = comments_to_apply_df.filter(
                (col("SOURCE_DATABASE_NAME") == db) &
                (col("SOURCE_SCHEMA_NAME") == schema) &
                (col("SOURCE_TABLE_NAME") == table)
            ).collect()

            # Dynamically construct a single ALTER TABLE statement for all columns in the current table.
            alter_statements = []
            for comment_row in comments_for_this_table:
                column_name = comment_row["SOURCE_COLUMN_NAME"]
                comment_text = comment_row["TARGET_COMMENT"].replace("'", "''") # Escape single quotes for SQL.
                alter_statements.append(f'COLUMN "{column_name}" COMMENT \'{comment_text}\'')
            
            alter_sql = f"ALTER TABLE {table_fqn} MODIFY {', '.join(alter_statements)}"

            try:
                logger.info(f'Executing: {alter_sql}')
                session.sql(alter_sql).collect()

                # Update the staging table to mark the comments as applied.
                staging_table.update(
                    {
                        "APPLICATION_STATUS": lit("APPLIED"),
                        "APPLICATION_TIMESTAMP": lit(application_timestamp)
                    },
                    (col("RUN_ID") == run_id) & (col("SOURCE_TABLE_NAME") == table) & (col("STATUS") == 'COMMENT_FOUND')
                )
                total_comments_applied += len(comments_for_this_table)

            except Exception as e:
                failed_columns = [row["SOURCE_COLUMN_NAME"] for row in comments_for_this_table]
                err_msg = f"Failed to apply {len(comments_for_this_table)} comment(s) for table {table_fqn}. Columns: {failed_columns}."
                logger.error(err_msg, exc_info=True)

                # If the ALTER statement fails, mark the comments as skipped.
                staging_table.update(
                    {
                        "APPLICATION_STATUS": lit("SKIPPED"),
                        "APPLICATION_TIMESTAMP": lit(application_timestamp)
                    },
                    (col("RUN_ID") == run_id) & (col("SOURCE_TABLE_NAME") == table) & (col("STATUS") == 'COMMENT_FOUND')
                )
                total_comments_skipped += len(comments_for_this_table)

        success_msg = f"Success: Applied {total_comments_applied} comments and skipped {total_comments_skipped} for RUN_ID: {run_id}"
        logger.info(success_msg)
        return success_msg

    except Exception as e:
        err_msg = f"An unexpected error occurred in apply_comment_propagation_data for RUN_ID {run_id}: {e}"
        logger.critical(err_msg, exc_info=True)
        return err_msg
$$;
