-- *********************************************************************************************************************
-- TEST SCRIPT FOR RECORD_COMMENT_PROPAGATION_DATA_SP
-- *********************************************************************************************************************
-- This script provides a self-contained test for the Python stored procedure.
-- It sets up a temporary environment, runs the procedure, verifies the results, and cleans up after itself.
-- *********************************************************************************************************************

-- Set variables for the test databases to make cleanup easier.
SET (TEST_DB_NAME_1, TEST_DB_NAME_2, SCHEMA_NAME_1, SCHEMA_NAME_2) = ('COMMENT_PROP_SP_TEST_DB_1', 'COMMENT_PROP_SP_TEST_DB_2', 'TEST_SCHEMA', 'SOURCE_SCHEMA');

-- Construct the fully qualified names (FQNs) for all database objects.
SET FQN_SCHEMA_1 = $TEST_DB_NAME_1 || '.' || $SCHEMA_NAME_1;
SET FQN_SCHEMA_2 = $TEST_DB_NAME_2 || '.' || $SCHEMA_NAME_2;

SET (FQN_TABLE_L1, FQN_TABLE_L2) = ($FQN_SCHEMA_2 || '.LEVEL_1', $FQN_SCHEMA_2 || '.LEVEL_2');
SET (FQN_TABLE_L3, FQN_TABLE_TARGET, FQN_STAGING_TABLE, FQN_PROCEDURE) = (
    $FQN_SCHEMA_1 || '.LEVEL_3',
    $FQN_SCHEMA_1 || '.TARGET_TABLE',
    $FQN_SCHEMA_1 || '.COMMENT_PROPAGATION_STAGING',
    $FQN_SCHEMA_1 || '.RECORD_COMMENT_PROPAGATION_DATA_SP'
);


-- Use a role that has permissions to create databases.
USE ROLE SYSADMIN;

-- Create new databases and schemas for this test to run in isolation.
CREATE OR REPLACE DATABASE IDENTIFIER($TEST_DB_NAME_1);
CREATE OR REPLACE DATABASE IDENTIFIER($TEST_DB_NAME_2);
CREATE OR REPLACE SCHEMA IDENTIFIER($FQN_SCHEMA_1);
CREATE OR REPLACE SCHEMA IDENTIFIER($FQN_SCHEMA_2);

-- *********************************************************************************************************************
-- 1. SETUP: Create a multi-level, cross-database table lineage with comments distributed throughout.
-- *********************************************************************************************************************

-- Level 1: The original source table with some commented and some uncommented columns.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L1) (
    ID INT COMMENT 'L1 Comment: The unique identifier.',
    FIRST_NAME VARCHAR, -- No comment here, will be added downstream.
    LAST_NAME VARCHAR, -- No comment here, will be added downstream.
    EMAIL VARCHAR, -- No comment here, will be added downstream.
    STATUS VARCHAR COMMENT 'L1 Comment: The status from the source.'
);

-- Level 2: A downstream table that adds a new column and comments on an existing one.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L2)
AS SELECT * FROM IDENTIFIER($FQN_TABLE_L1);

-- Add a new column with a comment at this level.
ALTER TABLE IDENTIFIER($FQN_TABLE_L2) ADD COLUMN ADDRESS VARCHAR COMMENT 'L2 Comment: The address, added at Level 2.';
-- Add a comment to a column that was previously uncommented.
SET FQN_COLUMN_L2_FIRST_NAME = $FQN_TABLE_L2 || '.FIRST_NAME';
COMMENT ON COLUMN IDENTIFIER($FQN_COLUMN_L2_FIRST_NAME) IS 'L2 Comment: The first name of the person.';


-- Level 3: Another downstream table in a different database that adds a concatenated column and another comment.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L3)
AS
SELECT
    ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME, -- This column's lineage is untraceable.
    STATUS,
    ADDRESS
FROM IDENTIFIER($FQN_TABLE_L2);

-- Add a comment to another previously uncommented column.
SET FQN_COLUMN_L3_EMAIL = $FQN_TABLE_L3 || '.EMAIL';
COMMENT ON COLUMN IDENTIFIER($FQN_COLUMN_L3_EMAIL) IS 'L3 Comment: The email address, added at Level 3.';

-- This creates a test case where the closest comment should be chosen.
-- Let's add a conflicting comment further up the lineage to ensure the procedure picks the L3 comment.
SET FQN_COLUMN_L1_EMAIL = $FQN_TABLE_L1 || '.EMAIL';
COMMENT ON COLUMN IDENTIFIER($FQN_COLUMN_L1_EMAIL) IS 'L1 Comment: This is an older email comment that should NOT be selected.';


-- Create a separate lineage branch to test joins.
SET FQN_TABLE_L2_ALT = $FQN_SCHEMA_2 || '.LEVEL_2_ALT';
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L2_ALT) (
    EXTRA_ID INT,
    EXTRA_DATA VARCHAR COMMENT 'L2_ALT Comment: Extra data from an alternate source.'
);
SET FQN_COLUMN_L2_ALT_EXTRA_ID = $FQN_TABLE_L2_ALT || '.EXTRA_ID';
COMMENT ON COLUMN IDENTIFIER($FQN_COLUMN_L2_ALT_EXTRA_ID) IS 'L2_ALT Comment: ID from an alternate source.';


-- Level 4 (View): A view that joins the main lineage with the alternate branch.
SET FQN_VIEW_L4 = $FQN_SCHEMA_1 || '.LEVEL_4_VIEW';
CREATE OR REPLACE VIEW IDENTIFIER($FQN_VIEW_L4)
AS
SELECT
    l3.ID,
    l3.FULL_NAME,
    l3.EMAIL,
    l3.STATUS,
    l3.ADDRESS,
    alt.EXTRA_DATA,
    alt.EXTRA_ID
FROM IDENTIFIER($FQN_TABLE_L3) l3
JOIN IDENTIFIER($FQN_TABLE_L2_ALT) alt ON l3.ID = alt.EXTRA_ID; -- Join on ID

-- Final Target Table: The table we want to document.
-- All columns except FULL_NAME should be able to find a comment upstream.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_TARGET)
AS SELECT * FROM IDENTIFIER($FQN_VIEW_L4);

-- Add a comment on one column in the target table itself to ensure it is ignored by the procedure.
SET FQN_COLUMN_TARGET_ID = $FQN_TABLE_TARGET || '.ID';
COMMENT ON COLUMN IDENTIFIER($FQN_COLUMN_TARGET_ID) IS 'This ID already has a comment and should be ignored.';

-- *********************************************************************************************************************
-- 2. SETUP: Create the staging table where results will be stored.
-- *********************************************************************************************************************
CREATE OR REPLACE TABLE IDENTIFIER($FQN_STAGING_TABLE) (
    RUN_ID VARCHAR,
    SOURCE_DATABASE_NAME VARCHAR,
    SOURCE_SCHEMA_NAME VARCHAR,
    SOURCE_TABLE_NAME VARCHAR,
    SOURCE_COLUMN_NAME VARCHAR,
    SOURCE_COLUMN_FQN VARCHAR,
    TARGET_DATABASE_NAME VARCHAR,
    TARGET_SCHEMA_NAME VARCHAR,
    TARGET_TABLE_NAME VARCHAR,
    TARGET_COLUMN_NAME VARCHAR,
    TARGET_COLUMN_FQN VARCHAR,
    TARGET_COMMENT VARCHAR,
    LINEAGE_DISTANCE INTEGER,
    STATUS VARCHAR,
    RECORD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    APPLICATION_STATUS VARCHAR,
    APPLICATION_TIMESTAMP TIMESTAMP_LTZ
);

-- *********************************************************************************************************************
-- 3. DEPLOYMENT: Create the stored procedure with the inline Python code.
-- *********************************************************************************************************************
CREATE OR REPLACE PROCEDURE IDENTIFIER($FQN_PROCEDURE)(P_DATABASE_NAME VARCHAR, P_SCHEMA_NAME VARCHAR, P_TABLE_NAME VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.12
  PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
  HANDLER = 'record_comment_propagation_data'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, concat, when, row_number, count, replace, regexp_replace, current_timestamp, min, upper
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
        
        columns_df = session.read.table(f'{database_name}.INFORMATION_SCHEMA.COLUMNS')
        
        uncommented_columns_df = columns_df.filter(
            (upper(col("TABLE_SCHEMA")) == schema_name.upper()) &
            (upper(col("TABLE_NAME")) == table_name.upper()) &
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
        if uncommented_columns_list:
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

        # Step 4 & 5: Gather comments from all unique upstream databases and tables.
        telemetry.add_event('Step 5: Gather comments - Started')

        upstream_dbs = temp_lineage_df.select("TARGET_OBJECT_DATABASE").distinct().filter(col("TARGET_OBJECT_DATABASE").is_not_null()).collect()
        
        comment_dfs = []
        if upstream_dbs:
            for row in upstream_dbs:
                db_name = row['TARGET_OBJECT_DATABASE']
                info_schema_cols = session.read.table(f'{db_name}.INFORMATION_SCHEMA.COLUMNS')
                relevant_cols_for_db = temp_lineage_df.filter(upper(col("TARGET_OBJECT_DATABASE")) == db_name.upper()).select(
                    col("TARGET_OBJECT_SCHEMA"), col("TARGET_OBJECT_NAME"), col("TARGET_COLUMN_NAME")
                ).distinct()
                comments_df = info_schema_cols.join(
                    relevant_cols_for_db,
                    (upper(info_schema_cols.col("TABLE_SCHEMA")) == upper(relevant_cols_for_db.col("TARGET_OBJECT_SCHEMA"))) &
                    (upper(info_schema_cols.col("TABLE_NAME")) == upper(relevant_cols_for_db.col("TARGET_OBJECT_NAME"))) &
                    (upper(info_schema_cols.col("COLUMN_NAME")) == upper(relevant_cols_for_db.col("TARGET_COLUMN_NAME")))
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
            (upper(temp_lineage_df.TARGET_OBJECT_DATABASE) == upper(all_upstream_comments_df.table_catalog)) &
            (upper(temp_lineage_df.TARGET_OBJECT_SCHEMA) == upper(all_upstream_comments_df.table_schema)) &
            (upper(temp_lineage_df.TARGET_OBJECT_NAME) == upper(all_upstream_comments_df.table_name)) &
            (upper(temp_lineage_df.TARGET_COLUMN_NAME) == upper(all_upstream_comments_df.column_name)),
            "left"
        ).select(
            col("source_column_fqn"),
            col("table_catalog").alias("target_database_name"),
            col("table_schema").alias("target_schema_name"),
            col("table_name").alias("target_table_name"),
            col("column_name").alias("target_column_name"),
            col("comment").alias("target_comment"),
            col("DISTANCE").alias("lineage_distance")
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
        ).withColumn(
            "status",
            when(col("target_comment").is_null(), lit("NO_COMMENT_FOUND"))
            .when(col("comments_at_this_distance") > 1, lit("MULTIPLE_COMMENTS_AT_SAME_DISTANCE"))
            .otherwise(lit("COMMENT_FOUND"))
        )

        final_results_df = comment_propagation_logic.select(
            lit(run_id).alias("RUN_ID"),
            "source_database_name", "source_schema_name", "source_table_name", "source_column_name", "source_column_fqn",
            "target_database_name", "target_schema_name", "target_table_name", "target_column_name", "target_column_fqn",
            col("target_comment"),
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

-- *********************************************************************************************************************
-- 4. EXECUTION: Run the stored procedure.
-- *********************************************************************************************************************

-- Call the procedure on the final target table.
CALL IDENTIFIER($FQN_PROCEDURE)($TEST_DB_NAME_1, $SCHEMA_NAME_1, 'TARGET_TABLE');

-- *********************************************************************************************************************
-- 5. VERIFICATION: Check the results in the staging table.
-- *********************************************************************************************************************

-- Query the staging table to see if the comments were found correctly.
-- We expect the procedure to find comments for all uncommented columns except FULL_NAME.
-- - ADDRESS:       Comment from L2
-- - EMAIL:         Comment from L3 (closest)
-- - EXTRA_DATA:    Comment from L2_ALT
-- - EXTRA_ID:      Comment from L2_ALT
-- - FIRST_NAME:    Comment from L2
-- - FULL_NAME:     Will inherit comment from FIRST_NAME due to GET_LINEAGE limitation with concatenated strings.
-- - LAST_NAME:     No comment (never commented) -> NO_COMMENT_FOUND
-- - STATUS:        Comment from L1
-- The ID column is ignored because it already has a comment in the target table.
SELECT
    SOURCE_COLUMN_NAME,
    TARGET_COMMENT,
    TARGET_COLUMN_FQN,
    LINEAGE_DISTANCE,
    STATUS
FROM IDENTIFIER($FQN_STAGING_TABLE)
ORDER BY SOURCE_COLUMN_NAME;

-- *********************************************************************************************************************
-- 6. CLEANUP: Drop the test databases.
-- *********************************************************************************************************************

-- USE ROLE SYSADMIN;
-- DROP DATABASE IDENTIFIER($TEST_DB_NAME_1);
-- DROP DATABASE IDENTIFIER($TEST_DB_NAME_2);
