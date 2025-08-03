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
-- 1. SETUP: Create a four-level deep, cross-database table lineage using fully qualified names.
-- *********************************************************************************************************************

-- Level 1: The original source table with well-commented columns.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L1) (
    ID INT COMMENT 'This is the unique identifier.',
    FIRST_NAME VARCHAR COMMENT 'The first name of the person.',
    LAST_NAME VARCHAR COMMENT 'The last name of the person.',
    EMAIL VARCHAR COMMENT 'The email address.'
);

-- Level 2: A downstream table in the same database.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L2) 
AS SELECT * FROM IDENTIFIER($FQN_TABLE_L1);

-- Level 3: Another downstream table in a different database with a concatenated column.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_L3) 
AS 
SELECT 
    ID, 
    FIRST_NAME,
    LAST_NAME,
    FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
    EMAIL
FROM IDENTIFIER($FQN_TABLE_L2);

-- Level 4 (Target Table): The final table, which has no comments.
CREATE OR REPLACE TABLE IDENTIFIER($FQN_TABLE_TARGET) 
AS SELECT ID, FULL_NAME, EMAIL FROM IDENTIFIER($FQN_TABLE_L3);

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
from snowflake.snowpark.functions import col, lit, concat, when, row_number, count, replace, regexp_replace, current_timestamp
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
        # It first removes any surrounding quotes and then replaces any internal quotes with double quotes.
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
            # Construct the fully qualified name (FQN) for each column.
            concat(
                safe_quote(col("TABLE_CATALOG")), lit('.'),
                safe_quote(col("TABLE_SCHEMA")), lit('.'),
                safe_quote(col("TABLE_NAME")), lit('.'),
                safe_quote(col("COLUMN_NAME"))
            ).alias("source_column_fqn")
        ).cache_result()  # Cache the result as it's used multiple times.
        
        uncommented_column_count = uncommented_columns_df.count()
        telemetry.add_event('Step 1: Find uncommented columns - Finished', {'uncommented_column_count': uncommented_column_count})

        # Step 2 & 3: For each uncommented column, trace its lineage upstream.
        # GET_LINEAGE requires a literal string for the object name, so we must loop through each column.
        telemetry.add_event('Step 2: Discover lineage - Started')
        
        uncommented_columns_list = uncommented_columns_df.select("source_column_fqn").collect()
        
        lineage_dfs = []
        for row in uncommented_columns_list:
            column_fqn = row["SOURCE_COLUMN_FQN"]
            
            # Call the GET_LINEAGE table function for the current column.
            lineage_df = session.table_function("SNOWFLAKE.CORE.GET_LINEAGE", lit(column_fqn), lit('COLUMN'), lit('UPSTREAM')).select(
                lit(column_fqn).alias("source_column_fqn"),
                col("SOURCE_OBJECT_DATABASE").alias("TARGET_OBJECT_DATABASE"),
                col("SOURCE_OBJECT_SCHEMA").alias("TARGET_OBJECT_SCHEMA"),
                col("SOURCE_OBJECT_NAME").alias("TARGET_OBJECT_NAME"),
                col("SOURCE_COLUMN_NAME").alias("TARGET_COLUMN_NAME"),
                col("DISTANCE")
            )
            lineage_dfs.append(lineage_df)

        # Union all the individual lineage DataFrames into a single DataFrame.
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
        for row in upstream_dbs:
            db_name = row['TARGET_OBJECT_DATABASE']
            telemetry.add_event('Gathering comments from database', {'database_name': db_name})
            
            # For each upstream database, find the comments for the specific columns identified in the lineage.
            info_schema_cols = session.read.table(f'"{db_name}".INFORMATION_SCHEMA.COLUMNS')
            relevant_cols_for_db = temp_lineage_df.filter(col("TARGET_OBJECT_DATABASE") == db_name).select(
                col("TARGET_OBJECT_SCHEMA"), col("TARGET_OBJECT_NAME"), col("TARGET_COLUMN_NAME")
            ).distinct()

            comments_df = info_schema_cols.join(
                relevant_cols_for_db,
                (info_schema_cols.col("table_schema") == relevant_cols_for_db.col("TARGET_OBJECT_SCHEMA")) &
                (info_schema_cols.col("table_name") == relevant_cols_for_db.col("TARGET_OBJECT_NAME")) &
                (info_schema_cols.col("column_name") == relevant_cols_for_db.col("TARGET_COLUMN_NAME"))
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
            (temp_lineage_df.col("TARGET_OBJECT_DATABASE") == all_upstream_comments_df.col("table_catalog")) &
            (temp_lineage_df.col("TARGET_OBJECT_SCHEMA") == all_upstream_comments_df.col("table_schema")) &
            (temp_lineage_df.col("TARGET_OBJECT_NAME") == all_upstream_comments_df.col("table_name")) &
            (temp_lineage_df.col("TARGET_COLUMN_NAME") == all_upstream_comments_df.col("column_name")),
            "left"
        ).select(
            temp_lineage_df.col("source_column_fqn"),
            all_upstream_comments_df.col("table_catalog").alias("target_database_name"),
            all_upstream_comments_df.col("table_schema").alias("target_schema_name"),
            all_upstream_comments_df.col("table_name").alias("target_table_name"),
            all_upstream_comments_df.col("column_name").alias("target_column_name"),
            all_upstream_comments_df.col("comment").alias("target_comment"),
            temp_lineage_df.col("DISTANCE").alias("lineage_distance")
        ).withColumn(
             "target_column_fqn",
             concat(
                safe_quote(col("target_database_name")), lit('.'),
                safe_quote(col("target_schema_name")), lit('.'),
                safe_quote(col("target_table_name")), lit('.'),
                safe_quote(col("target_column_name"))
            )
        )

        # Rank comments by lineage distance. The closest comment (smallest distance) is preferred.
        window = Window.partitionBy("source_column_fqn").orderBy("lineage_distance")
        ranked_lineage = lineage_with_comments.filter(
            col("target_comment").is_not_null() & (col("target_comment") != '')
        ).withColumn(
            "rn", row_number().over(window)
        ).withColumn(
            "comments_at_this_distance", count(lit(1)).over(Window.partitionBy("source_column_fqn", "lineage_distance"))
        )

        # Determine the final status for each column: found, not found, or multiple options.
        comment_propagation_logic = uncommented_columns_df.join(
            ranked_lineage.filter(col("rn") == 1),
            "source_column_fqn",
            "left"
        ).withColumn(
            "status",
            when(col("target_comment").is_null(), lit("NO_COMMENT_FOUND"))
            .when(col("comments_at_this_distance") > 1, lit("MULTIPLE_COMMENTS_AT_SAME_DISTANCE"))
            .otherwise(lit("COMMENT_FOUND"))
        )

        final_results_df = comment_propagation_logic.select(
            lit(run_id).alias("RUN_ID"),
            "source_database_name",
            "source_schema_name",
            "source_table_name",
            "source_column_name",
            "source_column_fqn",
            "target_database_name",
            "target_schema_name",
            "target_table_name",
            "target_column_name",
            "target_column_fqn",
            "target_comment",
            "lineage_distance",
            "status",
            current_timestamp().alias("RECORD_TIMESTAMP"),
            lit(None).cast(StringType()).alias("APPLICATION_STATUS"),
            lit(None).cast(TimestampType()).alias("APPLICATION_TIMESTAMP")
        )

        # Write the final results to the staging table.
        # The staging table name is unqualified because a procedure running with owner's rights
        # resolves unqualified objects against the procedure's own schema.
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
-- We expect to see 3 rows for ID, FULL_NAME, and EMAIL.
-- The comment for FULL_NAME should be null, as lineage is not tracked for concatenated columns.
SELECT
    SOURCE_COLUMN_NAME,
    TARGET_COMMENT,
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
