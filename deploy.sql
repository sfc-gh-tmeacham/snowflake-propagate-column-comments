-- *********************************************************************************************************************
-- DEPLOYMENT SCRIPT
-- *********************************************************************************************************************
-- This script deploys all the necessary objects for the column comment propagation project.
-- It should be run in a session where the user has the necessary privileges to create objects.
-- The objects will be created in the current database and schema.
--
-- PERMISSIONS: This procedure relies on SNOWFLAKE.ACCOUNT_USAGE views. The role that creates and
-- runs this procedure must have the necessary privileges to access this data. It is recommended
-- to use the ACCOUNTADMIN role or a custom role with imported privileges on the SNOWFLAKE database.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. `SAFE_QUOTE` Function
-- This helper function ensures that database identifiers are correctly double-quoted.
-- *********************************************************************************************************************
CREATE OR REPLACE FUNCTION SAFE_QUOTE(s VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Takes an identifier and returns a version that is safely double-quoted, handling cases where the identifier is already quoted or contains quotes.'
AS
$$
  SELECT '"' || REPLACE(TRIM(s, '"'), '"', '""') || '"'
$$;

-- *********************************************************************************************************************
-- 2. `COMMENT_PROPAGATION_STAGING` Table
-- This table stores the results of the comment propagation process.
-- *********************************************************************************************************************
CREATE OR REPLACE TABLE COMMENT_PROPAGATION_STAGING (
    RUN_ID VARCHAR COMMENT 'Unique identifier for each run of the data propagation process.',
    SOURCE_DATABASE_NAME VARCHAR COMMENT 'Database name of the source table.',
    SOURCE_SCHEMA_NAME VARCHAR COMMENT 'Schema name of the source table.',
    SOURCE_TABLE_NAME VARCHAR COMMENT 'Table name of the source table.',
    SOURCE_COLUMN_NAME VARCHAR COMMENT 'Column name in the source table that is missing a comment.',
    SOURCE_COLUMN_FQN VARCHAR COMMENT 'The fully qualified name of the source column.',
    TARGET_DATABASE_NAME VARCHAR COMMENT 'Database name of the target object where a comment was found.',
    TARGET_SCHEMA_NAME VARCHAR COMMENT 'Schema name of the target object where a comment was found.',
    TARGET_TABLE_NAME VARCHAR COMMENT 'Table name of the target object where a comment was found.',
    TARGET_COLUMN_NAME VARCHAR COMMENT 'Column name in the target object where a comment was found.',
    TARGET_COLUMN_FQN VARCHAR COMMENT 'The fully qualified name of the target column where a comment was found.',
    TARGET_COMMENT VARCHAR COMMENT 'The comment found on the target column, or a status if none was found.',
    LINEAGE_DISTANCE INTEGER COMMENT 'The number of steps in the lineage between the source and target objects.',
    HAS_MULTIPLE_COMMENTS_AT_SAME_DISTANCE BOOLEAN COMMENT 'Flag to indicate if multiple comments were found at the same lineage distance.',
    STATUS VARCHAR COMMENT 'The status of the comment propagation for this column. One of COMMENT_FOUND, NO_COMMENT_FOUND, or MULTIPLE_COMMENTS_AT_SAME_DISTANCE.',
    RECORD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'The timestamp when this record was created.'
)
CHANGE_TRACKING = TRUE
COPY GRANTS
COMMENT = 'A staging table that records potential column comments propagated from downstream objects via data lineage.';

-- *********************************************************************************************************************
-- 3. Stored Procedures
-- These procedures contain the core logic for the comment propagation process.
-- *********************************************************************************************************************

-- Helper procedure to find and record a comment for a single column.
CREATE OR REPLACE PROCEDURE FIND_AND_RECORD_COMMENT_FOR_COLUMN(P_RUN_ID VARCHAR, P_DATABASE_NAME VARCHAR, P_SCHEMA_NAME VARCHAR, P_TABLE_NAME VARCHAR, P_UNCOMMENTED_COLUMN_NAME VARCHAR)
  COPY GRANTS
  RETURNS VARCHAR
  LANGUAGE SQL
  COMMENT = 'Helper procedure that processes a single un-commented column, finds the first downstream comment via lineage, and records the result.'
  EXECUTE AS OWNER
AS
$$
DECLARE
  v_source_col_fqn VARCHAR;
  v_target_db_name VARCHAR;
  v_target_schema_name VARCHAR;
  v_target_table_name VARCHAR;
  v_target_col_name VARCHAR;
  v_target_col_fqn VARCHAR;
  v_lineage_distance INT;
  v_target_comment VARCHAR;
  v_status VARCHAR;
  v_multiple_comments_found BOOLEAN;
BEGIN
  v_source_col_fqn := SAFE_QUOTE(P_DATABASE_NAME) || '.' || SAFE_QUOTE(P_SCHEMA_NAME) || '.' || SAFE_QUOTE(P_TABLE_NAME) || '.' || SAFE_QUOTE(P_UNCOMMENTED_COLUMN_NAME);
  SYSTEM$LOG_INFO('Starting FIND_AND_RECORD_COMMENT_FOR_COLUMN for column: ' || v_source_col_fqn);

  -- NOTE: The GET_LINEAGE function relies on SNOWFLAKE.ACCOUNT_USAGE, so its results
  -- may have some latency. This query finds the closest comment and checks for duplicates at
  -- that same distance in a single operation.
  SELECT
      res.TARGET_OBJECT_DATABASE,
      res.TARGET_OBJECT_SCHEMA,
      res.TARGET_OBJECT_NAME,
      res.TARGET_COLUMN_NAME,
      res.DISTANCE,
      res.COMMENT,
      (res.comments_at_this_distance > 1)
  INTO
      v_target_db_name,
      v_target_schema_name,
      v_target_table_name,
      v_target_col_name,
      v_lineage_distance,
      v_target_comment,
      v_multiple_comments_found
  FROM (
      SELECT
          d.TARGET_OBJECT_DATABASE,
          d.TARGET_OBJECT_SCHEMA,
          d.TARGET_OBJECT_NAME,
          d.TARGET_COLUMN_NAME,
          d.DISTANCE,
          c.COMMENT,
          COUNT(*) OVER (PARTITION BY d.DISTANCE) as comments_at_this_distance
      FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(:v_source_col_fqn, 'COLUMN', 'DOWNSTREAM')) d
      JOIN SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
        ON d.TARGET_OBJECT_DATABASE = c.TABLE_CATALOG
        AND d.TARGET_OBJECT_SCHEMA = c.TABLE_SCHEMA
        AND d.TARGET_OBJECT_NAME = c.TABLE_NAME
        AND d.TARGET_COLUMN_NAME = c.COLUMN_NAME
      WHERE c.COMMENT IS NOT NULL AND c.COMMENT <> '' AND c.DELETED IS NULL
      ORDER BY d.DISTANCE
  ) res
  LIMIT 1;

  -- If no rows are found, variables will be NULL.
  IF (v_target_db_name IS NOT NULL) THEN
    v_target_col_fqn := SAFE_QUOTE(v_target_db_name) || '.' || SAFE_QUOTE(v_target_schema_name) || '.' || SAFE_QUOTE(v_target_table_name) || '.' || SAFE_QUOTE(v_target_col_name);
    IF (v_multiple_comments_found) THEN
        v_status := 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE';
        SYSTEM$LOG_WARN('Found multiple comments at same distance for ' || v_source_col_fqn || '.');
    ELSE
        v_status := 'COMMENT_FOUND';
        SYSTEM$LOG_INFO('Found single downstream comment for ' || v_source_col_fqn || ' in ' || v_target_col_fqn);
    END IF;
  ELSE
    v_status := 'NO_COMMENT_FOUND';
    SYSTEM$LOG_WARN('No downstream comment found for ' || v_source_col_fqn || '.');
  END IF;

  INSERT INTO COMMENT_PROPAGATION_STAGING (
      RUN_ID,
      SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, SOURCE_COLUMN_FQN,
      TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_COLUMN_FQN,
      TARGET_COMMENT, LINEAGE_DISTANCE, HAS_MULTIPLE_COMMENTS_AT_SAME_DISTANCE, STATUS
  ) VALUES (
      :P_RUN_ID,
      :P_DATABASE_NAME, :P_SCHEMA_NAME, :P_TABLE_NAME, :P_UNCOMMENTED_COLUMN_NAME, :v_source_col_fqn,
      :v_target_db_name, :v_target_schema_name, :v_target_table_name, :v_target_col_name, :v_target_col_fqn,
      :v_target_comment, :v_lineage_distance, :v_multiple_comments_found, :v_status
  );

  SYSTEM$LOG_INFO('Finished FIND_AND_RECORD_COMMENT_FOR_COLUMN for column: ' || v_source_col_fqn);
  RETURN 'Processed';
EXCEPTION
    WHEN OTHER THEN
        LET err_msg := 'ERROR in FIND_AND_RECORD_COMMENT_FOR_COLUMN for ' || v_source_col_fqn || ': ' || SQLERRM;
        SYSTEM$LOG_ERROR(err_msg);
        RETURN err_msg;
END;
$$;

-- Main procedure to orchestrate the comment propagation.
CREATE OR REPLACE PROCEDURE RECORD_COMMENT_PROPAGATION_DATA(P_DATABASE_NAME VARCHAR, P_SCHEMA_NAME VARCHAR, P_TABLE_NAME VARCHAR)
  COPY GRANTS
  RETURNS VARCHAR
  LANGUAGE SQL
  COMMENT = 'Orchestrates the comment propagation process by finding un-commented columns and asynchronously calling a helper procedure to find and record their potential comments.'
  EXECUTE AS OWNER
AS
$$
DECLARE
  run_id VARCHAR;
  uncommented_column_name VARCHAR;
  total_columns INTEGER DEFAULT 0;
  table_fqn VARCHAR;
  v_table_exists INT;
BEGIN
  -- Validate that input parameters are not NULL.
  IF (P_DATABASE_NAME IS NULL OR P_SCHEMA_NAME IS NULL OR P_TABLE_NAME IS NULL) THEN
    LET err_msg := 'ERROR in RECORD_COMMENT_PROPAGATION_DATA: Input parameters cannot be NULL.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  table_fqn := SAFE_QUOTE(P_DATABASE_NAME) || '.' || SAFE_QUOTE(P_SCHEMA_NAME) || '.' || SAFE_QUOTE(P_TABLE_NAME);
  SYSTEM$LOG_INFO('Starting RECORD_COMMENT_PROPAGATION_DATA for table: ' || table_fqn);

  -- Check if table exists using INFORMATION_SCHEMA for real-time results.
  SELECT COUNT(1) INTO :v_table_exists
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_CATALOG = :P_DATABASE_NAME
    AND TABLE_SCHEMA = :P_SCHEMA_NAME
    AND TABLE_NAME = :P_TABLE_NAME;

  IF (v_table_exists = 0) THEN
    LET err_msg := 'ERROR: Table ' || table_fqn || ' not found.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  run_id := UUID_STRING();
  SYSTEM$LOG_INFO('Generated RUN_ID: ' || run_id);

  -- Find uncommented columns using INFORMATION_SCHEMA for real-time results.
  FOR row_val IN (
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = :P_DATABASE_NAME
      AND TABLE_SCHEMA = :P_SCHEMA_NAME
      AND TABLE_NAME = :P_TABLE_NAME
      AND (COMMENT IS NULL OR COMMENT = '')
  )
  DO
    uncommented_column_name := row_val.COLUMN_NAME;
    SYSTEM$LOG_INFO('Dispatching async job for column: ' || uncommented_column_name);
    ASYNC CALL FIND_AND_RECORD_COMMENT_FOR_COLUMN(:run_id, :P_DATABASE_NAME, :P_SCHEMA_NAME, :P_TABLE_NAME, :uncommented_column_name);
    total_columns := total_columns + 1;
  END FOR;

  IF (total_columns > 0) THEN
    SYSTEM$LOG_INFO('Waiting for ' || total_columns || ' async jobs to complete.');
    AWAIT ALL;
    SYSTEM$LOG_INFO('All async jobs completed.');
  END IF;

  LET success_msg := 'Success: Dispatched processing for ' || total_columns || ' columns under RUN_ID: ' || run_id;
  SYSTEM$LOG_INFO(success_msg);
  RETURN success_msg;
EXCEPTION
    WHEN OTHER THEN
        LET err_msg := 'ERROR in RECORD_COMMENT_PROPAGATION_DATA for table ' || table_fqn || ': ' || SQLERRM;
        SYSTEM$LOG_FATAL(err_msg);
        RETURN err_msg;
END;
$$;

-- *********************************************************************************************************************
-- 4. `APPLY_COMMENT_PROPAGATION_DATA` Procedure
-- This procedure applies the comments found by the `RECORD_COMMENT_PROPAGATION_DATA` procedure.
-- *********************************************************************************************************************
CREATE OR REPLACE PROCEDURE APPLY_COMMENT_PROPAGATION_DATA(P_RUN_ID VARCHAR)
  COPY GRANTS
  RETURNS VARCHAR
  LANGUAGE SQL
  COMMENT = 'Applies the column comments recorded in the COMMENT_PROPAGATION_STAGING table for a given RUN_ID.'
  EXECUTE AS OWNER
AS
$$
DECLARE
  total_comments_applied INTEGER := 0;
  total_comments_skipped INTEGER := 0;
  v_run_id_exists INT;
  alter_sql VARCHAR;
  v_table_fqn VARCHAR;
BEGIN
  -- Validate that the RUN_ID is not NULL.
  IF (P_RUN_ID IS NULL) THEN
    LET err_msg := 'ERROR in APPLY_COMMENT_PROPAGATION_DATA: Input RUN_ID cannot be NULL.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  SYSTEM$LOG_INFO('Starting APPLY_COMMENT_PROPAGATION_DATA for RUN_ID: ' || P_RUN_ID);

  -- Check if the RUN_ID exists in the staging table to provide a better error message.
  SELECT COUNT(1) INTO :v_run_id_exists
  FROM COMMENT_PROPAGATION_STAGING
  WHERE RUN_ID = :P_RUN_ID;

  IF (v_run_id_exists = 0) THEN
    LET err_msg := 'ERROR: RUN_ID ' || P_RUN_ID || ' not found in COMMENT_PROPAGATION_STAGING.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  -- Group comments by table and build a single ALTER TABLE statement for each.
  FOR table_rec IN (
    SELECT
        SOURCE_DATABASE_NAME,
        SOURCE_SCHEMA_NAME,
        SOURCE_TABLE_NAME,
        COUNT(*) as comments_to_apply
    FROM COMMENT_PROPAGATION_STAGING
    WHERE RUN_ID = :P_RUN_ID AND STATUS = 'COMMENT_FOUND'
    GROUP BY 1, 2, 3
  )
  DO
    v_table_fqn := SAFE_QUOTE(table_rec.SOURCE_DATABASE_NAME) || '.' || SAFE_QUOTE(table_rec.SOURCE_SCHEMA_NAME) || '.' || SAFE_QUOTE(table_rec.SOURCE_TABLE_NAME);
    
    -- It is not possible to bind identifiers to a prepared statement, so we must build the query string dynamically.
    -- The use of SAFE_QUOTE helps prevent SQL injection.
    alter_sql := 'ALTER TABLE ' || v_table_fqn || ' ALTER ';

    FOR column_rec IN (
        SELECT
            SOURCE_COLUMN_NAME,
            TARGET_COMMENT
        FROM COMMENT_PROPAGATION_STAGING
        WHERE RUN_ID = :P_RUN_ID
          AND STATUS = 'COMMENT_FOUND'
          AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
          AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
          AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME
    )
    DO
        -- To prevent SQL injection in the comment text, we replace single quotes.
        alter_sql := alter_sql || 'COLUMN ' || SAFE_QUOTE(column_rec.SOURCE_COLUMN_NAME) || ' SET COMMENT ''' || REPLACE(column_rec.TARGET_COMMENT, '''', '''''') || ''', ';
    END FOR;

    -- Remove the trailing comma and space
    alter_sql := LEFT(alter_sql, LENGTH(alter_sql) - 2);

    BEGIN
        SYSTEM$LOG_INFO('Executing: ' || alter_sql);
        EXECUTE IMMEDIATE alter_sql;
        total_comments_applied := total_comments_applied + table_rec.comments_to_apply;
    EXCEPTION
        WHEN OTHER THEN
            LET err_msg := 'Failed to apply ' || table_rec.comments_to_apply || ' comment(s) for table ' || v_table_fqn || ': ' || SQLERRM;
            SYSTEM$LOG_ERROR(err_msg);
            total_comments_skipped := total_comments_skipped + table_rec.comments_to_apply;
    END;
  END FOR;

  LET success_msg := 'Success: Applied ' || total_comments_applied || ' comments and skipped ' || total_comments_skipped || ' for RUN_ID: ' || P_RUN_ID;
  SYSTEM$LOG_INFO(success_msg);
  RETURN success_msg;

EXCEPTION
    WHEN OTHER THEN
        LET err_msg := 'ERROR in APPLY_COMMENT_PROPAGATION_DATA for RUN_ID ' || P_RUN_ID || ': ' || SQLERRM;
        SYSTEM$LOG_FATAL(err_msg);
        RETURN err_msg;
END;
$$;
