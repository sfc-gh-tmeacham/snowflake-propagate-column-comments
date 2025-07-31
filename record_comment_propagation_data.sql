-- PERMISSIONS: This procedure relies on SNOWFLAKE.ACCOUNT_USAGE views. The role that creates and
-- runs this procedure must have the necessary privileges to access this data. It is recommended
-- to use the ACCOUNTADMIN role or a custom role with imported privileges on the SNOWFLAKE database.
--
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
  v_multiple_comments_found BOOLEAN := FALSE;
  v_first_comment_distance INT;
  v_comment_found BOOLEAN := FALSE;

BEGIN
  v_source_col_fqn := SAFE_QUOTE(P_DATABASE_NAME) || '.' || SAFE_QUOTE(P_SCHEMA_NAME) || '.' || SAFE_QUOTE(P_TABLE_NAME) || '.' || SAFE_QUOTE(P_UNCOMMENTED_COLUMN_NAME);
  SYSTEM$LOG_INFO('Starting FIND_AND_RECORD_COMMENT_FOR_COLUMN for column: ' || v_source_col_fqn);

  -- NOTE: The GET_LINEAGE function relies on SNOWFLAKE.ACCOUNT_USAGE, so its results
  -- may have some latency. However, joining with ACCOUNT_USAGE.COLUMNS here is still
  -- the most reliable way to get comments for objects discovered via lineage.
  FOR row_val IN (
    SELECT
        TARGET_OBJECT_DATABASE,
        TARGET_OBJECT_SCHEMA,
        TARGET_OBJECT_NAME,
        TARGET_COLUMN_NAME,
        DISTANCE,
        COMMENT
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(v_source_col_fqn, 'COLUMN', 'DOWNSTREAM')) d
    JOIN SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
      ON d.TARGET_OBJECT_DATABASE = c.TABLE_CATALOG
      AND d.TARGET_OBJECT_SCHEMA = c.TABLE_SCHEMA
      AND d.TARGET_OBJECT_NAME = c.TABLE_NAME
      AND d.TARGET_COLUMN_NAME = c.COLUMN_NAME
    WHERE c.COMMENT IS NOT NULL AND c.COMMENT <> '' AND c.DELETED IS NULL
    ORDER BY DISTANCE
  )
  DO
    IF NOT v_comment_found THEN
      v_comment_found := TRUE;
      v_first_comment_distance := row_val.DISTANCE;
      v_target_db_name := row_val.TARGET_OBJECT_DATABASE;
      v_target_schema_name := row_val.TARGET_OBJECT_SCHEMA;
      v_target_table_name := row_val.TARGET_OBJECT_NAME;
      v_target_col_name := row_val.TARGET_COLUMN_NAME;
      v_target_col_fqn := SAFE_QUOTE(v_target_db_name) || '.' || SAFE_QUOTE(v_target_schema_name) || '.' || SAFE_QUOTE(v_target_table_name) || '.' || SAFE_QUOTE(v_target_col_name);
      v_target_comment := row_val.COMMENT;
      v_lineage_distance := row_val.DISTANCE;
      SYSTEM$LOG_INFO('Found first downstream comment for ' || v_source_col_fqn || ' in ' || v_target_col_fqn);
    ELSE
      IF row_val.DISTANCE = v_first_comment_distance THEN
        v_multiple_comments_found := TRUE;
        SYSTEM$LOG_WARN('Found multiple comments at same distance for ' || v_source_col_fqn || '.');
      END IF;
      BREAK; -- Exit after processing the first set of comments at the closest distance
    END IF;
  END FOR;

  IF v_comment_found THEN
    IF v_multiple_comments_found THEN
        v_status := 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE';
    ELSE
        v_status := 'COMMENT_FOUND';
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
      :v_target_comment, :v_lineage_distance, 
      CASE WHEN v_status = 'NO_COMMENT_FOUND' THEN NULL ELSE v_multiple_comments_found END, 
      :v_status
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
