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
  target_db_name VARCHAR;
  target_schema_name VARCHAR;
  target_table_name VARCHAR;
  target_col_name VARCHAR;
  target_col_fqn VARCHAR;
  lineage_distance INT;
  
  target_comment VARCHAR; -- Used to hold the found comment
  comment_found BOOLEAN DEFAULT FALSE;
  
  source_col_fqn VARCHAR;
BEGIN
  -- Construct the fully qualified column name for the lineage query and for logging.
  source_col_fqn := P_DATABASE_NAME || '.' || P_SCHEMA_NAME || '.' || P_TABLE_NAME || '.' || P_UNCOMMENTED_COLUMN_NAME;
  SYSTEM$LOG_INFO('Starting FIND_AND_RECORD_COMMENT_FOR_COLUMN for column: ' || source_col_fqn);

  -- Loop through each downstream dependency, ordered by distance.
  'downstream_loop':
  FOR row_val IN (
    SELECT TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME, TARGET_COLUMN_NAME, DISTANCE
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(:source_col_fqn, 'COLUMN', 'DOWNSTREAM'))
    ORDER BY DISTANCE
  )
  DO
    target_db_name := row_val.TARGET_OBJECT_DATABASE;
    target_schema_name := row_val.TARGET_OBJECT_SCHEMA;
    target_table_name := row_val.TARGET_OBJECT_NAME;
    target_col_name := row_val.TARGET_COLUMN_NAME;
    lineage_distance := row_val.DISTANCE;
    target_col_fqn := target_db_name || '.' || target_schema_name || '.' || target_table_name || '.' || target_col_name;

    -- Query for a comment on the downstream column.
    SYSTEM$LOG_DEBUG('Executing query to find comment for column: ' || target_col_fqn);
    SELECT COMMENT INTO :target_comment
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS 
    WHERE TABLE_CATALOG = :target_db_name 
      AND TABLE_SCHEMA = :target_schema_name 
      AND TABLE_NAME = :target_table_name 
      AND COLUMN_NAME = :target_col_name 
      AND (COMMENT IS NOT NULL AND COMMENT <> '') 
      AND DELETED IS NULL 
    LIMIT 1;
    
    -- If a comment is found, record it and exit the loop.
    IF (target_comment IS NOT NULL) THEN
      comment_found := TRUE;
      SYSTEM$LOG_INFO('Found downstream comment for ' || source_col_fqn || '.');
      
      INSERT INTO COMMENT_PROPAGATION_STAGING (
          RUN_ID,
          SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, SOURCE_COLUMN_FQN,
          TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_COLUMN_FQN,
          TARGET_COMMENT, LINEAGE_DISTANCE
      ) VALUES (
          :P_RUN_ID,
          :P_DATABASE_NAME, :P_SCHEMA_NAME, :P_TABLE_NAME, :P_UNCOMMENTED_COLUMN_NAME, :source_col_fqn,
          :target_db_name, :target_schema_name, :target_table_name, :target_col_name, :target_col_fqn,
          :target_comment, :lineage_distance
      );
      
      BREAK 'downstream_loop'; -- Exit as soon as the first comment is found.
    END IF;
  END FOR;
  
  -- If no comment was found after checking all dependencies, record that outcome.
  IF (NOT comment_found) THEN
    SYSTEM$LOG_WARN('No downstream comment found for ' || source_col_fqn || '.');
    INSERT INTO COMMENT_PROPAGATION_STAGING (
        RUN_ID,
        SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, SOURCE_COLUMN_FQN,
        TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_COLUMN_FQN,
        TARGET_COMMENT, LINEAGE_DISTANCE
    ) VALUES (
        :P_RUN_ID,
        :P_DATABASE_NAME, :P_SCHEMA_NAME, :P_TABLE_NAME, :P_UNCOMMENTED_COLUMN_NAME, :source_col_fqn,
        NULL, NULL, NULL, NULL, NULL,
        'No downstream comment found', NULL
    );
  END IF;

  SYSTEM$LOG_INFO('Finished FIND_AND_RECORD_COMMENT_FOR_COLUMN for column: ' || source_col_fqn);
  RETURN 'Processed';
EXCEPTION
    WHEN OTHER THEN
        LET err_msg := 'ERROR in FIND_AND_RECORD_COMMENT_FOR_COLUMN for ' || source_col_fqn || ': ' || SQLERRM;
        SYSTEM$LOG_ERROR(err_msg);
        RETURN err_msg;
END;
$$;

-- Main procedure to orchestrate the asynchronous comment propagation.
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
BEGIN
  table_fqn := P_DATABASE_NAME || '.' || P_SCHEMA_NAME || '.' || P_TABLE_NAME;
  SYSTEM$LOG_INFO('Starting RECORD_COMMENT_PROPAGATION_DATA for table: ' || table_fqn);
  run_id := UUID_STRING();
  SYSTEM$LOG_INFO('Generated RUN_ID: ' || run_id);

  -- Step 1 & 2: Identify and asynchronously process each un-commented column.
  FOR row_val IN (
    SELECT COLUMN_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE TABLE_CATALOG = :P_DATABASE_NAME
      AND TABLE_SCHEMA = :P_SCHEMA_NAME
      AND TABLE_NAME = :P_TABLE_NAME
      AND (COMMENT IS NULL OR COMMENT = '')
      AND DELETED IS NULL
  )
  DO
    uncommented_column_name := row_val.COLUMN_NAME;
    
    SYSTEM$LOG_INFO('Dispatching async job for column: ' || uncommented_column_name);
    ASYNC CALL FIND_AND_RECORD_COMMENT_FOR_COLUMN(:run_id, :P_DATABASE_NAME, :P_SCHEMA_NAME, :P_TABLE_NAME, :uncommented_column_name);
    total_columns := total_columns + 1;

  END FOR;

  -- Step 3: Wait for all async calls to complete, if any were dispatched.
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
