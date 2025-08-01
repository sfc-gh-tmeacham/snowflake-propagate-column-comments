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
    TARGET_COMMENT VARCHAR COMMENT 'The comment found on the target column.',
    LINEAGE_DISTANCE INTEGER COMMENT 'The number of steps in the lineage between the source and target objects.',
    STATUS VARCHAR COMMENT 'The status of the comment propagation for this column. One of COMMENT_FOUND, NO_COMMENT_FOUND, or MULTIPLE_COMMENTS_AT_SAME_DISTANCE.',
    RECORD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'The timestamp when this record was created.',
    APPLICATION_STATUS VARCHAR COMMENT 'The status of the comment application. One of APPLIED or SKIPPED.',
    APPLICATION_TIMESTAMP TIMESTAMP_LTZ COMMENT 'The timestamp when the comment was applied or skipped.'
)
CHANGE_TRACKING = TRUE
COPY GRANTS
COMMENT = 'A staging table that records potential column comments propagated from upstream objects via data lineage.';

-- *********************************************************************************************************************
-- 3. Stored Procedures
-- These procedures contain the core logic for the comment propagation process.
-- *********************************************************************************************************************

-- Main procedure to orchestrate the comment propagation.
CREATE OR REPLACE PROCEDURE RECORD_COMMENT_PROPAGATION_DATA(P_DATABASE_NAME VARCHAR, P_SCHEMA_NAME VARCHAR, P_TABLE_NAME VARCHAR)
  COPY GRANTS
  RETURNS VARCHAR
  LANGUAGE SQL
  COMMENT = 'Orchestrates the comment propagation process by finding un-commented columns and recording their potential comments in a single operation.'
  EXECUTE AS OWNER
AS
$$
DECLARE
  run_id VARCHAR;
  table_fqn VARCHAR;
  v_table_exists INT;
  rows_inserted INTEGER DEFAULT 0;
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

  -- This single INSERT statement finds all uncommented columns, gets their lineage,
  -- finds the closest comment, determines the status, and inserts into the staging table.
  INSERT INTO COMMENT_PROPAGATION_STAGING (
      RUN_ID,
      SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, SOURCE_COLUMN_FQN,
      TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_COLUMN_FQN,
      TARGET_COMMENT, LINEAGE_DISTANCE, STATUS
  )
  WITH
  uncommented_columns AS (
    SELECT
      c.TABLE_CATALOG AS source_database_name,
      c.TABLE_SCHEMA AS source_schema_name,
      c.TABLE_NAME AS source_table_name,
      c.COLUMN_NAME AS source_column_name,
      SAFE_QUOTE(c.TABLE_CATALOG) || '.' || SAFE_QUOTE(c.TABLE_SCHEMA) || '.' || SAFE_QUOTE(c.TABLE_NAME) || '.' || SAFE_QUOTE(c.COLUMN_NAME) as source_column_fqn
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_CATALOG = :P_DATABASE_NAME
      AND c.TABLE_SCHEMA = :P_SCHEMA_NAME
      AND c.TABLE_NAME = :P_TABLE_NAME
      AND (c.COMMENT IS NULL OR c.COMMENT = '')
  ),
  lineage_with_comments AS (
      SELECT
          uc.source_column_fqn,
          l.TARGET_OBJECT_DATABASE AS target_database_name,
          l.TARGET_OBJECT_SCHEMA AS target_schema_name,
          l.TARGET_OBJECT_NAME AS target_table_name,
          l.TARGET_COLUMN_NAME AS target_column_name,
          SAFE_QUOTE(l.TARGET_OBJECT_DATABASE) || '.' || SAFE_QUOTE(l.TARGET_OBJECT_SCHEMA) || '.' || SAFE_QUOTE(l.TARGET_OBJECT_NAME) || '.' || SAFE_QUOTE(l.TARGET_COLUMN_NAME) as target_column_fqn,
          c.COMMENT AS target_comment,
          l.DISTANCE AS lineage_distance
      FROM uncommented_columns uc,
      LATERAL (SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(uc.source_column_fqn, 'COLUMN', 'UPSTREAM'))) l
      JOIN SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
        ON l.TARGET_OBJECT_DATABASE = c.TABLE_CATALOG
        AND l.TARGET_OBJECT_SCHEMA = c.TABLE_SCHEMA
        AND l.TARGET_OBJECT_NAME = c.TABLE_NAME
        AND l.TARGET_COLUMN_NAME = c.COLUMN_NAME
      WHERE c.COMMENT IS NOT NULL AND c.COMMENT <> ''
  ),
  ranked_lineage AS (
      SELECT
          *,
          COUNT(*) OVER (PARTITION BY source_column_fqn, lineage_distance) as comments_at_this_distance,
          ROW_NUMBER() OVER (PARTITION BY source_column_fqn ORDER BY lineage_distance) as rn
      FROM lineage_with_comments
  )
  SELECT
      :run_id,
      uc.source_database_name,
      uc.source_schema_name,
      uc.source_table_name,
      uc.source_column_name,
      uc.source_column_fqn,
      rl.target_database_name,
      rl.target_schema_name,
      rl.target_table_name,
      rl.target_column_name,
      rl.target_column_fqn,
      rl.target_comment,
      rl.lineage_distance,
      CASE
          WHEN rl.source_column_fqn IS NULL THEN 'NO_COMMENT_FOUND'
          WHEN rl.comments_at_this_distance > 1 THEN 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE'
          ELSE 'COMMENT_FOUND'
      END as status
  FROM uncommented_columns uc
  LEFT JOIN ranked_lineage rl
    ON uc.source_column_fqn = rl.source_column_fqn AND rl.rn = 1;

  rows_inserted := SQLROWCOUNT;

  LET success_msg := 'Success: Found ' || rows_inserted || ' uncommented columns. Results are under RUN_ID: ' || run_id;
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
  v_application_timestamp TIMESTAMP_LTZ;
BEGIN
  v_application_timestamp := CURRENT_TIMESTAMP();
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
    WHERE RUN_ID = :P_RUN_ID AND STATUS = 'COMMENT_FOUND' AND APPLICATION_STATUS IS NULL
    GROUP BY 1, 2, 3
  )
  DO
    v_table_fqn := SAFE_QUOTE(table_rec.SOURCE_DATABASE_NAME) || '.' || SAFE_QUOTE(table_rec.SOURCE_SCHEMA_NAME) || '.' || SAFE_QUOTE(table_rec.SOURCE_TABLE_NAME);
    
    -- Use LISTAGG to build the column alteration list in a single query
    SELECT
      'ALTER TABLE ' || :v_table_fqn || ' ALTER ' ||
      LISTAGG(
          'COLUMN ' || SAFE_QUOTE(SOURCE_COLUMN_NAME) || ' SET COMMENT $$' || TARGET_COMMENT || '$$',
          ', '
      )
    INTO
      alter_sql
    FROM COMMENT_PROPAGATION_STAGING
    WHERE RUN_ID = :P_RUN_ID
      AND STATUS = 'COMMENT_FOUND'
      AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
      AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
      AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME;

    BEGIN
        SYSTEM$LOG_INFO('Executing: ' || alter_sql);
        EXECUTE IMMEDIATE alter_sql;
        
        UPDATE COMMENT_PROPAGATION_STAGING
        SET APPLICATION_STATUS = 'APPLIED', APPLICATION_TIMESTAMP = :v_application_timestamp
        WHERE RUN_ID = :P_RUN_ID
          AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
          AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
          AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME;
        
        total_comments_applied := total_comments_applied + table_rec.comments_to_apply;
    EXCEPTION
        WHEN OTHER THEN
            LET err_msg := 'Failed to apply ' || table_rec.comments_to_apply || ' comment(s) for table ' || v_table_fqn || ': ' || SQLERRM;
            SYSTEM$LOG_ERROR(err_msg);

            UPDATE COMMENT_PROPAGATION_STAGING
            SET APPLICATION_STATUS = 'SKIPPED', APPLICATION_TIMESTAMP = :v_application_timestamp
            WHERE RUN_ID = :P_RUN_ID
              AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
              AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
              AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME;

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