-- *********************************************************************************************************************
-- DEPLOYMENT SCRIPT
-- *********************************************************************************************************************
-- This script deploys all the necessary objects for the column comment propagation project.
-- It should be run in a session where the user has the necessary privileges to create objects.
-- The objects will be created in the current database and schema.
--
-- PERMISSIONS: This procedure relies on SNOWFLAKE.CORE.GET_LINEAGE and the INFORMATION_SCHEMA.
-- The role that creates and runs this procedure must have the necessary privileges to access this data.
-- Specifically, it needs USAGE on all upstream databases to query their INFORMATION_SCHEMA.
-- It is recommended to use a role with broad read privileges or a custom role with specific USAGE grants.
-- *********************************************************************************************************************


USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS COLUMN_PROPIGATE_DEV;

USE ROLE ACCOUNTADMIN;

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
  v_count INTEGER DEFAULT 0;

  -- Variables for dynamic query generation
  db_info_schema_fqn VARCHAR;
  tables_view_fqn VARCHAR;
  columns_view_fqn VARCHAR;
  insert_query VARCHAR;
  db_name VARCHAR;

  -- Cursor variables
  c1 CURSOR FOR SELECT source_column_fqn FROM temp_uncommented_columns;
  v_column_fqn VARCHAR;
  
  c2 CURSOR FOR SELECT DISTINCT TARGET_OBJECT_DATABASE FROM temp_upstream_objects WHERE TARGET_OBJECT_DATABASE IS NOT NULL;

BEGIN
  -- Validate that input parameters are not NULL.
  IF (P_DATABASE_NAME IS NULL OR P_SCHEMA_NAME IS NULL OR P_TABLE_NAME IS NULL) THEN
    LET err_msg := 'ERROR in RECORD_COMMENT_PROPAGATION_DATA: Input parameters cannot be NULL.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  -- Set span attributes for the main procedure execution.
  SYSTEM$SET_SPAN_ATTRIBUTES({'target_database': :P_DATABASE_NAME, 'target_schema': :P_SCHEMA_NAME, 'target_table': :P_TABLE_NAME});

  table_fqn := SAFE_QUOTE(P_DATABASE_NAME) || '.' || SAFE_QUOTE(P_SCHEMA_NAME) || '.' || SAFE_QUOTE(P_TABLE_NAME);
  db_info_schema_fqn := SAFE_QUOTE(P_DATABASE_NAME) || '.INFORMATION_SCHEMA';
  tables_view_fqn := db_info_schema_fqn || '.TABLES';
  columns_view_fqn := db_info_schema_fqn || '.COLUMNS';
  SYSTEM$ADD_EVENT('Procedure Started', {'target_table_fqn': table_fqn});

  -- Check if table exists using INFORMATION_SCHEMA.
  SELECT COUNT(1) INTO :v_table_exists
  FROM IDENTIFIER(:tables_view_fqn)
  WHERE TABLE_SCHEMA = :P_SCHEMA_NAME AND TABLE_NAME = :P_TABLE_NAME;

  IF (v_table_exists = 0) THEN
    LET err_msg := 'ERROR: Table ' || table_fqn || ' not found.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  run_id := UUID_STRING();
  SYSTEM$SET_SPAN_ATTRIBUTES({'run_id': :run_id});
  SYSTEM$ADD_EVENT('RUN_ID Generated', {'run_id': :run_id});

  -- Wrap the core logic in a block to ensure cleanup happens.
  BEGIN

    -- Step 1: Find uncommented columns.
    SYSTEM$ADD_EVENT('Step 1: Find uncommented columns - Started');
    CREATE OR REPLACE TEMPORARY TABLE temp_uncommented_columns AS
      SELECT
        TABLE_CATALOG AS source_database_name,
        TABLE_SCHEMA AS source_schema_name,
        TABLE_NAME AS source_table_name,
        COLUMN_NAME AS source_column_name,
        SAFE_QUOTE(TABLE_CATALOG) || '.' || SAFE_QUOTE(TABLE_SCHEMA) || '.' || SAFE_QUOTE(TABLE_NAME) || '.' || SAFE_QUOTE(COLUMN_NAME) as source_column_fqn
      FROM IDENTIFIER(:columns_view_fqn)
      WHERE TABLE_SCHEMA = :P_SCHEMA_NAME
        AND TABLE_NAME = :P_TABLE_NAME
        AND (COMMENT IS NULL OR COMMENT = '');
    v_count := SQLROWCOUNT;
    SYSTEM$ADD_EVENT('Step 1: Find uncommented columns - Finished', {'uncommented_column_count': :v_count});

    -- Step 2: Create the lineage table and iterate through each uncommented column to get its lineage.
    SYSTEM$ADD_EVENT('Step 2: Discover lineage - Started');
    CREATE OR REPLACE TEMPORARY TABLE temp_lineage (
        source_column_fqn VARCHAR,
        TARGET_OBJECT_DATABASE VARCHAR,
        TARGET_OBJECT_SCHEMA VARCHAR,
        TARGET_OBJECT_NAME VARCHAR,
        TARGET_COLUMN_NAME VARCHAR,
        DISTANCE INTEGER
    );

    OPEN c1;
    FETCH c1 INTO v_column_fqn;
    WHILE (v_column_fqn IS NOT NULL) DO
        INSERT INTO temp_lineage (source_column_fqn, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME, TARGET_COLUMN_NAME, DISTANCE)
        SELECT :v_column_fqn, l.TARGET_OBJECT_DATABASE, l.TARGET_OBJECT_SCHEMA, l.TARGET_OBJECT_NAME, l.TARGET_COLUMN_NAME, l.DISTANCE
        FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(:v_column_fqn, 'COLUMN', 'UPSTREAM')) l;
        FETCH c1 INTO v_column_fqn;
    END WHILE;
    CLOSE c1;

    SELECT COUNT(*) INTO :v_count FROM temp_lineage;
    SYSTEM$ADD_EVENT('Step 2: Discover lineage - Finished', {'lineage_path_count': :v_count});

    -- Step 3: Get distinct upstream objects that might contain comments.
    SYSTEM$ADD_EVENT('Step 3: Identify unique sources - Started');
    CREATE OR REPLACE TEMPORARY TABLE temp_upstream_objects AS
    SELECT DISTINCT
        TARGET_OBJECT_DATABASE,
        TARGET_OBJECT_SCHEMA,
        TARGET_OBJECT_NAME
    FROM temp_lineage
    WHERE TARGET_OBJECT_DATABASE IS NOT NULL;
    v_count := SQLROWCOUNT;
    SYSTEM$ADD_EVENT('Step 3: Identify unique sources - Finished', {'unique_source_object_count': :v_count});

    -- Step 4: Create a table to hold the comments for the relevant upstream columns.
    SYSTEM$ADD_EVENT('Step 4: Prepare comments table - Started');
    CREATE OR REPLACE TEMPORARY TABLE temp_all_upstream_column_comments (
        table_catalog VARCHAR,
        table_schema VARCHAR,
        table_name VARCHAR,
        column_name VARCHAR,
        comment VARCHAR
    );
    SYSTEM$ADD_EVENT('Step 4: Prepare comments table - Finished');

    -- Step 5: For each upstream database, get comments only for the specific objects identified in the lineage.
    SYSTEM$ADD_EVENT('Step 5: Gather comments - Started');
    LET temp_table_for_dynamic_sql := 'temp_all_upstream_column_comments_' || REPLACE(UUID_STRING(), '-', '_');
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || temp_table_for_dynamic_sql || ' (table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, comment VARCHAR)';
    
    OPEN c2;
    FETCH c2 INTO db_name;
    WHILE (db_name IS NOT NULL) DO
        SYSTEM$ADD_EVENT('Querying for comments', {'database_name': :db_name});
        insert_query := 'INSERT INTO ' || temp_table_for_dynamic_sql || ' (table_catalog, table_schema, table_name, column_name, comment) ' ||
                          'SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.comment ' ||
                          'FROM ' || SAFE_QUOTE(db_name) || '.INFORMATION_SCHEMA.COLUMNS c ' ||
                          'WHERE c.comment IS NOT NULL AND c.comment <> '''' AND EXISTS (' ||
                          '  SELECT 1 FROM temp_upstream_objects uo ' ||
                          '  WHERE c.table_catalog = uo.TARGET_OBJECT_DATABASE ' ||
                          '    AND c.table_schema = uo.TARGET_OBJECT_SCHEMA ' ||
                          '    AND c.table_name = uo.TARGET_OBJECT_NAME' ||
                          ')';
        EXECUTE IMMEDIATE :insert_query;
        FETCH c2 INTO db_name;
    END WHILE;
    CLOSE c2;
    
    EXECUTE IMMEDIATE 'INSERT INTO temp_all_upstream_column_comments SELECT * FROM ' || temp_table_for_dynamic_sql;
    EXECUTE IMMEDIATE 'DROP TABLE ' || temp_table_for_dynamic_sql;

    SELECT COUNT(*) INTO :v_count FROM temp_all_upstream_column_comments;
    SYSTEM$ADD_EVENT('Step 5: Gather comments - Finished', {'total_comments_found': :v_count});


    -- Step 6: Join the lineage with the comments, rank them, and insert the final results into the staging table.
    SYSTEM$ADD_EVENT('Step 6: Stage results - Started');
    INSERT INTO COMMENT_PROPAGATION_STAGING (
        RUN_ID,
        SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN_NAME, SOURCE_COLUMN_FQN,
        TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN_NAME, TARGET_COLUMN_FQN,
        TARGET_COMMENT, LINEAGE_DISTANCE, STATUS
    )
    WITH
    lineage_with_comments AS (
        SELECT
            lin.source_column_fqn,
            c.table_catalog AS target_database_name,
            c.table_schema AS target_schema_name,
            c.table_name AS target_table_name,
            c.column_name AS target_column_name,
            SAFE_QUOTE(c.table_catalog) || '.' || SAFE_QUOTE(c.table_schema) || '.' || SAFE_QUOTE(c.table_name) || '.' || SAFE_QUOTE(c.column_name) as target_column_fqn,
            c.comment AS target_comment,
            lin.DISTANCE AS lineage_distance
        FROM temp_lineage lin
        JOIN temp_all_upstream_column_comments c
          ON lin.TARGET_OBJECT_DATABASE = c.table_catalog
          AND lin.TARGET_OBJECT_SCHEMA = c.table_schema
          AND lin.TARGET_OBJECT_NAME = c.table_name
          AND lin.TARGET_COLUMN_NAME = c.column_name
    ),
    ranked_lineage AS (
        SELECT
            *,
            COUNT(*) OVER (PARTITION BY source_column_fqn, lineage_distance) as comments_at_this_distance,
            ROW_NUMBER() OVER (PARTITION BY source_column_fqn ORDER BY lineage_distance) as rn
        FROM lineage_with_comments
    ),
    COMMENT_PROPAGATION_LOGIC AS (
        SELECT
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
        FROM temp_uncommented_columns uc
        LEFT JOIN ranked_lineage rl
          ON uc.source_column_fqn = rl.source_column_fqn AND rl.rn = 1
    )
    SELECT
        :run_id,
        source_database_name,
        source_schema_name,
        source_table_name,
        source_column_name,
        source_column_fqn,
        target_database_name,
        target_schema_name,
        target_table_name,
        target_column_name,
        target_column_fqn,
        target_comment,
        lineage_distance,
        status
    FROM COMMENT_PROPAGATION_LOGIC;

      rows_inserted := SQLROWCOUNT;

    SELECT COUNT_IF(STATUS = 'COMMENT_FOUND') INTO :v_count FROM COMMENT_PROPAGATION_STAGING WHERE RUN_ID = :run_id;
    SYSTEM$ADD_EVENT('Step 6: Stage results - Finished', {'rows_inserted': :rows_inserted, 'actionable_comments': :v_count});

  EXCEPTION
    WHEN OTHER THEN
        RAISE; 
  END;

  LET success_msg := 'Success: Found ' || rows_inserted || ' uncommented columns. Results are under RUN_ID: ' || run_id;
  SYSTEM$LOG_INFO(success_msg);
  RETURN success_msg;
END;
$$;

-- Enable automatic tracing to capture detailed execution data in an event table.
ALTER PROCEDURE RECORD_COMMENT_PROPAGATION_DATA(VARCHAR, VARCHAR, VARCHAR) SET AUTO_EVENT_LOGGING = 'TRACING';

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
  v_failed_columns VARCHAR;
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

  -- To minimize DDL executions, iterate through each table that has pending comments.
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
    
    -- Dynamically construct a single ALTER TABLE statement that updates all column comments for the current table in one operation.
    SELECT
      'ALTER TABLE ' || :v_table_fqn || ' ALTER (' ||
      LISTAGG(
          CONCAT('COLUMN ', SAFE_QUOTE(SOURCE_COLUMN_NAME), ' SET COMMENT ''', REPLACE(TARGET_COMMENT, '''', ''''''), ''''),
          ', '
      ) || ')'
    INTO
      alter_sql
    FROM COMMENT_PROPAGATION_STAGING
    WHERE RUN_ID = :P_RUN_ID
      AND STATUS = 'COMMENT_FOUND'
      AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
      AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
      AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME;

    -- Execute the dynamic DDL. By wrapping this in its own BEGIN/EXCEPTION block,
    -- we ensure that a failure on one table does not halt the entire procedure.
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
            -- Capture the list of columns that failed to be updated for better logging.
            SELECT LISTAGG(SOURCE_COLUMN_NAME, ', ')
            INTO :v_failed_columns
            FROM COMMENT_PROPAGATION_STAGING
            WHERE RUN_ID = :P_RUN_ID
              AND STATUS = 'COMMENT_FOUND'
              AND SOURCE_DATABASE_NAME = table_rec.SOURCE_DATABASE_NAME
              AND SOURCE_SCHEMA_NAME = table_rec.SOURCE_SCHEMA_NAME
              AND SOURCE_TABLE_NAME = table_rec.SOURCE_TABLE_NAME;

            LET err_msg := 'Failed to apply ' || table_rec.comments_to_apply || ' comment(s) for table ' || v_table_fqn || '. Columns: [' || :v_failed_columns || ']. Error: ' || SQLERRM;
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
