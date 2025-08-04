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
CREATE OR REPLACE DATABASE COLUMN_PROPIGATE_DEV;

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
    STATUS VARCHAR COMMENT 'The status of the comment propagation for this column. One of COMMENT_FOUND, NO_COMMENT_FOUND, or MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE.',
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
  err_msg VARCHAR;

  -- Variables for dynamic query generation
  db_info_schema_fqn VARCHAR;
  tables_view_fqn VARCHAR;
  columns_view_fqn VARCHAR;

BEGIN
  -- Validate that input parameters are not NULL.
  IF (P_DATABASE_NAME IS NULL OR P_SCHEMA_NAME IS NULL OR P_TABLE_NAME IS NULL) THEN
    err_msg := 'ERROR in RECORD_COMMENT_PROPAGATION_DATA: Input parameters cannot be NULL.';
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
    err_msg := 'ERROR: Table ' || table_fqn || ' not found.';
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

    -- Step 2: Create the lineage table and get lineage for all uncommented columns in a single query.
    SYSTEM$ADD_EVENT('Step 2: Discover lineage - Started');
    CREATE OR REPLACE TEMPORARY TABLE temp_lineage (
        source_column_fqn VARCHAR,
        TARGET_OBJECT_DATABASE VARCHAR,
        TARGET_OBJECT_SCHEMA VARCHAR,
        TARGET_OBJECT_NAME VARCHAR,
        TARGET_COLUMN_NAME VARCHAR,
        DISTANCE INTEGER
    );

    -- Dynamically construct a single query that unions all GET_LINEAGE calls.
    DECLARE
      lineage_union_query VARCHAR;
      full_lineage_query VARCHAR;
    BEGIN
      SELECT LISTAGG(
          'SELECT ''' || REPLACE(c.source_column_fqn, '''', '''''') || ''', ' ||
          'l.SOURCE_OBJECT_DATABASE, l.SOURCE_OBJECT_SCHEMA, l.SOURCE_OBJECT_NAME, l.SOURCE_COLUMN_NAME, l.DISTANCE ' ||
          'FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(''' || REPLACE(c.source_column_fqn, '''', '''''') || ''', ''COLUMN'', ''UPSTREAM'')) l',
          ' UNION ALL '
      )
      INTO :lineage_union_query
      FROM temp_uncommented_columns c;

      IF (lineage_union_query IS NOT NULL AND lineage_union_query <> '') THEN
          full_lineage_query := 'INSERT INTO temp_lineage (source_column_fqn, TARGET_OBJECT_DATABASE, TARGET_OBJECT_SCHEMA, TARGET_OBJECT_NAME, TARGET_COLUMN_NAME, DISTANCE) ' || lineage_union_query;
          EXECUTE IMMEDIATE :full_lineage_query;
      END IF;
    END;

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

    -- Step 5: Gather all upstream comments in a single dynamic query.
    SYSTEM$ADD_EVENT('Step 5: Gather comments - Started');
    DECLARE
      get_comments_query VARCHAR;
    BEGIN
      SELECT LISTAGG(
          'SELECT DISTINCT icc.table_catalog, icc.table_schema, icc.table_name, icc.column_name, icc.comment ' ||
          'FROM ' || SAFE_QUOTE(uo.TARGET_OBJECT_DATABASE) || '.INFORMATION_SCHEMA.COLUMNS AS icc ' ||
          'JOIN temp_lineage tl ' ||
          'ON tl.TARGET_OBJECT_DATABASE = icc.table_catalog ' ||
          'AND tl.TARGET_OBJECT_SCHEMA = icc.table_schema ' ||
          'AND tl.TARGET_OBJECT_NAME = icc.table_name ' ||
          'AND tl.TARGET_COLUMN_NAME = icc.column_name ' ||
          'WHERE icc.comment IS NOT NULL AND icc.comment <> ''''',
          ' UNION ALL '
      )
      INTO :get_comments_query
      FROM (SELECT DISTINCT TARGET_OBJECT_DATABASE FROM temp_upstream_objects) uo;

      IF (get_comments_query IS NOT NULL AND get_comments_query <> '') THEN
        EXECUTE IMMEDIATE 'INSERT INTO temp_all_upstream_column_comments (table_catalog, table_schema, table_name, column_name, comment) ' || get_comments_query;
      END IF;
    END;

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
    -- 1. Find the minimum distance to any ancestor for each column.
    min_lineage_distance AS (
        SELECT source_column_fqn, MIN(DISTANCE) as min_distance
        FROM temp_lineage
        GROUP BY source_column_fqn
    ),
    -- 2. Count how many parents exist at that closest distance.
    closest_parent_counts AS (
        SELECT
            mld.source_column_fqn,
            COUNT(tl.TARGET_COLUMN_NAME) as parent_count
        FROM min_lineage_distance mld
        JOIN temp_lineage tl ON mld.source_column_fqn = tl.source_column_fqn AND mld.min_distance = tl.DISTANCE
        GROUP BY mld.source_column_fqn
    ),
    -- 3. For columns with a single parent, find the comment.
    single_parent_details AS (
        SELECT
            tl.source_column_fqn,
            c.table_catalog AS target_database_name,
            c.table_schema AS target_schema_name,
            c.table_name AS target_table_name,
            c.column_name AS target_column_name,
            SAFE_QUOTE(c.table_catalog) || '.' || SAFE_QUOTE(c.table_schema) || '.' || SAFE_QUOTE(c.table_name) || '.' || SAFE_QUOTE(c.column_name) as target_column_fqn,
            c.comment AS target_comment,
            tl.DISTANCE AS lineage_distance
        FROM temp_lineage tl
        LEFT JOIN temp_all_upstream_column_comments c
          ON tl.TARGET_OBJECT_DATABASE = c.table_catalog
          AND tl.TARGET_OBJECT_SCHEMA = c.table_schema
          AND tl.TARGET_OBJECT_NAME = c.table_name
          AND tl.TARGET_COLUMN_NAME = c.column_name
        WHERE (tl.source_column_fqn, tl.DISTANCE) IN (SELECT source_column_fqn, min_distance FROM min_lineage_distance)
          AND tl.source_column_fqn IN (SELECT source_column_fqn FROM closest_parent_counts WHERE parent_count = 1)
    )
    -- 4. Combine all logic to determine the final status for each uncommented column.
    SELECT
        :run_id,
        uc.source_database_name,
        uc.source_schema_name,
        uc.source_table_name,
        uc.source_column_name,
        uc.source_column_fqn,
        spd.target_database_name,
        spd.target_schema_name,
        spd.target_table_name,
        spd.target_column_name,
        spd.target_column_fqn,
        spd.target_comment,
        spd.lineage_distance,
        CASE
            WHEN cpc.parent_count > 1 THEN 'MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE'
            WHEN spd.target_comment IS NOT NULL AND spd.target_comment <> '' THEN 'COMMENT_FOUND'
            ELSE 'NO_COMMENT_FOUND'
        END as status
    FROM temp_uncommented_columns uc
    LEFT JOIN closest_parent_counts cpc ON uc.source_column_fqn = cpc.source_column_fqn
    LEFT JOIN single_parent_details spd ON uc.source_column_fqn = spd.source_column_fqn;

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
  err_msg VARCHAR;
BEGIN
  v_application_timestamp := CURRENT_TIMESTAMP();
  -- Validate that the RUN_ID is not NULL.
  IF (P_RUN_ID IS NULL) THEN
    err_msg := 'ERROR in APPLY_COMMENT_PROPAGATION_DATA: Input RUN_ID cannot be NULL.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  SYSTEM$LOG_INFO('Starting APPLY_COMMENT_PROPAGATION_DATA for RUN_ID: ' || P_RUN_ID);

  -- Check if the RUN_ID exists in the staging table to provide a better error message.
  SELECT COUNT(1) INTO :v_run_id_exists
  FROM COMMENT_PROPAGATION_STAGING
  WHERE RUN_ID = :P_RUN_ID;

  IF (v_run_id_exists = 0) THEN
    err_msg := 'ERROR: RUN_ID ' || P_RUN_ID || ' not found in COMMENT_PROPAGATION_STAGING.';
    SYSTEM$LOG_FATAL(err_msg);
    RETURN err_msg;
  END IF;

  -- Since each RUN_ID corresponds to a single table, get the table info directly
  DECLARE
    v_source_database_name VARCHAR;
    v_source_schema_name VARCHAR;
    v_source_table_name VARCHAR;
    v_comments_to_apply INTEGER;
  BEGIN
    -- Get the table information and count of comments to apply
    SELECT DISTINCT
        SOURCE_DATABASE_NAME,
        SOURCE_SCHEMA_NAME,
        SOURCE_TABLE_NAME,
        COUNT(*) OVER () as comments_to_apply
    INTO
        :v_source_database_name,
        :v_source_schema_name,
        :v_source_table_name,
        :v_comments_to_apply
    FROM COMMENT_PROPAGATION_STAGING
    WHERE RUN_ID = :P_RUN_ID AND STATUS = 'COMMENT_FOUND' AND APPLICATION_STATUS IS NULL
    LIMIT 1;

    -- If no records found, exit early
    IF (v_comments_to_apply = 0 OR v_source_database_name IS NULL) THEN
      LET success_msg := 'No comments to apply for RUN_ID: ' || P_RUN_ID;
      SYSTEM$LOG_INFO(success_msg);
      RETURN success_msg;
    END IF;

    v_table_fqn := SAFE_QUOTE(v_source_database_name) || '.' || SAFE_QUOTE(v_source_schema_name) || '.' || SAFE_QUOTE(v_source_table_name);
    
    -- Dynamically construct a single ALTER TABLE statement that updates all column comments for the table in one operation.
    SELECT
      'ALTER TABLE ' || :v_table_fqn || ' MODIFY ' ||
      LISTAGG(
          CONCAT('COLUMN ', SAFE_QUOTE(SOURCE_COLUMN_NAME), ' COMMENT ''', REPLACE(TARGET_COMMENT, '''', ''''''), ''''),
          ', '
      )
    INTO
      alter_sql
    FROM COMMENT_PROPAGATION_STAGING
    WHERE RUN_ID = :P_RUN_ID
      AND STATUS = 'COMMENT_FOUND'
      AND APPLICATION_STATUS IS NULL;

    -- Execute the dynamic DDL. By wrapping this in its own BEGIN/EXCEPTION block,
    -- we ensure that a failure does not halt the entire procedure.
    BEGIN
        SYSTEM$LOG_INFO('Executing: ' || alter_sql);
        EXECUTE IMMEDIATE alter_sql;
        
        UPDATE COMMENT_PROPAGATION_STAGING
        SET APPLICATION_STATUS = 'APPLIED', APPLICATION_TIMESTAMP = :v_application_timestamp
        WHERE RUN_ID = :P_RUN_ID
          AND STATUS = 'COMMENT_FOUND'
          AND APPLICATION_STATUS IS NULL;
        
        total_comments_applied := total_comments_applied + v_comments_to_apply;
    EXCEPTION
        WHEN OTHER THEN
            -- Capture the list of columns that failed to be updated for better logging.
            SELECT LISTAGG(SOURCE_COLUMN_NAME, ', ')
            INTO :v_failed_columns
            FROM COMMENT_PROPAGATION_STAGING
            WHERE RUN_ID = :P_RUN_ID
              AND STATUS = 'COMMENT_FOUND'
              AND APPLICATION_STATUS IS NULL;

            err_msg := 'Failed to apply ' || v_comments_to_apply || ' comment(s) for table ' || v_table_fqn || '. Columns: [' || :v_failed_columns || ']. Error: ' || SQLERRM;
            SYSTEM$LOG_ERROR(err_msg);

            UPDATE COMMENT_PROPAGATION_STAGING
            SET APPLICATION_STATUS = 'SKIPPED', APPLICATION_TIMESTAMP = :v_application_timestamp
            WHERE RUN_ID = :P_RUN_ID
              AND STATUS = 'COMMENT_FOUND'
              AND APPLICATION_STATUS IS NULL;

            total_comments_skipped := total_comments_skipped + v_comments_to_apply;
    END;
  END;

  LET success_msg := 'Success: Applied ' || total_comments_applied || ' comments and skipped ' || total_comments_skipped || ' for RUN_ID: ' || P_RUN_ID;
  SYSTEM$LOG_INFO(success_msg);
  RETURN success_msg;

EXCEPTION
    WHEN OTHER THEN
        err_msg := 'ERROR in APPLY_COMMENT_PROPAGATION_DATA for RUN_ID ' || P_RUN_ID || ': ' || SQLERRM;
        SYSTEM$LOG_FATAL(err_msg);
        RETURN err_msg;
END;
$$;
