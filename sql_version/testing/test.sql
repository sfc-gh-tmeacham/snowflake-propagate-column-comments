-- *********************************************************************************************************************
-- TEST SCRIPT V4
-- *********************************************************************************************************************
-- This script provides a comprehensive, automated test for the column comment propagation project.
-- It creates a dedicated schema and a series of tables with a deep lineage structure (4 ancestors) 
-- and 20 columns to test various complex scenarios.
-- The script uses assertions to automatically verify the results, making it suitable for automated testing.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create a dedicated test schema and a deep lineage of tables
-- *********************************************************************************************************************
CREATE OR REPLACE SCHEMA TEST_SCHEMA;
USE SCHEMA TEST_SCHEMA;

-- ANCESTOR_1: The most distant ancestor, providing the baseline comments.
CREATE OR REPLACE TABLE ANCESTOR_1 (
    COL_1 VARCHAR COMMENT 'Ancestor 1 Comment for COL_1',
    COL_2 VARCHAR COMMENT 'Ancestor 1 Comment for COL_2',
    COL_3 VARCHAR COMMENT 'Ancestor 1 Comment for COL_3',
    COL_4 VARCHAR COMMENT 'Ancestor 1 Comment for COL_4',
    COL_5 VARCHAR COMMENT 'Ancestor 1 Comment for COL_5',
    COL_6 VARCHAR COMMENT 'Ancestor 1 Comment for COL_6',
    COL_7 VARCHAR COMMENT 'Ancestor 1 Comment for COL_7',
    COL_8 VARCHAR COMMENT 'Ancestor 1 Comment for COL_8',
    COL_9 VARCHAR COMMENT 'Ancestor 1 Comment for COL_9',
    COL_10 VARCHAR COMMENT 'Ancestor 1 Comment for COL_10',
    COL_11 VARCHAR COMMENT 'Ancestor 1 Comment for COL_11',
    COL_12 VARCHAR COMMENT 'Ancestor 1 Comment for COL_12',
    COL_13 VARCHAR COMMENT 'Ancestor 1 Comment for COL_13',
    COL_14 VARCHAR COMMENT 'Ancestor 1 Comment for COL_14',
    COL_15 VARCHAR COMMENT 'Ancestor 1 Comment for COL_15',
    COL_16 VARCHAR, -- No comment
    COL_17 VARCHAR, -- No comment
    COL_18 VARCHAR, -- No comment
    COL_19 VARCHAR, -- No comment
    COL_20 VARCHAR  -- No comment
);

-- ANCESTOR_2: Second-level ancestor. Overrides one comment, drops another, and adds a new one.
CREATE OR REPLACE TABLE ANCESTOR_2 AS SELECT * FROM ANCESTOR_1;
ALTER TABLE ANCESTOR_2 ALTER (
    COLUMN COL_1 COMMENT 'Ancestor 2 Comment for COL_1 (override)',
    COLUMN COL_2 COMMENT '', -- Drop comment, will be picked up from ANCESTOR_3 later
    COLUMN COL_16 COMMENT 'Ancestor 2 Comment for COL_16 (new)'
);

-- ANCESTOR_3: Third-level ancestor. Re-adds a comment, drops another, and adds a new one.
CREATE OR REPLACE TABLE ANCESTOR_3 AS SELECT * FROM ANCESTOR_2;
ALTER TABLE ANCESTOR_3 ALTER (
    COLUMN COL_2 COMMENT 'Ancestor 3 Comment for COL_2 (re-added)',
    COLUMN COL_3 COMMENT '', -- Drop comment
    COLUMN COL_17 COMMENT 'Ancestor 3 Comment for COL_17 (new)'
);

-- ANCESTOR_4: The direct parent of FINAL_TABLE. Re-adds a comment, drops another, and adds a new one.
CREATE OR REPLACE TABLE ANCESTOR_4 AS SELECT * FROM ANCESTOR_3;
ALTER TABLE ANCESTOR_4 ALTER (
    COLUMN COL_3 COMMENT 'Ancestor 4 Comment for COL_3 (re-added)',
    COLUMN COL_4 COMMENT '', -- Drop comment
    COLUMN COL_18 COMMENT 'Ancestor 4 Comment for COL_18 (new)'
);

-- FINAL_TABLE: The target table where we want to propagate comments. It has no comments to start.
CREATE OR REPLACE TABLE FINAL_TABLE AS SELECT * FROM ANCESTOR_4;

-- *********************************************************************************************************************
-- 2. EXECUTION: Run the comment propagation procedures
-- *********************************************************************************************************************
-- Use fully qualified procedure names to make the test script portable.
SET DEPLOY_DB = CURRENT_DATABASE();
SET DEPLOY_SCHEMA = 'PUBLIC'; -- Assumes procedures are in PUBLIC schema, change if needed.
SET TEST_DB = CURRENT_DATABASE();
SET TEST_SCHEMA_NAME = 'TEST_SCHEMA';
SET RECORD_PROC_FQN = $DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.RECORD_COMMENT_PROPAGATION_DATA';
SET APPLY_PROC_FQN = $DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.APPLY_COMMENT_PROPAGATION_DATA';

-- Step 2.1: Call the procedure to find and record comments for FINAL_TABLE.
SET record_call_stmt = 'CALL ' || $RECORD_PROC_FQN || ' (''' || $TEST_DB || ''', ''' || $TEST_SCHEMA_NAME || ''', ''FINAL_TABLE'')';
EXECUTE IMMEDIATE $record_call_stmt;

-- Correctly capture the RUN_ID by parsing the return value of the procedure.
SET CALL_RESULT = (SELECT "RECORD_COMMENT_PROPAGATION_DATA" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
SET RUN_ID = SPLIT_PART($CALL_RESULT, 'RUN_ID: ', 2);

-- *********************************************************************************************************************
-- 3. VERIFICATION (Part 1): Check the staging table for correct status and comments.
-- *********************************************************************************************************************
-- Expected Outcomes for FINAL_TABLE columns:
-- COMMENT_FOUND (18 columns):
--   - COL_1: from ANCESTOR_2 (dist 3)
--   - COL_2: from ANCESTOR_3 (dist 2)
--   - COL_3: from ANCESTOR_4 (dist 1)
--   - COL_4: from ANCESTOR_1 (dist 4, since it was dropped in ANCESTOR_4)
--   - COL_5 to COL_15: from ANCESTOR_1 (dist 4, untouched) (11 columns)
--   - COL_16: from ANCESTOR_2 (dist 3, new)
--   - COL_17: from ANCESTOR_3 (dist 2, new)
--   - COL_18: from ANCESTOR_4 (dist 1, new)
-- NO_COMMENT_FOUND (2 columns):
--   - COL_19, COL_20: No comments in any ancestor.

CREATE OR REPLACE TEMPORARY PROCEDURE VERIFY_STAGING_TABLE(RUN_ID_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_comment_found_count INTEGER;
  v_multiple_comments_count INTEGER;
  v_no_comment_count INTEGER;
  v_total_count INTEGER;
  assertion_failed EXCEPTION (-20001, 'An assertion failed.');
BEGIN
  SELECT
    COUNT_IF(STATUS = 'COMMENT_FOUND'),
    COUNT_IF(STATUS = 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE'),
    COUNT_IF(STATUS = 'NO_COMMENT_FOUND'),
    COUNT(*)
  INTO
    :v_comment_found_count,
    :v_multiple_comments_count,
    :v_no_comment_count,
    :v_total_count
  FROM COMMENT_PROPAGATION_STAGING
  WHERE RUN_ID = :RUN_ID_PARAM;

  -- Assert the expected counts for each status.
  IF (v_comment_found_count != 18) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_multiple_comments_count != 0) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_no_comment_count != 2) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_total_count != 20) THEN
    RAISE assertion_failed;
  END IF;

  RETURN 'Staging table verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_STAGING_TABLE. ' ||
               'COMMENT_FOUND=' || :v_comment_found_count || ' (expected 18), ' ||
               'MULTIPLE_COMMENTS=' || :v_multiple_comments_count || ' (expected 0), ' ||
               'NO_COMMENT=' || :v_no_comment_count || ' (expected 2), ' ||
               'TOTAL=' || :v_total_count || ' (expected 20)';
END;
$$;

-- Call the verification procedure
CALL VERIFY_STAGING_TABLE($RUN_ID);


-- *********************************************************************************************************************
-- 4. EXECUTION (Part 2): Apply the comments
-- *********************************************************************************************************************
SET apply_call_stmt = 'CALL ' || $APPLY_PROC_FQN || ' (''' || $RUN_ID || ''')';
EXECUTE IMMEDIATE $apply_call_stmt;


-- *********************************************************************************************************************
-- 5. VERIFICATION (Part 2): Check that comments were physically applied to the final table.
-- *********************************************************************************************************************
CREATE OR REPLACE TEMPORARY PROCEDURE VERIFY_APPLIED_COMMENTS(SCHEMA_NAME_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_applied_comment_count INTEGER;
  v_uncommented_count INTEGER;
  assertion_failed EXCEPTION (-20002, 'An assertion failed.');
BEGIN
  SELECT
    COUNT_IF(COMMENT IS NOT NULL AND COMMENT <> ''),
    COUNT_IF(COMMENT IS NULL OR COMMENT = '')
  INTO
    :v_applied_comment_count,
    :v_uncommented_count
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = :SCHEMA_NAME_PARAM AND TABLE_NAME = 'FINAL_TABLE';

  -- Assert that the correct number of comments were applied.
  IF (v_applied_comment_count != 18) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_uncommented_count != 2) THEN
    RAISE assertion_failed;
  END IF;

  RETURN 'Comment application verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_APPLIED_COMMENTS. ' ||
               'APPLIED=' || :v_applied_comment_count || ' (expected 18), ' ||
               'UNCOMMENTED=' || :v_uncommented_count || ' (expected 2)';
END;
$$;

-- Call the verification procedure
CALL VERIFY_APPLIED_COMMENTS($TEST_SCHEMA_NAME);


-- *********************************************************************************************************************
-- 6. CLEANUP: Drop the test schema and all its objects
-- *********************************************************************************************************************
DROP SCHEMA IF EXISTS TEST_SCHEMA;
