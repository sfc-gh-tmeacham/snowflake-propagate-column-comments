-- *********************************************************************************************************************
-- TEST SCRIPT V3
-- *********************************************************************************************************************
-- This script provides a comprehensive, automated test for the column comment propagation project.
-- It creates a dedicated schema and a series of tables with lineage to test various scenarios.
-- The script uses assertions to automatically verify the results, making it suitable for automated testing.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create a dedicated test schema and tables
-- *********************************************************************************************************************
CREATE OR REPLACE SCHEMA TEST_SCHEMA;
USE SCHEMA TEST_SCHEMA;

-- BASE_TABLE: The original source of truth for comments.
CREATE OR REPLACE TABLE BASE_TABLE (
    ID INT COMMENT 'The unique identifier for the record.',
    FIRST_NAME VARCHAR COMMENT 'The first name of the user.',
    LAST_NAME VARCHAR, -- This column intentionally has no comment.
    EMAIL VARCHAR COMMENT 'The primary email address of the user.',
    STATUS VARCHAR COMMENT 'The status of the user account.',
    CREATED_AT TIMESTAMP_NTZ COMMENT 'The timestamp when the record was created.'
);

-- MIDSTREAM_TABLE: An intermediate table to test multi-hop lineage and conflicting comments.
CREATE OR REPLACE TABLE MIDSTREAM_TABLE AS
SELECT
    ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    STATUS, -- The comment for STATUS is intentionally dropped here to test propagation from a more distant ancestor.
    CREATED_AT
FROM BASE_TABLE;

-- Add a conflicting comment on FIRST_NAME and a new comment on LAST_NAME.
ALTER TABLE MIDSTREAM_TABLE ALTER (
    COLUMN FIRST_NAME COMMENT 'The given name of the user.',
    COLUMN LAST_NAME COMMENT 'The surname of the user.'
);

-- FINAL_TABLE: The target table where we want to propagate comments. It has no comments to start.
CREATE OR REPLACE TABLE FINAL_TABLE AS SELECT * FROM MIDSTREAM_TABLE;

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
-- Expected Outcomes:
-- ID: COMMENT_FOUND from BASE_TABLE (distance 2).
-- FIRST_NAME: MULTIPLE_COMMENTS_AT_SAME_DISTANCE because the comment on MIDSTREAM is closer.
-- LAST_NAME: COMMENT_FOUND from MIDSTREAM_TABLE (distance 1).
-- EMAIL: COMMENT_FOUND from BASE_TABLE (distance 2).
-- STATUS: COMMENT_FOUND from BASE_TABLE (distance 2).
-- CREATED_AT: COMMENT_FOUND from BASE_TABLE (distance 2).

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
  IF (v_comment_found_count != 6) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_multiple_comments_count != 0) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_no_comment_count != 0) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_total_count != 6) THEN
    RAISE assertion_failed;
  END IF;

  RETURN 'Staging table verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_STAGING_TABLE. ' ||
               'COMMENT_FOUND=' || :v_comment_found_count || ', ' ||
               'MULTIPLE_COMMENTS=' || :v_multiple_comments_count || ', ' ||
               'NO_COMMENT=' || :v_no_comment_count || ', ' ||
               'TOTAL=' || :v_total_count;
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
    COUNT(COMMENT),
    COUNT(*) - COUNT(COMMENT)
  INTO
    :v_applied_comment_count,
    :v_uncommented_count
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = :SCHEMA_NAME_PARAM AND TABLE_NAME = 'FINAL_TABLE';

  -- Assert that the correct number of comments were applied.
  IF (v_applied_comment_count != 6) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_uncommented_count != 0) THEN
    RAISE assertion_failed;
  END IF;

  RETURN 'Comment application verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_APPLIED_COMMENTS. ' ||
               'APPLIED=' || :v_applied_comment_count || ', ' ||
               'UNCOMMENTED=' || :v_uncommented_count;
END;
$$;

-- Call the verification procedure
CALL VERIFY_APPLIED_COMMENTS($TEST_SCHEMA_NAME);


-- *********************************************************************************************************************
-- 6. CLEANUP: Drop the test schema and all its objects
-- *********************************************************************************************************************
DROP SCHEMA IF EXISTS TEST_SCHEMA;
