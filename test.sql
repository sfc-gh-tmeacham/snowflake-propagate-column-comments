-- *********************************************************************************************************************
-- TEST SCRIPT V2
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
ALTER TABLE MIDSTREAM_TABLE ALTER
    COLUMN FIRST_NAME SET COMMENT = 'The given name of the user.',
    COLUMN LAST_NAME SET COMMENT = 'The surname of the user.';

-- FINAL_TABLE: The target table where we want to propagate comments. It has no comments to start.
CREATE OR REPLACE TABLE FINAL_TABLE AS SELECT * FROM MIDSTREAM_TABLE;

-- *********************************************************************************************************************
-- 2. EXECUTION: Run the comment propagation procedures
-- *********************************************************************************************************************
-- Use the fully qualified procedure name, including the database where it was deployed.
-- This makes the test script more portable.
SET DEPLOY_DB = CURRENT_DATABASE();
SET DEPLOY_SCHEMA = 'PUBLIC'; -- Assuming procedures are in PUBLIC schema, change if needed.
SET TEST_DB = CURRENT_DATABASE();
SET TEST_SCHEMA = 'TEST_SCHEMA';

-- Step 2.1: Call the procedure to find and record comments for FINAL_TABLE.
CALL IDENTIFIER($DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.RECORD_COMMENT_PROPAGATION_DATA')($TEST_DB, $TEST_SCHEMA, 'FINAL_TABLE');
SET RUN_ID = (SELECT LAST_QUERY_ID()); -- Capture the RUN_ID from the return value.

-- *********************************************************************************************************************
-- 3. VERIFICATION (Part 1): Check the staging table for correct status and comments.
-- *********************************************************************************************************************
-- We will now verify that the staging table contains the expected results for our test cases.
-- Expected Outcomes:
-- ID: COMMENT_FOUND from BASE_TABLE (distance 2) because MIDSTREAM has no comment.
-- FIRST_NAME: MULTIPLE_COMMENTS_AT_SAME_DISTANCE because both BASE and MIDSTREAM have a comment at distance 1.
-- LAST_NAME: COMMENT_FOUND from MIDSTREAM_TABLE (distance 1).
-- EMAIL: COMMENT_FOUND from BASE_TABLE (distance 2).
-- STATUS: COMMENT_FOUND from BASE_TABLE (distance 2).
-- CREATED_AT: COMMENT_FOUND from BASE_TABLE (distance 2).

DECLARE
  v_comment_found_count INTEGER;
  v_multiple_comments_count INTEGER;
  v_no_comment_count INTEGER;
  v_total_count INTEGER;
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
  WHERE RUN_ID = :RUN_ID;

  -- Assert the expected counts for each status.
  SYSTEM$ASSERT(v_comment_found_count = 4, 'Expected 4 columns with COMMENT_FOUND status. Found: ' || v_comment_found_count);
  SYSTEM$ASSERT(v_multiple_comments_count = 1, 'Expected 1 column with MULTIPLE_COMMENTS_AT_SAME_DISTANCE status. Found: ' || v_multiple_comments_count);
  SYSTEM$ASSERT(v_no_comment_count = 0, 'Expected 0 columns with NO_COMMENT_FOUND status. Found: ' || v_no_comment_count);
  SYSTEM$ASSERT(v_total_count = 5, 'Expected a total of 5 columns to be processed. Found: ' || v_total_count);

  RETURN 'Staging table verification successful!';
END;


-- *********************************************************************************************************************
-- 4. EXECUTION (Part 2): Apply the comments
-- *********************************************************************************************************************
CALL IDENTIFIER($DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.APPLY_COMMENT_PROPAGATION_DATA')(:RUN_ID);


-- *********************************************************************************************************************
-- 5. VERIFICATION (Part 2): Check that comments were physically applied to the final table.
-- *********************************************************************************************************************
DECLARE
  v_applied_comment_count INTEGER;
  v_uncommented_count INTEGER;
  v_first_name_comment VARCHAR;
BEGIN
  SELECT
    COUNT(COMMENT),
    COUNT(*) - COUNT(COMMENT)
  INTO
    :v_applied_comment_count,
    :v_uncommented_count
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = :TEST_SCHEMA AND TABLE_NAME = 'FINAL_TABLE';

  -- The comment for FIRST_NAME should NOT have been applied due to the 'MULTIPLE_COMMENTS' status.
  SELECT COMMENT
  INTO :v_first_name_comment
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = :TEST_SCHEMA AND TABLE_NAME = 'FINAL_TABLE' AND COLUMN_NAME = 'FIRST_NAME';

  -- Assert that the correct number of comments were applied.
  SYSTEM$ASSERT(v_applied_comment_count = 4, 'Expected 4 comments to be applied to FINAL_TABLE. Found: ' || v_applied_comment_count);
  SYSTEM$ASSERT(v_uncommented_count = 1, 'Expected 1 column to remain uncommented in FINAL_TABLE. Found: ' || v_uncommented_count);
  SYSTEM$ASSERT(v_first_name_comment IS NULL, 'FIRST_NAME column should not have a comment, but found: ' || v_first_name_comment);

  RETURN 'Comment application verification successful!';
END;


-- *********************************************************************************************************************
-- 6. CLEANUP: Drop the test schema and all its objects
-- *********************************************************************************************************************
DROP SCHEMA IF EXISTS TEST_SCHEMA;
