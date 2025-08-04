-- *********************************************************************************************************************
-- TEST SCRIPT V8
-- *********************************************************************************************************************
-- This script provides a simplified, robust, and accurate test for the three core scenarios
-- of the column comment propagation project:
-- 1. COMMENT_FOUND: A column with a single, unambiguous commented parent.
-- 2. MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE: A column with multiple parents at the same distance.
-- 3. NO_COMMENT_FOUND: A column with a single parent that has no comment.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create a dedicated test schema and tables.
-- *********************************************************************************************************************
CREATE OR REPLACE SCHEMA TEST_SCHEMA;
USE SCHEMA TEST_SCHEMA;

-- PARENT_A: Has a column with a comment.
CREATE OR REPLACE TABLE PARENT_A (
    ID INT,
    COL_WITH_COMMENT VARCHAR COMMENT 'This is the comment to be propagated.'
);

-- PARENT_B: Has a column with no comment.
CREATE OR REPLACE TABLE PARENT_B (
    ID INT,
    COL_WITHOUT_COMMENT VARCHAR
);

-- FINAL_TABLE: The target table where comments will be propagated.
CREATE OR REPLACE TABLE FINAL_TABLE (
    -- This column's lineage traces back to PARENT_A.COL_WITH_COMMENT.
    -- Expected status: COMMENT_FOUND
    SINGLE_PARENT_WITH_COMMENT VARCHAR,

    -- This column's lineage traces back to PARENT_B.COL_WITHOUT_COMMENT.
    -- Expected status: NO_COMMENT_FOUND
    SINGLE_PARENT_NO_COMMENT VARCHAR,

    -- This column is derived from two parents at the same distance.
    -- Expected status: MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE
    MULTI_PARENT_COL VARCHAR
);

-- Populate the final table using a CTAS that defines the lineage.
CREATE OR REPLACE TABLE FINAL_TABLE AS
SELECT
    a.COL_WITH_COMMENT AS SINGLE_PARENT_WITH_COMMENT,
    b.COL_WITHOUT_COMMENT AS SINGLE_PARENT_NO_COMMENT,
    CONCAT(a.COL_WITH_COMMENT, b.COL_WITHOUT_COMMENT) AS MULTI_PARENT_COL
FROM PARENT_A a
JOIN PARENT_B b ON a.ID = b.ID;


-- *********************************************************************************************************************
-- 2. EXECUTION: Run the comment propagation procedures
-- *********************************************************************************************************************
SET DEPLOY_DB = CURRENT_DATABASE();
SET DEPLOY_SCHEMA = 'PUBLIC';
SET TEST_DB = CURRENT_DATABASE();
SET TEST_SCHEMA_NAME = 'TEST_SCHEMA';
SET RECORD_PROC_FQN = $DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.RECORD_COMMENT_PROPAGATION_DATA';
SET APPLY_PROC_FQN = $DEPLOY_DB || '.' || $DEPLOY_SCHEMA || '.APPLY_COMMENT_PROPAGATION_DATA';

-- Call the procedure to find and record comments for FINAL_TABLE.
SET record_call_stmt = 'CALL ' || $RECORD_PROC_FQN || ' (''' || $TEST_DB || ''', ''' || $TEST_SCHEMA_NAME || ''', ''FINAL_TABLE'')';
EXECUTE IMMEDIATE $record_call_stmt;

SET CALL_RESULT = (SELECT "RECORD_COMMENT_PROPAGATION_DATA" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
SET RUN_ID = SPLIT_PART($CALL_RESULT, 'RUN_ID: ', 2);

-- *********************************************************************************************************************
-- 3. VERIFICATION (Part 1): Check the staging table for correct status and comments.
-- *********************************************************************************************************************
CREATE OR REPLACE TEMPORARY PROCEDURE VERIFY_STAGING_TABLE(RUN_ID_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_comment_found_count INTEGER;
  v_multiple_cols_count INTEGER;
  v_no_comment_count INTEGER;
  v_total_count INTEGER;
  assertion_failed EXCEPTION (-20001, 'An assertion failed.');
BEGIN
  SELECT
    COUNT_IF(STATUS = 'COMMENT_FOUND'),
    COUNT_IF(STATUS = 'MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE'),
    COUNT_IF(STATUS = 'NO_COMMENT_FOUND'),
    COUNT(*)
  INTO
    :v_comment_found_count,
    :v_multiple_cols_count,
    :v_no_comment_count,
    :v_total_count
  FROM COMMENT_PROPAGATION_STAGING
  WHERE RUN_ID = :RUN_ID_PARAM;

  -- Assert the expected counts for each status.
  IF (v_comment_found_count != 1) THEN RAISE assertion_failed; END IF;
  IF (v_multiple_cols_count != 1) THEN RAISE assertion_failed; END IF;
  IF (v_no_comment_count != 1) THEN RAISE assertion_failed; END IF;
  IF (v_total_count != 3) THEN RAISE assertion_failed; END IF;

  RETURN 'Staging table verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_STAGING_TABLE. ' ||
               'COMMENT_FOUND=' || :v_comment_found_count || ' (expected 1), ' ||
               'MULTIPLE_COLUMNS=' || :v_multiple_cols_count || ' (expected 1), ' ||
               'NO_COMMENT=' || :v_no_comment_count || ' (expected 1), ' ||
               'TOTAL=' || :v_total_count || ' (expected 3)';
END;
$$;

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
  IF (v_applied_comment_count != 1) THEN RAISE assertion_failed; END IF;
  IF (v_uncommented_count != 2) THEN RAISE assertion_failed; END IF;

  RETURN 'Comment application verification successful!';
EXCEPTION
    WHEN assertion_failed THEN
        RETURN 'Assertion failed in VERIFY_APPLIED_COMMENTS. ' ||
               'APPLIED=' || :v_applied_comment_count || ' (expected 1), ' ||
               'UNCOMMENTED=' || :v_uncommented_count || ' (expected 2)';
END;
$$;

CALL VERIFY_APPLIED_COMMENTS($TEST_SCHEMA_NAME);

-- *********************************************************************************************************************
-- 6. CLEANUP: Drop the test schema and all its objects
-- *********************************************************************************************************************
DROP SCHEMA IF EXISTS TEST_SCHEMA;
