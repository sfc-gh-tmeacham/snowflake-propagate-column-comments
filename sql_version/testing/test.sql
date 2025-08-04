-- *********************************************************************************************************************
-- TEST SCRIPT V6
-- *********************************************************************************************************************
-- This script provides a comprehensive, automated test for the column comment propagation project.
-- It creates a dedicated schema and a series of tables with a deep lineage structure that includes
-- joins and various derived columns (single-parent, multi-parent, and aggregate) to test complex scenarios.
-- The script uses assertions to automatically verify the results.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create a dedicated test schema and a deep lineage of tables
-- *********************************************************************************************************************
CREATE OR REPLACE SCHEMA TEST_SCHEMA;
USE SCHEMA TEST_SCHEMA;

-- ANCESTOR_1: The most distant ancestor, providing the baseline comments.
CREATE OR REPLACE TABLE ANCESTOR_1 (
    COL_1 INT COMMENT 'Ancestor 1 Comment for COL_1',
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
    COLUMN COL_2 COMMENT '', -- Drop comment
    COLUMN COL_16 COMMENT 'Ancestor 2 Comment for COL_16 (new)'
);

-- JOIN_TABLE: A table to be joined in the lineage chain.
CREATE OR REPLACE TABLE JOIN_TABLE (
    ID INT COMMENT 'Join key.',
    JOINED_COL_COMMENT VARCHAR, -- No comment, to test NO_COMMENT_FOUND
    NUMERIC_VAL_NO_COMMENT INT -- No comment, for derived sum test
);

-- ANCESTOR_3: The join and derivation happens here. It joins ANCESTOR_2 with JOIN_TABLE,
-- drops COL_17, and adds new derived ones to maintain a 20-column structure.
CREATE OR REPLACE TABLE ANCESTOR_3 AS
SELECT
    -- Pass through most columns from ANCESTOR_2, dropping COL_17
    a2.COL_1, a2.COL_2, a2.COL_3, a2.COL_4, a2.COL_5, a2.COL_6, a2.COL_7, a2.COL_8, a2.COL_9, a2.COL_10,
    a2.COL_11, a2.COL_12, a2.COL_13, a2.COL_14, a2.COL_15, a2.COL_16,
    -- New Derived and Joined Columns
    UPPER(a2.COL_5) AS DERIVED_UPPER,
    CONCAT(a2.COL_6, '-', a2.COL_7) AS DERIVED_CONCAT,
    jt.JOINED_COL_COMMENT AS JOINED_COL,
    (a2.COL_1 + jt.NUMERIC_VAL_NO_COMMENT) AS SUM_DERIVED_COL -- Derived from commented and uncommented cols
FROM ANCESTOR_2 a2
LEFT JOIN JOIN_TABLE jt ON a2.COL_1 = jt.ID;

ALTER TABLE ANCESTOR_3 ALTER (
    COLUMN COL_2 COMMENT 'Ancestor 3 Comment for COL_2 (re-added)',
    COLUMN COL_3 COMMENT '', -- Drop comment again
    COLUMN DERIVED_UPPER COMMENT 'Ancestor 3 Comment for DERIVED_UPPER (override)'
);

-- ANCESTOR_4: The direct parent of FINAL_TABLE.
CREATE OR REPLACE TABLE ANCESTOR_4 AS SELECT * FROM ANCESTOR_3;
ALTER TABLE ANCESTOR_4 ALTER (
    COLUMN COL_3 COMMENT 'Ancestor 4 Comment for COL_3 (re-added)',
    COLUMN COL_4 COMMENT '', -- Drop comment
    COLUMN DERIVED_CONCAT COMMENT '' -- Drop comment to test multi-parent lineage
);

-- FINAL_TABLE: The target table where we want to propagate comments.
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
-- Expected Outcomes for FINAL_TABLE columns (20 total):
-- - COMMENT_FOUND (18 columns):
--   - Most columns find a comment from the closest ancestor with a valid comment.
--   - DERIVED_UPPER: Correctly finds the explicit comment on the derived column in ANCESTOR_3.
--   - SUM_DERIVED_COL: Correctly finds the comment from the single commented parent column (COL_1).
--
-- - MULTIPLE_COMMENTS_AT_SAME_DISTANCE (1 column):
--   - DERIVED_CONCAT: The comment is dropped in ANCESTOR_4, forcing lineage to trace back
--     to its two parent columns (COL_6 and COL_7), resulting in this status.
--
-- - NO_COMMENT_FOUND (1 column):
--   - JOINED_COL: This column comes from JOIN_TABLE, where its source has no comment.

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
  IF (v_multiple_comments_count != 1) THEN
    RAISE assertion_failed;
  END IF;
  IF (v_no_comment_count != 1) THEN
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
               'MULTIPLE_COMMENTS=' || :v_multiple_comments_count || ' (expected 1), ' ||
               'NO_COMMENT=' || :v_no_comment_count || ' (expected 1), ' ||
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

  -- Assert that the correct number of comments were applied. We expect 18 comments to be applied.
  -- The MULTIPLE_COMMENTS and NO_COMMENT cases should not result in an applied comment.
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
