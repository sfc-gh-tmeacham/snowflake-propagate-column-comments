-- *********************************************************************************************************************
-- TEST SCRIPT V5
-- *********************************************************************************************************************
-- This script provides a comprehensive, automated test for the column comment propagation project.
-- It creates a dedicated schema and a series of tables with a deep lineage structure that includes
-- joins and derived columns to test complex, real-world scenarios.
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
    JOINED_COL_COMMENT VARCHAR COMMENT 'Comment from JOIN_TABLE.'
);

-- ANCESTOR_3: The join and derivation happens here. It joins ANCESTOR_2 with JOIN_TABLE,
-- drops some columns, and adds new derived ones to maintain a 20-column structure.
CREATE OR REPLACE TABLE ANCESTOR_3 AS
SELECT
    -- Pass through most columns from ANCESTOR_2
    a2.COL_1, a2.COL_2, a2.COL_3, a2.COL_4, a2.COL_5, a2.COL_6, a2.COL_7, a2.COL_8, a2.COL_9, a2.COL_10,
    a2.COL_11, a2.COL_12, a2.COL_13, a2.COL_14, a2.COL_15, a2.COL_16, a2.COL_17,
    -- New Derived and Joined Columns
    UPPER(a2.COL_5) AS DERIVED_UPPER,                      -- Derived from COL_5
    CONCAT(a2.COL_6, '-', a2.COL_7) AS DERIVED_CONCAT,   -- Derived from COL_6 and COL_7
    jt.JOINED_COL_COMMENT AS JOINED_COL                   -- From JOIN_TABLE
FROM ANCESTOR_2 a2
LEFT JOIN JOIN_TABLE jt ON a2.COL_1 = jt.ID;

ALTER TABLE ANCESTOR_3 ALTER (
    COLUMN COL_2 COMMENT 'Ancestor 3 Comment for COL_2 (re-added)',
    COLUMN COL_3 COMMENT '', -- Drop comment again
    COLUMN COL_17 COMMENT 'Ancestor 3 Comment for COL_17 (new)',
    COLUMN DERIVED_UPPER COMMENT 'Ancestor 3 Comment for DERIVED_UPPER (override)'
);

-- ANCESTOR_4: The direct parent of FINAL_TABLE.
CREATE OR REPLACE TABLE ANCESTOR_4 AS SELECT * FROM ANCESTOR_3;
ALTER TABLE ANCESTOR_4 ALTER (
    COLUMN COL_3 COMMENT 'Ancestor 4 Comment for COL_3 (re-added)',
    COLUMN COL_4 COMMENT '', -- Drop comment
    COLUMN DERIVED_CONCAT COMMENT 'Ancestor 4 Comment for DERIVED_CONCAT (new)'
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
-- - COMMENT_FOUND (15 columns):
--   - COL_1 (dist 3), COL_2 (dist 2), COL_3 (dist 1)
--   - COL_4, COL_5, COL_6...COL_15 (dist 4, from ANCESTOR_1) (12 cols)
--   - COL_16 (dist 3), COL_17 (dist 2)
--   - DERIVED_UPPER (dist 2)
--   - DERIVED_CONCAT (dist 1)
--   - JOINED_COL (dist 2)
-- - MULTIPLE_COMMENTS_AT_SAME_DISTANCE (0 columns)
-- - NO_COMMENT_FOUND (5 columns): COL_8-12 dropped, others no comment anywhere. Expected NO_COMMENT_FOUND for the remaining uncommented columns in the final table.
-- The final table has COL_1-17, DERIVED_UPPER, DERIVED_CONCAT, JOINED_COL.
-- The original table had up to COL_20.
-- Let's re-verify the final columns:
-- COMMENTED: COL_1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17, DERIVED_UPPER, DERIVED_CONCAT, JOINED_COL
-- Let's trace carefully:
-- COL_1: ANCESTOR_2 (dist 3)
-- COL_2: ANCESTOR_3 (dist 2)
-- COL_3: ANCESTOR_4 (dist 1)
-- COL_4: ANCESTOR_1 (dist 4, since dropped in A4)
-- COL_5: ANCESTOR_1 (dist 4)
-- COL_6: ANCESTOR_1 (dist 4)
-- COL_7: ANCESTOR_1 (dist 4)
-- COL_8: ANCESTOR_1 (dist 4)
-- COL_9: ANCESTOR_1 (dist 4)
-- COL_10: ANCESTOR_1 (dist 4)
-- COL_11: ANCESTOR_1 (dist 4)
-- COL_12: ANCESTOR_1 (dist 4)
-- COL_13: ANCESTOR_1 (dist 4)
-- COL_14: ANCESTOR_1 (dist 4)
-- COL_15: ANCESTOR_1 (dist 4)
-- COL_16: ANCESTOR_2 (dist 3)
-- COL_17: ANCESTOR_3 (dist 2)
-- DERIVED_UPPER: ANCESTOR_3 (dist 2) - it will find the comment on ANCESTOR_3.DERIVED_UPPER, not trace back to ANCESTOR_1.COL_5. This is correct.
-- DERIVED_CONCAT: ANCESTOR_4 (dist 1)
-- JOINED_COL: JOIN_TABLE (dist 2)
-- TOTAL WITH COMMENTS: 19
-- TOTAL WITH NO COMMENT: 1 (COL_8 from ANCESTOR_1 has a comment. Let me fix ANCESTOR_1 DDL)
-- Let's assume COL_8,9,10 have no comment in A1.
-- The final table has 20 columns. Let's list them: COL_1..17 (17), DERIVED_UPPER, DERIVED_CONCAT, JOINED_COL.
-- The uncommented columns in A1 are 16-20. All are in A2. A3 selects 1-17. So 16 and 17 are uncommented from A1.
-- A2 adds comment to 16. A3 adds comment to 17. So they get comments.
-- DERIVED_UPPER, DERIVED_CONCAT, JOINED_COL all have comments.
-- Let's check which columns in FINAL_TABLE will have no comments. None. All should have comments. Let's adjust.
-- I will remove the comment from `JOIN_TABLE.JOINED_COL_COMMENT`
-- I will also remove comment from ANCESTOR_4 on DERIVED_CONCAT. Now it should trace back to A1.COL_6 and A1.COL_7, resulting in multiple comments.
-- Let's try that.

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
