-- *********************************************************************************************************************
-- TEST SCRIPT
-- *********************************************************************************************************************
-- This script tests the column comment propagation logic.
-- It creates a series of tables with lineage to test various scenarios.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create test tables
-- We will create three tables in a chain: BASE_TABLE -> MIDSTREAM_TABLE -> FINAL_TABLE
-- *********************************************************************************************************************

-- Create the initial base table. This is the table that has the comments we want to propagate.
CREATE OR REPLACE TABLE BASE_TABLE (
    ID INT COMMENT 'The unique identifier.',
    FIRST_NAME VARCHAR COMMENT 'The first name of the user.',
    LAST_NAME VARCHAR,
    EMAIL VARCHAR COMMENT 'The primary email address of the user.',
    CREATED_AT TIMESTAMP_NTZ
);

-- Create a midstream table from the base table.
-- We will add a conflicting comment to the `FIRST_NAME` column here to test the multiple comments scenario.
CREATE OR REPLACE TABLE MIDSTREAM_TABLE AS SELECT * FROM BASE_TABLE;
ALTER TABLE MIDSTREAM_TABLE ALTER COLUMN FIRST_NAME SET COMMENT = 'The given name of the user.';

-- Create the final table from the midstream table.
-- This is the table we will be processing. Notice that none of its columns have comments.
CREATE OR REPLACE TABLE FINAL_TABLE AS SELECT * FROM MIDSTREAM_TABLE;

-- *********************************************************************************************************************
-- 2. EXECUTION: Run the comment propagation procedure
-- We will call the main stored procedure to process the FINAL_TABLE.
-- *********************************************************************************************************************

-- Before running, let's get the current database and schema.
SET DB_NAME = CURRENT_DATABASE();
SET SCHEMA_NAME = CURRENT_SCHEMA();

-- Call the procedure to find and record comments.
CALL RECORD_COMMENT_PROPAGATION_DATA($DB_NAME, $SCHEMA_NAME, 'FINAL_TABLE');

-- *********************************************************************************************************************
-- 3. VERIFICATION: Check the results in the staging table
-- We will query the staging table to verify that the comments were propagated as expected.
-- We'll filter by the most recent RUN_ID.
-- *********************************************************************************************************************

-- Expected outcomes:
-- 1. FIRST_NAME: Should have a status of 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE' because two comments exist at a lineage distance of 1 (MIDSTREAM_TABLE) and 2 (BASE_TABLE), but the closest one is ambiguous.
-- 2. ID, EMAIL: Should have a status of 'COMMENT_FOUND' and the comment from the BASE_TABLE.
-- 3. LAST_NAME, CREATED_AT: Should have a status of 'NO_COMMENT_FOUND'.

SELECT
    SOURCE_COLUMN_NAME,
    STATUS,
    TARGET_COMMENT,
    LINEAGE_DISTANCE
FROM
    COMMENT_PROPAGATION_STAGING
WHERE
    RUN_ID = (SELECT MAX(RUN_ID) FROM COMMENT_PROPAGATION_STAGING WHERE SOURCE_TABLE_NAME = 'FINAL_TABLE')
ORDER BY
    SOURCE_COLUMN_NAME;

-- *********************************************************************************************************************
-- 4. CLEANUP: Drop the test tables
-- *********************************************************************************************************************

DROP TABLE IF EXISTS BASE_TABLE;
DROP TABLE IF EXISTS MIDSTREAM_TABLE;
DROP TABLE IF EXISTS FINAL_TABLE;
