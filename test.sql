-- *********************************************************************************************************************
-- TEST SCRIPT
-- *********************************************************************************************************************
-- This script tests the column comment propagation logic.
-- It creates a series of tables with lineage to test various scenarios.
-- *********************************************************************************************************************

-- *********************************************************************************************************************
-- 1. SETUP: Create test tables
-- We will create three tables in a chain: UPSTREAM_TABLE -> MIDSTREAM_TABLE -> DOWNSTREAM_TABLE
-- *********************************************************************************************************************

-- Create the initial upstream table. This is the table we will be processing.
-- Notice that none of its columns have comments.
CREATE OR REPLACE TABLE UPSTREAM_TABLE (
    ID INT,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR,
    CREATED_AT TIMESTAMP_NTZ
);

-- Create a midstream table from the upstream table.
-- We will add a comment to the `FIRST_NAME` column here.
CREATE OR REPLACE TABLE MIDSTREAM_TABLE AS SELECT * FROM UPSTREAM_TABLE;
ALTER TABLE MIDSTREAM_TABLE ALTER COLUMN FIRST_NAME SET COMMENT = 'The first name of the user.';

-- Create another midstream table from the upstream table.
-- We will add a comment to the `FIRST_NAME` column here as well, to test the multiple comments scenario.
CREATE OR REPLACE TABLE MIDSTREAM_TABLE_2 AS SELECT * FROM UPSTREAM_TABLE;
ALTER TABLE MIDSTREAM_TABLE_2 ALTER COLUMN FIRST_NAME SET COMMENT = 'The given name of the user.';

-- Create the final downstream table from the midstream table.
-- We will add a comment to the `EMAIL` column here.
CREATE OR REPLACE TABLE DOWNSTREAM_TABLE AS SELECT * FROM MIDSTREAM_TABLE;
ALTER TABLE DOWNSTREAM_TABLE ALTER COLUMN EMAIL SET COMMENT = 'The primary email address of the user.';

-- *********************************************************************************************************************
-- 2. EXECUTION: Run the comment propagation procedure
-- We will call the main stored procedure to process the UPSTREAM_TABLE.
-- *********************************************************************************************************************

-- Before running, let's get the current database and schema.
SET DB_NAME = CURRENT_DATABASE();
SET SCHEMA_NAME = CURRENT_SCHEMA();

-- Call the procedure to find and record comments.
CALL RECORD_COMMENT_PROPAGATION_DATA($DB_NAME, $SCHEMA_NAME, 'UPSTREAM_TABLE');

-- *********************************************************************************************************************
-- 3. VERIFICATION: Check the results in the staging table
-- We will query the staging table to verify that the comments were propagated as expected.
-- We'll filter by the most recent RUN_ID.
-- *********************************************************************************************************************

-- Expected outcomes:
-- 1. FIRST_NAME: Should have a status of 'MULTIPLE_COMMENTS_AT_SAME_DISTANCE' because two comments exist at a lineage distance of 1.
-- 2. EMAIL: Should have a status of 'COMMENT_FOUND' and the comment from the DOWNSTREAM_TABLE.
-- 3. ID, LAST_NAME, CREATED_AT: Should have a status of 'NO_COMMENT_FOUND'.

SELECT
    SOURCE_COLUMN_NAME,
    STATUS,
    TARGET_COMMENT,
    LINEAGE_DISTANCE,
    HAS_MULTIPLE_COMMENTS_AT_SAME_DISTANCE
FROM
    COMMENT_PROPAGATION_STAGING
WHERE
    RUN_ID = (SELECT MAX(RUN_ID) FROM COMMENT_PROPAGATION_STAGING WHERE SOURCE_TABLE_NAME = 'UPSTREAM_TABLE')
ORDER BY
    SOURCE_COLUMN_NAME;

-- *********************************************************************************************************************
-- 4. CLEANUP: Drop the test tables
-- *********************************************************************************************************************

DROP TABLE IF EXISTS UPSTREAM_TABLE;
DROP TABLE IF EXISTS MIDSTREAM_TABLE;
DROP TABLE IF EXISTS MIDSTREAM_TABLE_2;
DROP TABLE IF EXISTS DOWNSTREAM_TABLE;
