# Plan for Snowflake Stored Procedure: Comment Propagation via Lineage (As-Built)

This document outlines the design of a Snowflake solution that automates the propagation of column comments from downstream tables to upstream tables. It leverages an asynchronous stored procedure architecture, Snowflake's `GET_LINEAGE` function, and a hybrid approach of using both `INFORMATION_SCHEMA` and `SNOWFLAKE.ACCOUNT_USAGE` for a balance of performance and functionality.

## Objective

To identify columns in a given input table that lack comments and then, using data lineage, find a matching downstream column with a comment. The procedure records the outcome for each column—including the found comment or a 'not found' status—into a versioned staging table for later review and application.

## High-Level Architecture

The solution is split into two stored procedures for performance and clarity:

1. **Helper Procedure (`FIND_AND_RECORD_COMMENT_FOR_COLUMN`):** A synchronous procedure responsible for processing a *single* un-commented column. It searches the lineage, finds a comment (or confirms none exists), and writes a single result to the staging table.
2. **Main Procedure (`RECORD_COMMENT_PROPAGATION_DATA`):** An orchestrator procedure that identifies all un-commented columns in a table and then dispatches an asynchronous call to the helper procedure for each column.

This architecture maximizes parallelism, as Snowflake can process multiple columns concurrently.

## Detailed Plan

### Step 1: Staging Table Setup (`setup.sql`)

A versioned staging table is created to store the results of each run. It is fully commented, includes change tracking for downstream consumers, and preserves grants on deployment.

```sql
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
    TARGET_COMMENT VARCHAR COMMENT 'The comment found on the target column, or a status if none was found.',
    LINEAGE_DISTANCE INTEGER COMMENT 'The number of steps in the lineage between the source and target objects.',
    HAS_MULTIPLE_COMMENTS_AT_SAME_DISTANCE BOOLEAN COMMENT 'Flag to indicate if multiple comments were found at the same lineage distance.',
    STATUS VARCHAR COMMENT 'The status of the comment propagation for this column. One of COMMENT_FOUND, NO_COMMENT_FOUND, or MULTIPLE_COMMENTS_AT_SAME_DISTANCE.',
    RECORD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'The timestamp when this record was created.'
)
CHANGE_TRACKING = TRUE
COPY GRANTS
COMMENT = 'A staging table that records potential column comments propagated from downstream objects via data lineage.';
```

### Step 2: Helper Procedure Implementation

The `FIND_AND_RECORD_COMMENT_FOR_COLUMN` procedure contains the core logic for a single column.

* **Parameters:** It accepts a `RUN_ID` and all necessary identifiers for the source column.
* **Logic:**
    1. Constructs the fully qualified name for the source column (e.g., `MY_DB.MY_SCHEMA.MY_TABLE.MY_COL`).
    2. Calls `GET_LINEAGE` for that specific column to get its downstream dependencies. This function requires `ACCOUNT_USAGE` and is subject to its latency.
    3. It loops through the downstream lineage objects, ordered by distance, joining with `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` to get comments.
    4. The first non-empty comment found is recorded. If other comments exist at the same lineage distance, a flag is set. The search loop is then broken.
    5. It uses a single `INSERT` statement to log the outcome to `COMMENT_PROPAGATION_STAGING`, tagged with the `RUN_ID`. It populates the `STATUS` column and leaves fields `NULL` where no comment was found.
* **Error Handling:** The entire procedure is wrapped in an `EXCEPTION` block to gracefully handle errors and log them.

### Step 3: Main Orchestrator Procedure Implementation

The `RECORD_COMMENT_PROPAGATION_DATA` procedure manages the overall process.

* **Parameters:** `P_DATABASE_NAME`, `P_SCHEMA_NAME`, `P_TABLE_NAME`.
* **Logic:**
    1. **Validate Table:** Checks if the provided table exists in the `INFORMATION_SCHEMA` for real-time validation.
    2. **Generate Run ID:** Creates a unique `RUN_ID` using `UUID_STRING()` for the entire execution.
    3. **Identify Un-commented Columns:** Queries the `INFORMATION_SCHEMA` to get a real-time list of active columns in the target table with `NULL` or empty comments.
    4. **Dispatch Asynchronous Calls:** Loops through the list of un-commented columns and executes `ASYNC CALL FIND_AND_RECORD_COMMENT_FOR_COLUMN(...)` for each one, passing the `RUN_ID` and column details.
    5. **Wait for Completion:** Uses `AWAIT ALL` to pause execution until all dispatched asynchronous jobs are complete.
* **Return Value:** Returns a success message including the `RUN_ID` for easy tracking.
* **Error Handling:** Also wrapped in an `EXCEPTION` block to report and log any failures during orchestration.

### Step 4: Structured Logging

Both procedures are instrumented with structured logging using `SYSTEM$LOG_<level>()`.

* **INFO:** Logs the start and end of key processes.
* **WARN:** Logs when no downstream comment can be found for a column or when multiple comments are found at the same distance.
* **FATAL/ERROR:** Logs the `SQLERRM` message when an exception is caught.

This requires a Snowflake event table to be set up and configured for the account to capture the logs.

## Considerations and Best Practices Implemented

* **Hybrid Metadata Approach:** The solution uses `INFORMATION_SCHEMA` for initial table/column lookups to ensure real-time accuracy. However, the core lineage query must still use `SNOWFLAKE.ACCOUNT_USAGE` because the `GET_LINEAGE` function depends on it. This means that while the initial checks are immediate, the lineage data itself is subject to the inherent latency of the `ACCOUNT_USAGE` views (up to 90 minutes).
* **Asynchronous Performance:** The async architecture dramatically improves performance by allowing Snowflake to process multiple columns in parallel. This feature requires Snowflake Enterprise Edition or higher.
* **Correctness:** The logic correctly uses column-level lineage and filters out deleted objects from the `ACCOUNT_USAGE` views.
* **Row Versioning Strategy:** The use of `RUN_ID` and `INSERT`-only logic provides a full, auditable history of comment suggestions over time.
* **Deployment Best Practices:** The use of `CREATE OR REPLACE`, `COPY GRANTS`, and `EXECUTE AS OWNER` on all objects ensures that permissions are maintained and deployments are smooth, secure, and idempotent.
* **Self-Documentation:** All objects, including tables, columns, and procedures, have `COMMENT` properties to explain their purpose.

## Future Step: Applying the Comments

A separate stored procedure can be created to consume the data from `COMMENT_PROPAGATION_STAGING`. This procedure would read the latest `RUN_ID` (or a specific one), select rows where a comment was found, and execute `ALTER TABLE ... ALTER COLUMN ... SET COMMENT`. This separation allows for review and manual approval before comments are applied to production tables.
