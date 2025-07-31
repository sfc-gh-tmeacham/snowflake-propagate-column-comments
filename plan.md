# Plan for Snowflake Stored Procedure: Comment Propagation via Lineage (As-Built)

This document outlines the design of a Snowflake solution that automates the propagation of column comments from downstream tables to upstream tables. It leverages an asynchronous stored procedure architecture, Snowflake's `GET_LINEAGE` function, and `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` for efficiency.

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
    2. Calls `GET_LINEAGE` for that specific column to get its downstream dependencies.
    3. It loops through the downstream lineage objects, ordered by distance.
    4. For each object, it dynamically constructs and executes a query against `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` to fetch the comment, ensuring to exclude deleted columns.
    5. The first non-empty comment found is recorded, and the search loop is broken.
    6. It uses an `INSERT` statement to log the outcome to `COMMENT_PROPAGATION_STAGING`, tagged with the `RUN_ID`. If no comment was found, it records this status explicitly. The record includes both the individual identifiers and the fully qualified names for the source and target columns.
* **Error Handling:** The entire procedure is wrapped in a `TRY...CATCH` block to gracefully handle errors and log them.

### Step 3: Main Orchestrator Procedure Implementation

The `RECORD_COMMENT_PROPAGATION_DATA` procedure manages the overall process.

* **Parameters:** `P_DATABASE_NAME`, `P_SCHEMA_NAME`, `P_TABLE_NAME`.
* **Logic:**
    1. **Generate Run ID:** Creates a unique `RUN_ID` using `UUID_STRING()` for the entire execution.
    2. **Identify Un-commented Columns:** Queries `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` to get a list of active (non-deleted) columns in the target table with `NULL` or empty comments.
    3. **Dispatch Asynchronous Calls:** Loops through the list of un-commented columns and executes `ASYNC CALL FIND_AND_RECORD_COMMENT_FOR_COLUMN(...)` for each one, passing the `RUN_ID` and column details.
    4. **Wait for Completion:** Uses `AWAIT ALL` to pause execution until all dispatched asynchronous jobs are complete.
* **Return Value:** Returns a success message including the `RUN_ID` for easy tracking.
* **Error Handling:** Also wrapped in a `TRY...CATCH` block to report and log any failures during orchestration.

### Step 4: Structured Logging

Both procedures are instrumented with structured logging using `SYSTEM$LOG_<level>()`.

* **INFO:** Logs the start and end of key processes.
* **WARN:** Logs when no downstream comment can be found for a column.
* **DEBUG:** Logs the dynamic SQL statements before execution.
* **ERROR/FATAL:** Logs the `SQLERRM` message when an exception is caught.

This requires a Snowflake event table to be set up and configured for the account to capture the logs.

## Considerations and Best Practices Implemented

* **Asynchronous Performance:** The async architecture dramatically improves performance by allowing Snowflake to process multiple columns in parallel.
* **Correctness:** The logic correctly uses column-level lineage and filters out deleted objects from the `ACCOUNT_USAGE` views.
* **Row Versioning Strategy:** The use of `RUN_ID` and `INSERT`-only logic provides a full, auditable history of comment suggestions over time.
* **Deployment Best Practices:** The use of `CREATE OR REPLACE`, `COPY GRANTS`, and `EXECUTE AS OWNER` on all objects ensures that permissions are maintained and deployments are smooth, secure, and idempotent.
* **`ACCOUNT_USAGE` Latency:** The solution relies on `ACCOUNT_USAGE` views, which have a data latency of up to 90 minutes. This means recent changes might not be immediately reflected.
* **Self-Documentation:** All objects, including tables, columns, and procedures, have `COMMENT` properties to explain their purpose.

## Future Step: Applying the Comments

A separate stored procedure can be created to consume the data from `COMMENT_PROPAGATION_STAGING`. This procedure would read the latest `RUN_ID` (or a specific one), select rows where a comment was found, and execute `ALTER TABLE ... ALTER COLUMN ... SET COMMENT`. This separation allows for review and manual approval before comments are applied to production tables.
