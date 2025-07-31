# Plan for Snowflake Stored Procedure: Comment Propagation via Lineage (As-Built)

This document outlines the design of a Snowflake solution that automates the propagation of column comments from downstream tables to upstream tables. It leverages an asynchronous stored procedure architecture, Snowflake's `GET_LINEAGE` function, and a hybrid approach of using both `INFORMATION_SCHEMA` and `SNOWFLAKE.ACCOUNT_USAGE` for a balance of performance and functionality.

## Objective

To identify columns in a given input table that lack comments and then, using data lineage, find a matching downstream column with a comment. The procedure records the outcome for each column—including the found comment or a 'not found' status—into a versioned staging table for later review and application.

## High-Level Architecture

The solution is delivered as a single deployment script (`deploy.sql`) that creates three core components:

1. **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that identifies all un-commented columns in a table and finds potential comments for them in downstream tables, running the search for each column in parallel.
3. **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that applies the suggestions from the staging table, allowing for a human-in-the-loop review process.

This architecture maximizes parallelism for data gathering and ensures a safe, controlled application of comments.

## Detailed Plan

### Step 1: Consolidated Deployment (`deploy.sql`)

A single, idempotent script creates all necessary objects:

1. **`SAFE_QUOTE` UDF**: A helper function for quoting identifiers.
2. **`COMMENT_PROPAGATION_STAGING` Table**: A versioned table to store results from each run. It is fully commented, includes change tracking, and preserves grants on deployment.
3. **`FIND_AND_RECORD_COMMENT_FOR_COLUMN` Stored Procedure**: The helper procedure that performs the lineage search for a single column.
4. **`RECORD_COMMENT_PROPAGATION_DATA` Stored Procedure**: The main orchestrator procedure.
5. **`APPLY_COMMENT_PROPAGATION_DATA` Stored Procedure**: The procedure to apply the found comments.

### Step 2: Comment Search Logic (`FIND_AND_RECORD_COMMENT_FOR_COLUMN`)

The helper procedure contains the core logic for a single column.

* **Parameters**: It accepts a `RUN_ID` and the fully qualified name of the source column.
* **Logic**:
    1. It uses a single, declarative `SELECT INTO` statement that calls `GET_LINEAGE`.
    2. The query joins the lineage results with `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` to find comments.
    3. It uses a `COUNT(*) OVER (PARTITION BY d.DISTANCE)` window function to efficiently find the closest comment and determine if multiple comments exist at that same distance.
    4. The result, including the status (`COMMENT_FOUND`, `MULTIPLE_COMMENTS_AT_SAME_DISTANCE`, or `NO_COMMENT_FOUND`), is inserted into the `COMMENT_PROPAGATION_STAGING` table.
* **Error Handling**: The procedure is wrapped in an `EXCEPTION` block to gracefully handle and log errors.

### Step 3: Orchestration (`RECORD_COMMENT_PROPAGATION_DATA`)

The main procedure manages the overall process.

* **Parameters**: `P_DATABASE_NAME`, `P_SCHEMA_NAME`, `P_TABLE_NAME`.
* **Logic**:
    1. **Validate Table**: Checks if the provided table exists in `INFORMATION_SCHEMA`.
    2. **Generate Run ID**: Creates a unique `RUN_ID` using `UUID_STRING()`.
    3. **Identify Un-commented Columns**: Queries `INFORMATION_SCHEMA` to get a real-time list of columns with `NULL` or empty comments.
    4. **Dispatch Asynchronous Calls**: Loops through the list and executes `ASYNC CALL FIND_AND_RECORD_COMMENT_FOR_COLUMN(...)` for each one.
    5. **Wait for Completion**: Uses `AWAIT ALL` to pause execution until all dispatched jobs are complete.
* **Return Value**: Returns a success message with the `RUN_ID`.

### Step 4: Applying Comments (`APPLY_COMMENT_PROPAGATION_DATA`)

This procedure applies the comments found in the staging table.

* **Parameters**: `P_RUN_ID`.
* **Logic**:
    1. It iterates through the `COMMENT_PROPAGATION_STAGING` table for the given `RUN_ID`, filtered to records with `STATUS = 'COMMENT_FOUND'`.
    2. For each table with comments to apply, it dynamically constructs a single `ALTER TABLE ... ALTER COLUMN ... SET COMMENT ...` statement for all columns in that table. This minimizes the number of DDL operations.
    3. Each `ALTER TABLE` statement is executed within its own `BEGIN...EXCEPTION` block to ensure that a failure on one table does not prevent others from being processed.
* **Return Value**: Returns a summary message of applied and skipped comments.

### Step 5: Structured Logging

All procedures are instrumented with structured logging using `SYSTEM$LOG_<level>()` for clear monitoring and debugging.

## Considerations and Best Practices Implemented

* **Hybrid Metadata Approach**: Uses `INFORMATION_SCHEMA` for real-time lookups and `SNOWFLAKE.ACCOUNT_USAGE` for the `GET_LINEAGE` function.
* **Asynchronous Performance**: Leverages `ASYNC CALL` for parallel execution.
* **Declarative Logic**: Prefers declarative SQL (`SELECT INTO`) over imperative loops for better performance and readability.
* **Row Versioning Strategy**: Uses a `RUN_ID` for auditable history.
* **Deployment Best Practices**: Uses `CREATE OR REPLACE`, `COPY GRANTS`, and `EXECUTE AS OWNER` for smooth, secure, and idempotent deployments.
* **Self-Documentation**: All objects have `COMMENT` properties.
