# Plan for Snowflake Stored Procedure: Comment Propagation via Lineage (As-Built)

This document outlines the design of a Snowflake solution that automates the propagation of column comments from ancestor tables (i.e., upstream sources) to descendant tables (i.e., downstream targets). It leverages a synchronous, set-based stored procedure architecture, Snowflake's `GET_LINEAGE` function, and a hybrid approach of using both `INFORMATION_SCHEMA` and `SNOWFLAKE.ACCOUNT_USAGE` for a balance of performance and functionality.

## Objective

To identify columns in a given input table that lack comments and then, using data lineage, find a matching ancestor column with a comment. The procedure records the outcome for each column—including the found comment or a 'not found' status—into a versioned staging table for later review and application.

## High-Level Architecture

The solution is delivered as a single deployment script (`deploy.sql`) that creates three core components:

1.  **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2.  **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that identifies all un-commented columns in a table and finds potential comments for them in upstream tables using a single, efficient, set-based query.
3.  **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that applies the suggestions from the staging table, allowing for a human-in-the-loop review process.

This architecture maximizes performance by using a single declarative SQL statement and ensures a safe, controlled application of comments.

## Detailed Plan

### Step 1: Consolidated Deployment (`deploy.sql`)

A single, idempotent script creates all necessary objects:

1.  **`SAFE_QUOTE` UDF**: A helper function for quoting identifiers.
2.  **`COMMENT_PROPAGATION_STAGING` Table**: A versioned table to store results from each run. It is fully commented, includes change tracking, and preserves grants on deployment.
3.  **`RECORD_COMMENT_PROPAGATION_DATA` Stored Procedure**: The main orchestrator procedure.
4.  **`APPLY_COMMENT_PROPAGATION_DATA` Stored Procedure**: The procedure to apply the found comments.

### Step 2: Comment Search and Record Logic (`RECORD_COMMENT_PROPAGATION_DATA`)

The main procedure contains the core logic for finding and recording comments.

*   **Parameters**: `P_DATABASE_NAME`, `P_SCHEMA_NAME`, `P_TABLE_NAME`.
*   **Logic**:
    1.  **Validate Table**: Checks if the provided table exists in `INFORMATION_SCHEMA`.
    2.  **Generate Run ID**: Creates a unique `RUN_ID` using `UUID_STRING()`.
    3.  **Find and Insert in One Operation**: Executes a single, multi-CTE `INSERT` statement that:
        a.  Identifies all columns with `NULL` or empty comments from `INFORMATION_SCHEMA`.
        b.  For each un-commented column, calls `GET_LINEAGE` to find all upstream source columns.
        c.  Joins the lineage with `SNOWFLAKE.ACCOUNT_USAGE.COLUMNS` to retrieve comments.
        d.  Uses window functions (`ROW_NUMBER`, `COUNT`) to rank the results by lineage distance and identify the single closest comment, while also flagging cases where multiple comments exist at the same distance.
        e.  Determines the final `STATUS` (`COMMENT_FOUND`, `MULTIPLE_COMMENTS_AT_SAME_DISTANCE`, or `NO_COMMENT_FOUND`).
        f.  Inserts the complete, processed results into the `COMMENT_PROPAGATION_STAGING` table.
*   **Return Value**: Returns a success message with the `RUN_ID` and the number of columns processed.

### Step 3: Applying Comments (`APPLY_COMMENT_PROPAGATION_DATA`)

This procedure applies the comments found in the staging table.

*   **Parameters**: `P_RUN_ID`.
*   **Logic**:
    1.  It iterates through the `COMMENT_PROPAGATION_STAGING` table for the given `RUN_ID`, filtered to records with `STATUS = 'COMMENT_FOUND'`.
    2.  To minimize DDL operations, it groups all comments by table and uses `LISTAGG` to dynamically construct a single `ALTER TABLE` statement for all column comment changes on that table.
    3.  Each `ALTER TABLE` statement is executed within its own `BEGIN...EXCEPTION` block to ensure that a failure on one table does not prevent others from being processed.
*   **Return Value**: Returns a summary message of applied and skipped comments.

### Step 4: Structured Logging

All procedures are instrumented with structured logging using `SYSTEM$LOG_<level>()` for clear monitoring and debugging.

## Considerations and Best Practices Implemented

*   **Set-Based Performance**: Prefers a single, declarative `INSERT ... SELECT` statement over a procedural, row-by-row approach for optimal performance.
*   **Hybrid Metadata Approach**: Uses `INFORMATION_SCHEMA` for real-time lookups and `SNOWFLAKE.ACCOUNT_USAGE` for comment and lineage data.
*   **Row Versioning Strategy**: Uses a `RUN_ID` for auditable history.
*   **Deployment Best Practices**: Uses `CREATE OR REPLACE`, `COPY GRANTS`, and `EXECUTE AS OWNER` for smooth, secure, and idempotent deployments.
*   **Self-Documentation**: All objects have `COMMENT` properties.
*   **Resilient DDL**: The application logic is wrapped in exception handlers to gracefully skip tables that fail to update.
