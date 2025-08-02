# Plan for Snowflake Stored Procedure: Comment Propagation via Lineage (As-Built)

This document outlines the design of a Snowflake solution that automates the propagation of column comments from ancestor tables (i.e., upstream sources) to descendant tables (i.e., downstream targets). It leverages a synchronous, set-based stored procedure architecture, Snowflake's `GET_LINEAGE` function, and the real-time `INFORMATION_SCHEMA` for maximum performance and up-to-date results.

## Objective

To identify columns in a given input table that lack comments and then, using data lineage, find a matching ancestor column with a comment. The procedure records the outcome for each column—including the found comment or a 'not found' status—into a versioned staging table for later review and application.

## High-Level Architecture

The solution is delivered as a single deployment script (`deploy.sql`) that creates three core components:

1. **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that identifies all un-commented columns in a table and finds potential comments for them in upstream tables using a single, efficient, set-based query.
3. **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that applies the suggestions from the staging table, allowing for a human-in-the-loop review process.

This architecture maximizes performance by using a single declarative SQL statement and ensures a safe, controlled application of comments.

## Detailed Plan

### Step 1: Consolidated Deployment (`deploy.sql`)

A single, idempotent script creates all necessary objects:

1. **`SAFE_QUOTE` UDF**: A helper function for quoting identifiers.
2. **`COMMENT_PROPAGATION_STAGING` Table**: A versioned table to store results from each run. It is fully commented, includes change tracking, and preserves grants on deployment.
3. **`RECORD_COMMENT_PROPAGATION_DATA` Stored Procedure**: The main orchestrator procedure.
4. **`APPLY_COMMENT_PROPAGATION_DATA` Stored Procedure**: The procedure to apply the found comments.

### Step 2: Comment Search and Record Logic (`RECORD_COMMENT_PROPAGATION_DATA`)

The main procedure contains the core logic for finding and recording comments.

* **Parameters**: `P_DATABASE_NAME`, `P_SCHEMA_NAME`, `P_TABLE_NAME`.
* **Logic**:
    1. **Validate Table**: Checks if the provided table exists in its own `INFORMATION_SCHEMA`.
    2. **Generate Run ID**: Creates a unique `RUN_ID` using `UUID_STRING()`.
    3. **Targeted Comment Gathering**: The procedure first identifies all distinct upstream source tables from the lineage results. It then iterates through each distinct upstream database and executes a single, targeted query to fetch comments *only* for the specific columns identified in the lineage (not just any commented columns from those tables). This column-specific filtering ensures maximum efficiency and precision in comment retrieval.
    4. **Find and Insert in One Operation**: It then executes a single, multi-CTE `INSERT` statement that:
        a.  Identifies all columns with `NULL` or empty comments from the target table's `INFORMATION_SCHEMA`.
        b.  For each un-commented column, calls `GET_LINEAGE` to find all upstream source columns.
        c.  Joins the lineage with the collected `INFORMATION_SCHEMA` comments.
        d.  Uses window functions (`ROW_NUMBER`, `COUNT`) to rank the results by lineage distance and identify the single closest comment, while also flagging cases where multiple comments exist at the same distance.
        e.  Determines the final `STATUS` (`COMMENT_FOUND`, `MULTIPLE_COMMENTS_AT_SAME_DISTANCE`, or `NO_COMMENT_FOUND`).
        f.  Inserts the complete, processed results into the `COMMENT_PROPAGATION_STAGING` table.
* **Return Value**: Returns a success message with the `RUN_ID` and the number of columns processed.

### Step 3: Applying Comments (`APPLY_COMMENT_PROPAGATION_DATA`)

This procedure applies the comments found in the staging table.

* **Parameters**: `P_RUN_ID`.
* **Logic**:
    1. It iterates through the `COMMENT_PROPAGATION_STAGING` table for the given `RUN_ID`, filtered to records with `STATUS = 'COMMENT_FOUND'`.
    2. To minimize DDL operations, it uses `LISTAGG` to dynamically construct a single `ALTER TABLE ... MODIFY COLUMN` statement for all column comment changes on the target table (since each RUN_ID corresponds to only one table).
    3. Each `ALTER TABLE` statement is executed within its own `BEGIN...EXCEPTION` block to ensure that a failure on one table does not prevent others from being processed.
* **Return Value**: Returns a summary message of applied and skipped comments.

### Step 4: Structured Logging and Tracing

All procedures are instrumented with:
- Structured logging using `SYSTEM$LOG_<level>()` for clear monitoring and debugging
- Custom tracing using `SYSTEM$ADD_EVENT` and `SYSTEM$SET_SPAN_ATTRIBUTES` for detailed execution metrics
- Comprehensive error handling with detailed context in exception messages

## Considerations and Best Practices Implemented

* **Correct GET_LINEAGE Usage**: Extracts SOURCE_OBJECT_* columns (not TARGET_OBJECT_*) for upstream lineage, as GET_LINEAGE returns relationship edges
* **Column-Specific Filtering**: Queries INFORMATION_SCHEMA for specific (schema, table, column) tuples rather than all commented columns from upstream tables
* **Proper DDL Syntax**: Uses `ALTER TABLE ... MODIFY COLUMN` syntax for applying multiple column comments efficiently
* **Variable Scoping**: Uses regular variables instead of cursor record variables in complex query contexts to avoid identifier resolution issues
* **Temporary Table Management**: Uses temporary tables where appropriate and permanent tables only when necessary for EXECUTE IMMEDIATE scoping
* **Real-Time Metadata**: Uses `INFORMATION_SCHEMA` for all metadata lookups to ensure data is current and queries are fast
* **Row Versioning Strategy**: Uses a `RUN_ID` for auditable history and single-table processing
* **Deployment Best Practices**: Uses `CREATE OR REPLACE`, `COPY GRANTS`, and `EXECUTE AS OWNER` for smooth, secure, and idempotent deployments
* **Self-Documentation**: All objects have `COMMENT` properties
* **Resilient DDL**: The application logic is wrapped in exception handlers to gracefully handle failures

## Key Implementation Insights

* **Lineage Distance**: Represents actual hops in the lineage path (1 = immediate upstream, 2 = two hops, etc.)
* **Record Variable Casing**: Field names are automatically up-cased in FOR...IN...SELECT loops (`record.FIELD_NAME` not `record.field_name`)
* **Variable Declaration Rules**: Use `DECLARE` at block start, direct assignment for declared variables, `LET` only for undeclared local variables
* **IDENTIFIER Limitations**: Cannot use string concatenation inside IDENTIFIER() function - must pre-build concatenated strings
* **Multi-hop Lineage**: Successfully traces through complex pipelines using SOURCE columns from GET_LINEAGE relationships
