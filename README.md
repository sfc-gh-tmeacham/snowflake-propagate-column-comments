# Snowflake Propagate Column Comments

This project provides a Snowflake stored procedure to automate the propagation of column comments from ancestor tables (i.e., upstream sources) to descendant tables (i.e., downstream targets) using data lineage.

## Objective

The primary goal is to identify columns in a given table that lack comments and automatically find a corresponding comment from an ancestor table in its lineage. This is useful for maintaining data documentation and ensuring that comments are consistently applied as data flows through your data warehouse.

The solution is delivered as a single deployment script that creates two stored procedures:

1. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that you call to find and record comment suggestions in a staging table.
2. **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that you call to apply the suggestions from the staging table.

This two-step process allows you to review the suggested comments before applying them.

## How It Works

The solution uses a single deployment script (`deploy.sql`) to create all the necessary database objects:

1. **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2. **`COMMENT_PROPAGATION_STAGING` Table**: A table that logs the results of the comment propagation process, including suggested comments or a "not found" status, with a unique `RUN_ID`.
3. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that identifies all columns in a table that are missing comments and finds potential comments for them in ancestor tables using Snowflake's `GET_LINEAGE` function to trace complete multi-hop lineage paths.
4. **`APPLY_COMMENT_PROPAGATION_DATA`**: A procedure that applies the comments found by the `RECORD_COMMENT_PROPAGATION_DATA` procedure using optimized `ALTER TABLE ... MODIFY COLUMN` statements.

## Permissions

This procedure relies on `SNOWFLAKE.CORE.GET_LINEAGE` and the `INFORMATION_SCHEMA` for each upstream database. Unlike `SNOWFLAKE.ACCOUNT_USAGE` views, the `INFORMATION_SCHEMA` provides real-time data, ensuring the results are always current.

To ensure proper execution, the procedure should be created and run by a role with the following privileges:

* **`USAGE` on all upstream databases**: The role must have the `USAGE` privilege on every database that contains a potential source table in the lineage. This is required to query the `INFORMATION_SCHEMA.COLUMNS` view in those databases.
* **Privileges on the target table**: The role needs standard read privileges on the target table to identify uncommented columns and write privileges to apply new comments.

A role with broad read privileges (like `ACCOUNTADMIN` or a dedicated data governance role) is recommended.

For more information on the required permissions for lineage, see the [Snowflake documentation on `GET_LINEAGE`](https://docs.snowflake.com/en/sql-reference/functions/get_lineage).

## Setup

To set up all the necessary objects, run the full `deploy.sql` script in your Snowflake environment. This will create the helper function, the staging table, and the stored procedures.

## Usage

The comment propagation process is a two-step process:

### 1. Record Comment Suggestions

To run the comment propagation process, call the `RECORD_COMMENT_PROPAGATION_DATA` stored procedure with the database, schema, and table name of the table you want to process.

```sql
CALL RECORD_COMMENT_PROPAGATION_DATA('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE');
```

Replace `MY_DATABASE`, `MY_SCHEMA`, and `MY_TABLE` with the actual names of your database, schema, and table. The procedure will return a `RUN_ID`, which you will need for the next steps.

### 2. Review the Results

After the procedure completes, you can query the `COMMENT_PROPAGATION_STAGING` table to see the results. Use the `RUN_ID` returned by the stored procedure to filter the results for a specific run.

The `STATUS` column will indicate the outcome for each column:

* `COMMENT_FOUND`: A single comment was found at the closest lineage distance.
* `MULTIPLE_COMMENTS_AT_SAME_DISTANCE`: Multiple comments were found at the same, closest lineage distance.
* `NO_COMMENT_FOUND`: No comment was found in any ancestor table.

> **Note**: The `MULTIPLE_COMMENTS_AT_SAME_DISTANCE` status is only triggered for ties at the *closest* distance. If comments exist at multiple distances (e.g., a comment is found 1 hop away and another is 2 hops away), the procedure will always choose the closest one, and the status will be `COMMENT_FOUND`.

```sql
-- Replace 'your_run_id' with the actual RUN_ID
SELECT * FROM COMMENT_PROPAGATION_STAGING WHERE RUN_ID = 'your_run_id';
```

### 3. Apply the Comments

After you have reviewed the suggestions and are ready to apply them, call the `APPLY_COMMENT_PROPAGATION_DATA` stored procedure with the `RUN_ID` from the first step. This will apply the comments for all columns where the `STATUS` is `COMMENT_FOUND`.

```sql
-- Replace 'your_run_id' with the actual RUN_ID
CALL APPLY_COMMENT_PROPAGATION_DATA('your_run_id');
```

This procedure will only apply comments that have been staged and will skip any that have multiple suggestions or where no comment was found, ensuring a safe and controlled update process.

## Key Features

* **Multi-hop lineage support**: Traces comments through complex data pipelines (e.g., `BASE_TABLE` → `MIDSTREAM_TABLE` → `FINAL_TABLE`)
* **Distance-based ranking**: Selects comments from the closest upstream source when multiple options exist
* **Batch DDL operations**: Applies multiple column comments in a single `ALTER TABLE` statement for efficiency
* **Comprehensive logging**: Includes detailed tracing and error handling for production use
* **Idempotent execution**: Safe to re-run procedures multiple times
* **Real-time lineage**: Uses live `GET_LINEAGE` data, not cached `ACCOUNT_USAGE` views

## Limitations

* **Maximum lineage distance**: The solution is limited by Snowflake's `GET_LINEAGE` function, which has a maximum distance of 5 levels. This means the procedure can trace comments through at most 5 hops in the data lineage (e.g., `TABLE_A` → `TABLE_B` → `TABLE_C` → `TABLE_D` → `TABLE_E` → `TABLE_F`). Comments from ancestor tables beyond this distance will not be discovered.

  **Workaround**: For lineage chains longer than 5 hops, apply the comment propagation procedures first on upstream tables within the 5-hop range. Once those intermediate tables have comments, they become discoverable sources for tables further downstream, effectively extending the reach of comment propagation through the entire lineage chain.

## Testing

The project includes a comprehensive test suite (`test.sql`) that:

* Creates realistic multi-hop lineage scenarios
* Tests comment propagation across multiple tables
* Validates both successful and edge-case scenarios
* Provides automated verification of results

To run the tests, execute `test.sql` after deploying the solution.
