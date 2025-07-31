# Snowflake Propagate Column Comments

This project provides a Snowflake stored procedure to automate the propagation of column comments from downstream tables to upstream tables using data lineage.

## Objective

The primary goal is to identify columns in a given table that lack comments and automatically find a corresponding comment from a downstream column. This is useful for maintaining data documentation and ensuring that comments are consistently applied across your data warehouse.

The solution is delivered as a single deployment script that creates two stored procedures:

1. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that you call to find and record comment suggestions in a staging table.
2. **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that you call to apply the suggestions from the staging table.

This two-step process allows you to review the suggested comments before applying them.

## How It Works

The solution uses a single deployment script (`deploy.sql`) to create all the necessary database objects:

1. **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2. **`COMMENT_PROPAGATION_STAGING` Table**: A table that logs the results of the comment propagation process, including suggested comments or a "not found" status, with a unique `RUN_ID`.
3. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that identifies all columns in a table that are missing comments and finds potential comments for them in downstream tables.
4. **`APPLY_COMMENT_PROPAGATION_DATA`**: A procedure that applies the comments found by the `RECORD_COMMENT_PROPAGATION_DATA` procedure.

## Permissions

This procedure relies on `SNOWFLAKE.ACCOUNT_USAGE` views, which have inherent latency and specific access requirements. To ensure proper execution, the procedure should be created and run by a role with the necessary privileges, such as `ACCOUNTADMIN` or a custom role with imported privileges on the `SNOWFLAKE` database.

For more information on the required permissions, see the [Snowflake documentation on `GET_LINEAGE`](https://docs.snowflake.com/en/sql-reference/functions/get_lineage).

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
* `NO_COMMENT_FOUND`: No downstream comment was found.

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
