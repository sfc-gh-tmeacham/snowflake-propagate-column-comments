# Snowflake Propagate Column Comments

This project provides a Snowflake stored procedure to automate the propagation of column comments from ancestor tables (i.e., upstream sources) to descendant tables (i.e., downstream targets) using data lineage.

## Objective

The primary goal is to identify columns in a given table that lack comments and automatically find a corresponding comment from an ancestor table in its lineage. This is useful for maintaining data documentation and ensuring that comments are consistently applied as data flows through your data warehouse.

The solution is delivered as a single deployment script that creates two stored procedures:

1. **`RECORD_COMMENT_PROPAGATION_DATA`**: The main procedure that you call to find and record comment suggestions in a staging table.
2. **`APPLY_COMMENT_PROPAGATION_DATA`**: A second procedure that you call to apply the suggestions from the staging table.

This two-step process allows you to review the suggested comments before applying them.

## How It Works

The solution uses a single deployment script (`deploy.sql`) to create all the necessary database objects. The core logic is contained within the `RECORD_COMMENT_PROPAGATION_DATA` procedure, which operates as follows:

1. **Finds Uncommented Columns**: It first identifies all columns in the target table that have `NULL` or empty comments.
2. **Discovers Lineage**: Using Snowflake's `GET_LINEAGE` function, it constructs the complete upstream lineage for all uncommented columns. This is done efficiently using a single, dynamic `UNION ALL` query.
3. **Gathers All Potential Comments**: It then queries the `INFORMATION_SCHEMA` of all unique upstream databases to collect all available comments for the identified ancestor columns.
4. **Determines Final Status**: In a single, multi-CTE query, it determines the final status for each column based on a clear order of precedence:
    * **Structural Ambiguity**: It first checks if a column has a "structural fork" in its lineage (i.e., more than one parent at the same, closest distance). If so, it is marked `MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE`.
    * **Comment Found**: If the lineage is structurally sound (a single parent at the closest distance), it checks if that parent has a comment. If it does, the status is `COMMENT_FOUND`.
    * **No Comment Found**: If the single parent has no comment, or if there is no lineage, the status is `NO_COMMENT_FOUND`.
5. **Logs Results**: The final, processed results are inserted into the `COMMENT_PROPAGATION_STAGING` table with a unique `RUN_ID`.

## Permissions

This procedure relies on `SNOWFLAKE.CORE.GET_LINEAGE` and the `INFORMATION_SCHEMA` for each upstream database. To ensure proper execution, the procedure should be created and run by a role with `USAGE` on all upstream databases.

For more information, see the [Snowflake documentation on `GET_LINEAGE`](https://docs.snowflake.com/en/sql-reference/functions/get_lineage).

## Setup

To set up all the necessary objects, run the full `deploy.sql` script in your Snowflake environment.

## Usage

The comment propagation process is a two-step process:

### 1. Record Comment Suggestions

To run the comment propagation process, call the `RECORD_COMMENT_PROPAGATION_DATA` stored procedure with the database, schema, and table name of the table you want to process.

```sql
CALL RECORD_COMMENT_PROPAGATION_DATA('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE');
```

The procedure will return a `RUN_ID`, which you will need for the next steps.

### 2. Review the Results

After the procedure completes, you can query the `COMMENT_PROPAGATION_STAGING` table to see the results. Use the `RUN_ID` returned by the stored procedure to filter the results for a specific run.

The `STATUS` column will indicate the outcome for each column:

* `COMMENT_FOUND`: A single, unambiguous comment was found at the closest lineage distance.
* `MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE`: The column has a structural fork in its lineage (multiple parents at the same closest distance), making the source ambiguous.
* `NO_COMMENT_FOUND`: No comment was found for the column, either because its single parent had no comment or because it had no lineage.

```sql
-- Replace 'your_run_id' with the actual RUN_ID
SELECT * FROM COMMENT_PROPAGATION_STAGING WHERE RUN_ID = 'your_run_id';
```

### 3. Apply the Comments

After you have reviewed the suggestions, call the `APPLY_COMMENT_PROPAGATION_DATA` stored procedure with the `RUN_ID`. This will apply the comments for all columns where the `STATUS` is `COMMENT_FOUND`.

```sql
-- Replace 'your_run_id' with the actual RUN_ID
CALL APPLY_COMMENT_PROPAGATION_DATA('your_run_id');
```

## Testing

The project includes a robust and simplified test suite (`testing/test.sql`) that validates the three core logical outcomes:

* **`COMMENT_FOUND`**: Tests that a comment is correctly propagated from a single, unambiguous parent.
* **`MULTIPLE_COLUMNS_FOUND_AT_SAME_DISTANCE`**: Tests that a column derived from two parents is correctly flagged as ambiguous.
* **`NO_COMMENT_FOUND`**: Tests that a column whose parent has no comment is correctly flagged.

To run the tests, execute `testing/test.sql` after deploying the solution.
