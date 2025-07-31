# Snowflake Propagate Column Comments

This project provides a Snowflake stored procedure to automate the propagation of column comments from downstream tables to upstream tables using data lineage.

## Objective

The primary goal is to identify columns in a given table that lack comments and automatically find a corresponding comment from a downstream column. This is useful for maintaining data documentation and ensuring that comments are consistently applied across your data warehouse.

The procedure records its findings in a staging table, allowing you to review the suggested comments before applying them.

## How It Works

The solution uses two stored procedures and a helper function:

1. **`SAFE_QUOTE` UDF**: A helper function that ensures database identifiers are correctly double-quoted, making the procedures robust against non-standard names.
2. `RECORD_COMMENT_PROPAGATION_DATA`: The main procedure that you call. It identifies all columns in a table that are missing comments.
3. `FIND_AND_RECORD_COMMENT_FOR_COLUMN`: A helper procedure that is called for each column without a comment. It searches the data lineage for a downstream column with a comment.

The results, including the suggested comment or a "not found" status, are logged in the `COMMENT_PROPAGATION_STAGING` table with a unique `RUN_ID`.

## Setup

1. **Create the Staging Table:** Run the `setup.sql` script to create the `COMMENT_PROPAGATION_STAGING` table. This table will store the results of the comment propagation process.
2. **Deploy the Helper Function:** Execute the `safe_quote.sql` script to create the `SAFE_QUOTE` UDF.
3. **Deploy the Stored Procedures:** Execute the `record_comment_propagation_data.sql` script to create the `RECORD_COMMENT_PROPAGATION_DATA` and `FIND_AND_RECORD_COMMENT_FOR_COLUMN` stored procedures.

## Usage

To run the comment propagation process, call the `RECORD_COMMENT_PROPAGATION_DATA` stored procedure with the database, schema, and table name of the table you want to process.

```sql
CALL RECORD_COMMENT_PROPAGATION_DATA('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE');
```

Replace `MY_DATABASE`, `MY_SCHEMA`, and `MY_TABLE` with the actual names of your database, schema, and table.

### Reviewing the Results

After the procedure completes, you can query the `COMMENT_PROPAGATION_STAGING` table to see the results. Use the `RUN_ID` returned by the stored procedure to filter the results for a specific run.

The `STATUS` column will indicate the outcome for each column:

* `COMMENT_FOUND`: A single comment was found at the closest lineage distance.
* `MULTIPLE_COMMENTS_AT_SAME_DISTANCE`: Multiple comments were found at the same, closest lineage distance.
* `NO_COMMENT_FOUND`: No downstream comment was found.

```sql
-- Replace 'your_run_id' with the actual RUN_ID
SELECT * FROM COMMENT_PROPAGATION_STAGING WHERE RUN_ID = 'your_run_id';
```

### Applying the Comments (Future Step)

This project does not automatically apply the comments. You can create a separate stored procedure to read the suggestions from the `COMMENT_PROPAGATION_STAGING` table and apply them using `ALTER TABLE ... ALTER COLUMN ... SET COMMENT`. This allows for a review and approval process before making changes to your table metadata.
