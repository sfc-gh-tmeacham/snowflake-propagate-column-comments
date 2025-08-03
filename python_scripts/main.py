import argparse
from snowflake.snowpark.session import Session
from record_comment_propagation_data import record_comment_propagation_data
from apply_comment_propagation_data import apply_comment_propagation_data

# It's recommended to use a more secure way to manage credentials,
# such as environment variables, a secrets manager, or the Snowflake config file.
# For simplicity, we'll define them here.
SNOWFLAKE_CONNECTION_PARAMS = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>"
}

def main():
    parser = argparse.ArgumentParser(description="Snowpark Column Comment Propagation")
    
    # Arguments for recording data
    parser.add_argument('--record', action='store_true', help='Run the recording process.')
    parser.add_argument('--database', type=str, help='Target database name for recording.')
    parser.add_argument('--schema', type=str, help='Target schema name for recording.')
    parser.add_argument('--table', type=str, help='Target table name for recording.')

    # Argument for applying comments
    parser.add_argument('--apply', action='store_true', help='Run the application process.')
    parser.add_argument('--run_id', type=str, help='The RUN_ID for applying comments.')

    args = parser.parse_args()

    try:
        session = Session.builder.configs(SNOWFLAKE_CONNECTION_PARAMS).create()
        print("Successfully created Snowpark session.")
        
        if args.record:
            if not all([args.database, args.schema, args.table]):
                print("For recording, --database, --schema, and --table are required.")
                return
            
            print(f"Recording comment propagation data for {args.database}.{args.schema}.{args.table}...")
            result = record_comment_propagation_data(session, args.database, args.schema, args.table)
            print(result)

        elif args.apply:
            if not args.run_id:
                print("For applying, --run_id is required.")
                return
                
            print(f"Applying comments for RUN_ID: {args.run_id}...")
            result = apply_comment_propagation_data(session, args.run_id)
            print(result)
        
        else:
            print("Please specify an action: --record or --apply.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'session' in locals() and session:
            session.close()
            print("Snowpark session closed.")

if __name__ == "__main__":
    main()
