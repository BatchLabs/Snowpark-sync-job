import os
import sys
import json
import time
import logging
import argparse
from datetime import date, datetime
import pandas as pd
import requests
from snowflake.snowpark import Session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle date objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            # Convert to RFC 3339 format
            if isinstance(obj, date) and not isinstance(obj, datetime):
                obj = datetime.combine(obj, datetime.min.time())
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        return super(DateTimeEncoder, self).default(obj)

def sync_stream_to_batch(conn, project_key, source_stream, id_column, date_columns='', url_columns='', api_credentials_table='BATCH_API_CREDENTIALS'):
    """
    Sync a Snowflake stream to Batch.com
    
    Args:
        conn: Snowflake connection
        project_key (str): Batch project key
        source_stream (str): Name of the source stream
        id_column (str): Column to use as the customer ID
        date_columns (str): Comma-separated list of date columns
        url_columns (str): Comma-separated list of URL columns
        api_credentials_table (str): Name of the table containing API credentials
    
    Returns:
        str: Result message
    """
    try:
        # Convert comma-separated lists to sets for easy lookup
        date_columns_set = {col.strip().upper() for col in date_columns.split(',')} if date_columns else set()
        url_columns_set = {col.strip().upper() for col in url_columns.split(',')} if url_columns else set()
        
        cursor = conn.cursor()
        
        # Retrieve API credentials
        logger.info(f"Retrieving API credentials for project key: {project_key}")
        cursor.execute(f"SELECT * FROM {api_credentials_table} WHERE project_key = '{project_key}'")
        creds = cursor.fetchone()
        if not creds:
            error_msg = f"No API credentials found for project key: {project_key}"
            logger.error(error_msg)
            return error_msg
        
        # Get the column names from cursor description
        col_names = [desc[0] for desc in cursor.description]
        
        # Find the index of the REST_API_KEY column
        rest_api_key_idx = next((i for i, col in enumerate(col_names) if col.upper() == 'REST_API_KEY'), None)
        if rest_api_key_idx is None:
            error_msg = "REST_API_KEY column not found in API_CREDENTIALS table"
            logger.error(error_msg)
            return error_msg
            
        rest_api_key = creds[rest_api_key_idx]
        api_url = "https://api.batch.com/2.4/profiles/update"
        
        # Begin a transaction for stream consumption
        cursor.execute("BEGIN TRANSACTION")
        
        try:
            # Get column information by directly querying the stream
            logger.info(f"Extracting columns directly from stream: {source_stream}")
            
            # Try to get a sample row to extract column names
            cursor.execute(f"SELECT * FROM {source_stream} LIMIT 1")
            sample_row = cursor.fetchone()
            
            if not sample_row:
                # No data in stream, commit to mark the position and exit
                cursor.execute("COMMIT")
                message = f"No data found in stream {source_stream}."
                logger.info(message)
                return message
            
            # Extract column names from cursor description
            column_names = [desc[0] for desc in cursor.description if not desc[0].startswith('METADATA$')]
            
            if not column_names:
                error_msg = f"No non-metadata columns found in stream {source_stream}"
                logger.error(error_msg)
                cursor.execute("ROLLBACK")
                return error_msg
            
            logger.info(f"Found {len(column_names)} columns in stream: {column_names}")
            
            # Verify the ID column exists (case-insensitive search)
            id_column_found = False
            for col in column_names:
                if col.upper() == id_column.upper():
                    id_column = col  # Use the exact case from the stream
                    id_column_found = True
                    break
            
            if not id_column_found:
                error_msg = f"Error: ID column '{id_column}' not found in stream columns: {column_names}"
                logger.error(error_msg)
                cursor.execute("ROLLBACK")
                return error_msg
            
            # Build the dynamic SQL query with all columns plus stream metadata
            # Quote the column names to handle case sensitivity and special characters
            columns_str = ', '.join([f'"{col}"' for col in column_names])
            query = f"""
                SELECT {columns_str}, 
                       METADATA$ACTION, 
                       METADATA$ISUPDATE,
                       METADATA$ROW_ID
                FROM {source_stream}
            """
            
            # Fetch data from the stream
            logger.info(f"Fetching changes from stream {source_stream}")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Get all column names including metadata
            all_columns = column_names + ["METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID"]
            
            # Convert to pandas DataFrame for easier processing
            changes_df = pd.DataFrame(rows, columns=all_columns)
            
            if changes_df.empty:
                # Commit the transaction to mark the current stream position
                cursor.execute("COMMIT")
                message = f"No rows found in stream {source_stream} despite having schema."
                logger.info(message)
                return message
                
        except Exception as e:
            error_msg = f"Error accessing stream {source_stream}: {str(e)}"
            logger.error(error_msg)
            cursor.execute("ROLLBACK")
            return error_msg
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + rest_api_key,
            "X-Batch-Project": project_key
        }
        
        success_count = 0
        fail_count = 0
        error_logs = []
        user_data_batch = []
        
        # Process each row in the dataframe
        logger.info(f"Processing {len(changes_df)} change records from {source_stream}")
        for index, row in changes_df.iterrows():
            try:
                # Get the action type (INSERT, UPDATE, or DELETE)
                action = row["METADATA$ACTION"]
                
                # Skip deleted records as Batch doesn't support deletion
                if action == "DELETE":
                    logger.debug(f"Skipping DELETE action for row {index}")
                    continue
                
                custom_id = str(row[id_column])
                
                # Process attributes with proper data typing
                attributes = {}
                
                for col_name, value in row.items():
                    # Skip metadata columns and ID column
                    if col_name in ["METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID"] or col_name == id_column:
                        continue
                    
                    if pd.isna(value):
                        continue  # Skip None/NaN values
                    
                    # Convert column name to lowercase for consistency in Batch
                    attr_name = col_name.lower()
                    
                    # Process based on field type, with appropriate attribute name wrapping
                    # Use case-insensitive matching for date and URL columns
                    if col_name.upper() in date_columns_set:
                        # Use date() wrapper for date field names
                        attributes[f"date({attr_name})"] = value
                    elif col_name.upper() in url_columns_set:
                        # Use url() wrapper for URL field names
                        attributes[f"url({attr_name})"] = value
                    else:
                        # Keep all other values with their native types
                        attributes[attr_name] = value
                
                user_data_batch.append({
                    "identifiers": {
                        "custom_id": custom_id,
                    },
                    "attributes": attributes
                })
                
                if len(user_data_batch) == 1000 or index == len(changes_df) - 1:
                    try:
                        # Use the custom encoder to handle date objects
                        json_data = json.dumps(user_data_batch, cls=DateTimeEncoder)
                        logger.debug(f"Sending batch of {len(user_data_batch)} records to Batch API")
                        response = requests.post(api_url, headers=headers, data=json_data)
                        if response.status_code == 202:
                            success_count += len(user_data_batch)
                            logger.debug(f"Successfully sent {len(user_data_batch)} records")
                        else:
                            fail_count += len(user_data_batch)
                            error_msg = f"Failed for batch starting with custom_id {user_data_batch[0]['identifiers']['custom_id']}: {response.text[:500]}"
                            logger.error(error_msg)
                            error_logs.append(error_msg)
                    except Exception as e:
                        fail_count += len(user_data_batch)
                        error_msg = f"Exception for batch starting with custom_id {user_data_batch[0]['identifiers']['custom_id']}: {str(e)}"
                        logger.error(error_msg)
                        error_logs.append(error_msg)
                    
                    user_data_batch = []
                    time.sleep(1)  # Rate limiting
            except Exception as e:
                fail_count += 1
                error_msg = f"Error processing row {index}: {str(e)}"
                logger.error(error_msg)
                error_logs.append(error_msg)
        
        # If everything was successful, commit the transaction to mark the stream as consumed
        if fail_count == 0:
            logger.info("All records processed successfully, committing transaction to consume stream data")
            cursor.execute("COMMIT")
        else:
            # If there were any failures, roll back so we can retry
            logger.warning(f"{fail_count} records failed processing, rolling back transaction")
            cursor.execute("ROLLBACK")
        
        # Save results to a log table if desired
        result_message = f"Stream sync complete for {source_stream}: {success_count} records succeeded, {fail_count} failed."
        logger.info(result_message)
        
        if error_logs:
            error_detail = "\n".join(error_logs)
            logger.warning(f"Errors encountered during sync:\n{error_detail}")
            result_message += "\n" + error_detail
            
        return result_message
    except Exception as e:
        error_msg = f"Error in sync_stream_to_batch: {str(e)}"
        logger.error(error_msg)
        try:
            cursor.execute("ROLLBACK")
            logger.info("Transaction rolled back due to error")
        except:
            pass
        return error_msg

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Sync Snowflake stream to Batch.com')
    parser.add_argument('--project-key', required=True, help='Batch project key')
    parser.add_argument('--source-stream', required=True, help='Source stream name (format: DATABASE.SCHEMA.STREAM)')
    parser.add_argument('--id-column', required=True, help='Column to use as the custom ID')
    parser.add_argument('--date-columns', default='', help='Comma-separated list of date columns')
    parser.add_argument('--url-columns', default='', help='Comma-separated list of URL columns')
    parser.add_argument('--api-credentials-table', default='BATCH_API_CREDENTIALS', help='Table containing API credentials')
    parser.add_argument('--connection-parameters', default='', help='JSON string of connection parameters')
    return parser.parse_args()

def get_login_token():
    """
    Read the login token supplied automatically by Snowflake. These tokens
    are short lived and should always be read right before creating any new connection.
    """
    with open("/snowflake/session/token", "r") as f:
        return f.read()

def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    if os.path.exists("/snowflake/session/token"):
        return {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "host": os.environ.get("SNOWFLAKE_HOST"),
            "authenticator": "oauth",
            "token": get_login_token(),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
            "role": os.environ.get("SNOWFLAKE_ROLE"),
            "application": "BATCH_CONNECTOR"
        }
    else:
        # Fallback to user/password if not in container environment
        return {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "host": os.environ.get("SNOWFLAKE_HOST"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            "password": os.environ.get("SNOWFLAKE_PASSWORD"),
            "role": os.environ.get("SNOWFLAKE_ROLE"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
            "application": "BATCH_CONNECTOR"
        }

def create_session(connection_params=None):
    """
    Create a Snowpark session using Snowflake-provided connection parameters
    
    Args:
        connection_params (dict, optional): Connection parameters as a dictionary
        
    Returns:
        snowflake.snowpark.Session: Snowpark session
    """
    try:
        # If connection parameters are provided, use them
        if connection_params:
            logger.info("Creating session using provided connection parameters")
            return Session.builder.configs(connection_params).create()
        
        # Otherwise, get parameters from environment
        params = get_connection_params()
        logger.info(f"Creating session with parameters: {params}")
        return Session.builder.configs(params).create()
    except Exception as e:
        logger.error(f"Error creating Snowpark session: {str(e)}")
        raise

def main():
    """Main function to execute the script"""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Create Snowflake session using connection parameters if provided, otherwise use environment
        connection_params = json.loads(args.connection_parameters) if args.connection_parameters else None
        session = create_session(connection_params)
        
        # Print out current session context information
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        logger.info(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )
        
        # Get the underlying connection
        conn = session._conn._conn
        
        # Call the sync function
        logger.info("Starting stream sync process")
        result = sync_stream_to_batch(
            conn,
            args.project_key,
            args.source_stream,
            args.id_column,
            args.date_columns,
            args.url_columns,
            args.api_credentials_table
        )
        
        logger.info(f"Final result: {result}")
        
        # Close the session
        session.close()
        return result
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()