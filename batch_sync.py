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

def sync_table_to_batch(conn, project_key, source_table, id_column, date_columns='', url_columns=''):
    """
    Sync a Snowflake table to Batch.com
    
    Args:
        conn: Snowflake connection
        project_key (str): Batch project key
        source_table (str): Name of the source table
        id_column (str): Column to use as the customer ID
        date_columns (str): Comma-separated list of date columns
        url_columns (str): Comma-separated list of URL columns
    
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
        cursor.execute(f"SELECT * FROM BATCH_API_CREDENTIALS WHERE project_key = '{project_key}'")
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
        
        # Validate the table exists and is accessible
        try:
            # Get columns dynamically from the source table
            logger.info(f"Validating table: {source_table}")
            table_schema = source_table.split('.')[-2]
            table_name = source_table.split('.')[-1]
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{table_schema}'")
            columns = cursor.fetchall()
            column_names = [row[0] for row in columns]
            
            if id_column.upper() not in [col.upper() for col in column_names]:
                error_msg = f"Error: ID column '{id_column}' not found in table '{source_table}'"
                logger.error(error_msg)
                return error_msg
                
            # Build the dynamic SQL query with all columns
            columns_str = ', '.join(column_names)
            query = f"SELECT {columns_str} FROM {source_table}"
            
            # Fetch data from the source table
            logger.info(f"Fetching data from {source_table}")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert to pandas DataFrame for easier processing
            user_df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        except Exception as e:
            error_msg = f"Error accessing table {source_table}: {str(e)}"
            logger.error(error_msg)
            return error_msg
        
        if user_df.empty:
            message = f"No rows found in {source_table}."
            logger.info(message)
            return message
        
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
        logger.info(f"Processing {len(user_df)} rows from {source_table}")
        for index, row in user_df.iterrows():
            try:
                custom_id = str(row[id_column])
                
                # Process attributes with proper data typing
                attributes = {}
                
                for col_name, value in row.items():
                    if col_name == id_column:
                        continue  # Skip the ID column as it's used for identification
                    
                    if pd.isna(value):
                        continue  # Skip None/NaN values
                    
                    # Convert column name to lowercase for consistency
                    attr_name = col_name.lower()
                        
                    # Process based on field type, with appropriate attribute name wrapping
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
                
                if len(user_data_batch) == 1000 or index == len(user_df) - 1:
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
        
        # Save results to a log table if desired
        result_message = f"Table sync complete for {source_table}: {success_count} records succeeded, {fail_count} failed."
        logger.info(result_message)
        
        if error_logs:
            error_detail = "\n".join(error_logs)
            logger.warning(f"Errors encountered during sync:\n{error_detail}")
            result_message += "\n" + error_detail
            
        return result_message
    except Exception as e:
        error_msg = f"Error in sync_table_to_batch: {str(e)}"
        logger.error(error_msg)
        return error_msg

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Sync Snowflake table to Batch.com')
    parser.add_argument('--project-key', required=True, help='Batch project key')
    parser.add_argument('--source-table', required=True, help='Source table name (format: DATABASE.SCHEMA.TABLE)')
    parser.add_argument('--id-column', required=True, help='Column to use as the custom ID')
    parser.add_argument('--date-columns', default='', help='Comma-separated list of date columns')
    parser.add_argument('--url-columns', default='', help='Comma-separated list of URL columns')
    parser.add_argument('--connection-parameters', default='', help='JSON string of connection parameters')
    return parser.parse_args()

def create_session(connection_params=None):
    """
    Create a Snowpark session using either connection parameters or environment variables
    
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
        
        # For Snowpark Container Services, first check if we're running in a container environment
        # with default connection parameters
        logger.info("Checking for Snowflake connection method")
        
        # In a Container Service, we might be able to use getConnection() which 
        # automatically uses the container's credentials
        try:
            from snowflake.connector import connect
            try:
                logger.info("Attempting to use default container service connection")
                conn = connect(application='BATCH_CONNECTOR')
                return Session._from_connection(conn)
            except Exception as container_err:
                logger.info(f"Container service connection not available: {str(container_err)}")
        except ImportError:
            logger.info("snowflake.connector module not available for direct connection")
            
        # Fall back to environment variables
        logger.info("Creating session using environment variables")
        connection_parameters = {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            "password": os.environ.get("SNOWFLAKE_PASSWORD"),
            "private_key_path": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            "private_key_passphrase": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA")
        }
        
        # Filter out None values
        connection_parameters = {k: v for k, v in connection_parameters.items() if v is not None}
        
        if not connection_parameters.get("account") and not (connection_parameters.get("user") or connection_parameters.get("private_key_path")):
            logger.warning("No account or authentication method specified in environment variables")
        
        return Session.builder.configs(connection_parameters).create()
    except Exception as e:
        logger.error(f"Error creating Snowpark session: {str(e)}")
        raise

def main():
    """Main function to execute the script"""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Create Snowflake session
        connection_params = json.loads(args.connection_parameters) if args.connection_parameters else None
        session = create_session(connection_params)
        
        # Get the underlying connection
        conn = session._conn._conn
        
        # Call the sync function
        logger.info("Starting table sync process")
        result = sync_table_to_batch(
            conn,
            args.project_key,
            args.source_table,
            args.id_column,
            args.date_columns,
            args.url_columns
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