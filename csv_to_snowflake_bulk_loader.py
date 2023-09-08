from sqlalchemy import create_engine
import pandas as pd

def load_csvs_to_snowflake_table(
    conn: snowflake.connector.connection.SnowflakeConnection, 
    fully_qualified_table_name: str, 
    csv_file_paths: List[str],
):

    # Step 1: Connect to Snowflake database using `conn` defined in the parameters
    # (Assuming `conn` is a connection object created using create_engine method from SQLAlchemy)
    
    # Step 2: Find all column names in the specific table using the parameter of `fully_qualified_table_name` in such database
    try:
        connection = conn.connect()
        result = connection.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{fully_qualified_table_name}'")
        column_names = [row[0] for row in result.fetchall()]
    except Exception as e:
        print(f"Error in fetching column names: {e}")
        return
    
    # Step 3: Read these csv files one by one using pandas and then check if all columns of each csv are all in the `fully_qualified_table_name`
    for csv_file_path in csv_file_paths:
        try:
            df = pd.read_csv(csv_file_path)
            csv_columns = df.columns.tolist()
            
            # Check if all columns in CSV are in the Snowflake table
            if set(csv_columns).issubset(set(column_names)):
                # Insert all data of this table into the snowflake table using chunk with chunksize of 5000
                try:
                    df.to_sql(fully_qualified_table_name, conn, index=False, method='multi', chunksize=5000)
                except Exception as e:
                    print(f"Error in inserting data from {csv_file_path}: {e}")
            else:
                # Insert data selected columns verified to the snowflake table, still using the same chunksize
                verified_columns = list(set(csv_columns) & set(column_names))
                try:
                    df[verified_columns].to_sql(fully_qualified_table_name, conn, index=False, method='multi', chunksize=5000)
                except Exception as e:
                    print(f"Error in inserting selected columns from {csv_file_path}: {e}")
        except Exception as e:
            print(f"Error in processing CSV file {csv_file_path}: {e}")
    
    # Close the connection
    connection.close()

