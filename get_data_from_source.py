import pandas as pd
from db_config import SOURCE_DB_CONFIG
from utils import mysql_connect

def query_mysql_and_save_parquet(query, output_file, process_date):
    
    connection = mysql_connect(SOURCE_DB_CONFIG)
    try:
        result = pd.read_sql_query(query, connection, params=[process_date])
        result.to_parquet(output_file, index=False)
    finally:
        connection.close()
   

if __name__ == "__main__":
    # Example query: Replace this with your actual SQL query
    with open("queries/get_books.sql", "r") as sql_file:
        sql_query = sql_file.read()

    # Output file for the Parquet data
    output_parquet_file = "output_data.parquet"

    # Execute the query and save the result to a Parquet file
    query_mysql_and_save_parquet(sql_query, output_parquet_file, '2024-01-01')
