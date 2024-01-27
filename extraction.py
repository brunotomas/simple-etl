from datetime import datetime
import pandas as pd
from db_config import SOURCE_DB_CONFIG
from utils import mysql_connect


def query_mysql_and_save_parquet(query_file_path: str, process_date: str=None):
    
    connection = mysql_connect(SOURCE_DB_CONFIG)

    with open(query_file_path, "r") as sql_file:
        sql_query = sql_file.read()
    
    output_parquet_file = f"extracts/output_data_{int(datetime.timestamp(datetime.now()))}.parquet"

    try:
        if process_date:
            result = pd.read_sql_query(sql_query, connection, params=[process_date])
        else:
            result = pd.read_sql_query(sql_query, connection)
        print(result.size)
        result.to_parquet(output_parquet_file, index=False)

        return output_parquet_file
    finally:
        connection.close()
   

if __name__ == "__main__":
    
    query_mysql_and_save_parquet("queries/get_books_incremental.sql", '2024-01-15')
    #query_mysql_and_save_parquet("queries/get_books_full.sql")
