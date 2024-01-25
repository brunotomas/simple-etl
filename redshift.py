from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from utils import redshift_connect
from db_config import REDSHIFT_CONFIG

def check_last_update():
    conn = redshift_connect(REDSHIFT_CONFIG)

    sql_query = "SELECT MAX(insertion_date) FROM books where insertion_date > '2024-01-20';"
    
    try:
        results = conn.execute(sql_query)
        last_update = results.fetchone()[0]
    finally:
        conn.close()
        return last_update

def upsert_data(s3_path: str):

    with open('queries/upsert_redshift.sql', 'r') as sql_file:
        sql_query = text(sql_file.read()).bindparams(s3path=s3_path)

    conn = redshift_connect(REDSHIFT_CONFIG)
    transaction = conn.begin()

    try:
        conn.execute(sql_query)
        transaction.commit()
        
    except SQLAlchemyError as e:
        transaction.rollback()
    finally:
        conn.close()


if __name__ == "__main__":

    check_last_update(REDSHIFT_CONFIG)
    #upsert_data(REDSHIFT_CONFIG, 's3://books-landing/1705960852.parquet')