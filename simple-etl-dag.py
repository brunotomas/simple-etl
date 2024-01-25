from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task

from extraction import query_mysql_and_save_parquet
from redshift import check_last_update, upsert_data

@dag(schedule_interval=None)
def simple_etl():

    start = EmptyOperator(task_id='start')

    @task
    def check_target_last_update():
        check_last_update()

    last_update = check_target_last_update()

    @task
    def incremental_extract(last_update):
        query_mysql_and_save_parquet('queries/get_books_incremental.sql', last_update)


    @task
    def full_extract():
        query_mysql_and_save_parquet('queries/get_books_full.sql')
    

    @task.branch
    def check_full_or_incremental_extract(last_update):
        if last_update:
            incremental_extract(last_update)
        else:
            full_extract()


    extract = check_full_or_incremental_extract

    @task
    def load(s3_path):
        upsert_data(s3_path)
    
    end = EmptyOperator(task_id='end')
    
    start >> load(extract(last_update)) >> end

simple_etl()