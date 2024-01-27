from datetime import date

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task

from extraction import query_mysql_and_save_parquet
from persistence import upload_parquet_to_s3
from redshift import check_last_update, upsert_data

@dag(schedule=None)
def simple_etl():

    start = EmptyOperator(task_id='start')

    @task
    def check_target_last_update():
        last_update = date.strftime(check_last_update(), '%Y-%m-%d')
        
        context = get_current_context()
        ti = context['ti']
        ti.xcom_push("last_update", last_update)
        return last_update


    @task
    def incremental_extract():

        context = get_current_context()
        ti = context['ti']
        last_update = ti.xcom_pull(key="return_value", task_ids='check_target_last_update')
        
        parquet_file = query_mysql_and_save_parquet('queries/get_books_incremental.sql', last_update)
        ti.xcom_push("parquet_file_inc", parquet_file)
        return parquet_file


    @task
    def full_extract():

        parquet_file = query_mysql_and_save_parquet('queries/get_books_full.sql')

        context = get_current_context()
        ti = context['ti']
        ti.xcom_push("parquet_file_full", parquet_file)
        return parquet_file
    

    @task.branch
    def check_full_or_incremental_extract(last_update):

        extract_type = 'incremental_extract' if last_update else 'full_extract'
        return extract_type
    
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def s3_upload():

        context = get_current_context()
        ti = context['ti']
        extract_type = ti.xcom_pull(key='return_value', task_ids='check_full_or_incremental_extract')
        parquet_file = ti.xcom_pull(key="parquet_file", task_ids=extract_type)
        
        s3_file_path = upload_parquet_to_s3(f'extracts/{parquet_file}', 'books-landing')
        return s3_file_path
    
    @task
    def load_into_target(s3_path):

        upsert_data(s3_path)
    
    end = EmptyOperator(task_id='end')
    
    
    last_update = check_target_last_update()
    branch = check_full_or_incremental_extract(last_update)
    full = full_extract()
    incremental = incremental_extract()
    upload = s3_upload()
    load = load_into_target(upload)


    start >> last_update >> branch 
    branch >> full >> upload 
    branch >> incremental >> upload
    upload >> load >> end

simple_etl()