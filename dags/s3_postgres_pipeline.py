import os
from pathlib import Path
import json
import csv
from io import StringIO
from operator import itemgetter
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


CWD = os.getcwd()
DATA_PATH = f'{CWD.replace('\\','/')}/dags/datasets'
s3_conn = 'S3_CONNECTION'
pg_conn = 'POSTGRES'
pg_car_conn = 'PG_CONNECTION_CAR'
bucket_name = Variable.get('BUCKET_NAME')
file_key = Variable.get('FILE_KEY')
cols = ['Brand','Model','BodyStyle','Seats','PriceEuro']
table_cols = ['brand','model','body_style','seat','price']

default_args = {
    'owner':'nshk'
}

@dag(
    dag_id='s3_postgres_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{CWD.replace('\\','/')}/dags/sql_statements',
    tags=['test','postgres','s3','pipeline']
)
def s3_postgres_pipeline_api():
    # create_db = PostgresOperator(
    #     task_id='create_db',
    #     postgres_conn_id=pg_conn,
    #     autocommit=True,
    #     sql='CREATE DATABASE car_db;'
    # )
    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id=pg_car_conn,
        sql='DROP TABLE IF EXISTS car_data;'
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=pg_car_conn,
        sql='create_car_table.sql'
    )
    @task
    def get_data_from_s3(bucket_name, file_key, data_path):
        s3_hook = S3Hook(
            aws_conn_id = s3_conn
        )
        obj = s3_hook.read_key(bucket_name=bucket_name, key=file_key)
        if isinstance(obj, bytes):
            obj = StringIO(obj.decode('utf-8'))
        with open(f'{data_path}/s3_file.csv', 'w') as f:
            f.writelines(obj)

        return f'{data_path}/s3_file.csv'
    
    
    def _insert_data(**kwargs):
        pg_hook = PostgresHook(
            postgres_conn_id=pg_car_conn
        )
        file_path = kwargs['templates_dict']['file_path']
        cols = kwargs['templates_dict']['cols']
        table_cols = kwargs['templates_dict']['table_cols']
        with open(file_path, newline='') as f:
            content = csv.DictReader(f)
            for row in content:
                print(row, type(row))
                vals = itemgetter(*cols)(row)
                sql = f"""
                INSERT INTO car_data ({','.join(table_cols)})
                VALUES ('{"','".join([x.strip() if isinstance(x, str) else x for x in vals])}')
                    """
                pg_hook.run(sql)

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=_insert_data,
        provide_context=True,
        # op_kwargs={'cols':cols},
        templates_dict={'file_path':'{{ ti.xcom_pull(task_ids="get_data_from_s3") }}',
                        'cols':cols,
                        'table_cols':table_cols}
    )
    
    # create_db >> 
    drop_table >> create_table >> get_data_from_s3(bucket_name=bucket_name,
                                     file_key=file_key, 
                                     data_path=DATA_PATH) >>\
                                     insert_data

s3_postgres_pipeline_api()
