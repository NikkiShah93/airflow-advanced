import os
from pathlib import Path
import glob
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator


CWD = os.getcwd()
CONN_ID = 'PG_CONN_LAPTOPS'
DATA_PATH = f"{CWD.replace('\\','/')}/dags/datasets/"
FILE_PATH = f"{DATA_PATH}/laptop_*.csv"
FILE_COLS = ['Id','Company', 'Product', 'TypeName', 'Price_euros']
default_args={
    'owner':'nshk'
}
@dag(
    dag_id='insert_data_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{CWD.replace('\\','/')}/dags/sql_statements',
    tags=['test','python','postgres','pipeline']
)
def insert_data_pipeline_api():
    create_laptop_table = PostgresOperator(
        task_id='create_laptop_table',
        postgres_conn_id=CONN_ID,
        sql='create_laptop_table.sql'
    )
    @task
    def insert_data():
        sql = f"""
            INSERT INTO laptops ({','.join(FILE_COLS)})
            VALUES 
        """
        for fn in glob.glob(FILE_PATH):
            print(fn)
            with open(fn) as f:
                next(f)
                for r in f:
                    sql += f"""('{"','".join(r.split(','))}')"""
            print(sql)
            sql_operator = PostgresOperator(
                task_id='insert_data',
                postgres_conn_id=CONN_ID,
                sql = sql,
                autocommit=True
            )
    pull_data = PostgresOperator(
        task_id='pull_data',
        postgres_conn_id=CONN_ID,
        sql="SELECT * FROM laptops;"
    )
    create_laptop_table >> insert_data() >> pull_data

insert_data_pipeline_api()