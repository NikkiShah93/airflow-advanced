import os
from pathlib import Path
import csv
import glob
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
CWD = os.getcwd()
DATA_PATH = Path(f'{CWD.replace('\\','/')}/datasets')
# DATA_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = Path(f'{CWD.replace('\\','/')}/output')
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
FILE_PATH = DATA_PATH/'laptop_*.csv'
FILE_COLS = ['Id','Company', 'Product', 'TypeName', 'Price_euros']
CONN_ID = Variable.get('PG_CONNECTION')
default_args = {
    'owner':'nshk'
}
@dag(
    dag_id='file_sensor_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=DATA_PATH,
    tags=['test','postgres','file_sensor','pipeline']
)
def file_sensor_pipeline_api():
    creat_db = PostgresOperator(
        task_id='create_db',
        postgres_conn_id=CONN_ID,
        autocommit=True,
        sql="CREATE DATABASE laptops;"
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=CONN_ID,
        sql='create_laptop_table.sql'
    )
    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath=str(FILE_PATH),
        poke_interval=10,
        timeout=10*10
    )
    @task
    def insert_data():
        sql = f"""
            INSERT INTO laptops ({','.join(FILE_COLS)})
            VALUES 
        """
        for fn in glob.glob(str(FILE_PATH)):
            with open(fn) as f:
                next(f)
                for r in f:
                    sql += f"""('{"','".join(r.split(','))}')"""
            sql_operator = PostgresOperator(
                task_id='insert_data',
                postgres_conn_id=CONN_ID,
                sql = sql
            )
    @task(
            task_id='filter_by_type',
            multiple_outputs=True
            )
    def filter_by_type():
        sql = f"""
        SELECT {','.join(FILE_COLS)}
        FROM laptops
        WHERE TypeName = 'palceholder'
        """
        sql_opt_gaming = PostgresOperator(
            task_id='gaming',
            postgres_conn_id=CONN_ID,
            sql=sql.replace('placeholder','Gaming')
        )
        sql_opt_notebook = PostgresOperator(
            task_id='notebook',
            postgres_conn_id=CONN_ID,
            sql=sql.replace('placeholder','Notebook')
        )
        sql_opt_ultrabook = PostgresOperator(
            task_id='ultrabook',
            postgres_conn_id=CONN_ID,
            sql=sql.replace('placeholder','Ultrabook')
        )
        return {'gaming':sql_opt_gaming,
                'notebook':sql_opt_notebook,
                'ultrabook':sql_opt_ultrabook}
    @task.virtualenv(
            task_id='write_files',
            requirements=['pandas'],
            system_site_packages=False
    )
    def write_file(file_dict):
        import pandas as pd
        print(file_dict)
        # for f in dict(file_dict):
        #     df = pd.read_json(file_dict[f])
        #     df.to_csv(f'{OUTPUT_PATH}/{f}.csv',index=False)

    # remove_files = BashOperator(
    #     task_id='remove_files',
    #     bash_command=f'rm {FILE_PATH}'
    # )

    creat_db >> create_table >> file_sensor >>\
          insert_data() >> write_file(filter_by_type()) 
    
file_sensor_pipeline_api()

    
        