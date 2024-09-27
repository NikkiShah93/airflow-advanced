import os
from io import StringIO
from pathlib import Path
import csv
from operator import itemgetter
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import (
    PythonOperator, 
    PythonVirtualenvOperator,
    is_venv_installed)
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

CWD = os.getcwd()
DATA_PATH = f"{CWD.replace('\\','/')}/datasets"
RAW_DATA_PATH=Path(f'{DATA_PATH}/raw')
RAW_DATA_PATH.mkdir(exist_ok=True)
PROC_DATA_PATH=Path(f'{DATA_PATH}/proc')
PROC_DATA_PATH.mkdir(exist_ok=True)
BUCKET_NAME=Variable.get('BUCKET_NAME')
PREFIX=Variable.get('S3_PREFIX')
S3_CONN='S3_CONNECTION'
PG_CONN='POSTGRES'
cols=Variable.get('COLS').split(',')
default_args={
    'owner':'nshk',
    'provide_context':True
}
@dag(
    dag_id='daily_data_aggregation',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['test','python', 'bash','pipeline']
)
def daily_data_agg():
    @task(
            task_id='get_data_from_s3',
            templates_dict={'bucket_name':BUCKET_NAME,
                            'prefix':PREFIX,
                            'date':'{{ ds_nodash }}',
                            'data_path':RAW_DATA_PATH}
    )
    def get_data_from_s3(**kwargs):
        bucket_name = kwargs['templates_dict']['bucket_name']
        prefix=kwargs['templates_dict']['prefix']
        date = kwargs['templates_dict']['date']
        data_path = kwargs['templates_dict']['data_path']
        s3_hook = S3Hook(
            aws_conn_id=S3_CONN
        )
        items = s3_hook.list_keys(bucket_name=bucket_name,
                                  prefix=prefix)
        items = [x for x in items if x.endswith('.csv')]
        for item in items:
            content = s3_hook.read_key(bucket_name=bucket_name,
                                       key=item)
            if isinstance(content, bytes):
                content = StringIO(content.decode('utf-8'))
            with open(f'{data_path}/{date}.csv', 'a') as f:
                f.writelines(content)
        return f'{data_path}/{date}.csv'
    @task(
            task_id='data_processing',
            templates_dict={'file_path':"{{ ti.xcom_pull(task_ids='get_data_from_s3') }}",
                            'data_path':PROC_DATA_PATH,
                            'cols':cols,
                            'date':"{{ ds_nodash }}"}
    )
    def process_data(**kwargs):
        file_path = kwargs['templates_dict']['file_path']
        data_path = kwargs['templates_dict']['data_path']
        cols = kwargs['templates_dict']['cols']
        date = kwargs['templates_dict']['date']
        data = [tuple(cols)]
        with open(file_path) as f:
            content = csv.DictReader(f)
            for row in content:
                if '' in itemgetter(*cols)(row) or None in itemgetter(*cols)(row):
                    continue
                data.append(itemgetter(*cols)(row))
        with open(f"{data_path}/{date}.csv",'a') as f:
            for row in data:
                print(row)
                f.write(str(row))
        return f"{data_path}/{date}.csv"
            
    @task.virtualenv(
            task_id = 'data_cleaning',
            requirements=['pandas'],
            templates_dict={'date':'{{ ds_nodash }}'},
            system_site_packages=True
    )
    def data_cleaning(date=None, **contxt):
        print('inside venv')
        print(date)
        print(contxt)
    # if not is_venv_installed():
    #     print('Virtualenv is not installed!')
    #     get_data_from_s3() >> process_data()
    # else:
    #     data_cleaning=PythonVirtualenvOperator(
    #         task_id = 'data_cleaning',
    #         python_callable=_data_cleaning,
    #         requirements=['pandas'],
    #         op_kwargs={'date':'{{ ds_nodash }}'},
    #         system_site_packages=True
    #     )
    get_data_from_s3() >> process_data() >> data_cleaning()

daily_data_agg()
