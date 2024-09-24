import os
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

CWD = os.getcwd()
TMP_PATH = Path(f'{CWD.replace('\\','/')}/tmp')
TMP_PATH.mkdir(parents=True, exist_ok=True)
default_args={
    'owner':'nshk'
}
with DAG(
    dag_id='simple_file_sensor',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test', 'python', 'file_sensor']
) as dag:
    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath=TMP_PATH/'file.csv',
        poke_interval=10,
        timeout=10*10
    )

    file_sensor