import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
default_args = {
    'owner':'nshk'
}
def task_a():
    print('Task a has been executed!')
def task_b():
    print('Task b has started!')
    time.sleep(4)
    print('Task b has ended!')
def task_c():
    print('Task c has started!')
    time.sleep(2)
    print('Task c has ended!')
with DAG(
    dag_id='dag_with_operator',
    description='Using operators for DAG creation',
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test','python','operator']
) as dag:
    task_1 = PythonOperator(
        task_id='task_a',
        python_callable=task_a
    )
    task_2 = PythonOperator(
        task_id='tast_b',
        python_callable=task_b
    )
    task_3 = PythonOperator(
        task_id='tast_c',
        python_callable=task_c
    )
    task_4 = EmptyOperator(
        task_id='task_d'
    )


task_1 >> [task_2, task_3] >> task_4