import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner':'nshk'
}

@dag(
    dag_id='dag_with_taskflow',
    description='Using taskflow for the pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test','python','taskflow']
)
def dag_with_taskflow_api():
    @task
    def task_a():
        print('Task a has been executed!')
    @task
    def task_b():
        print('Task b has started!')
        time.sleep(4)
        print('Task b has ended!')
    @task
    def task_c():
        print('Task c has started!')
        time.sleep(2)
        print('Task c has ended!')
    task_a() >> [task_b(), task_c()]

_ = dag_with_taskflow_api()