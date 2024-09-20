import json 
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner':'nshk'
}
@dag(
    dag_id='xcom_with_taskflow',
    description='Cross-communication with taskflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test','python','taskflow','xcom']
)
def xcom_with_taskflow_api():
    @task
    def item_mapping():
        items = {
            'i1':230,
            'i2':210,
            'i3':190
        }
        return items
    @task
    def calculate_sum(items):
        total = 0
        for _, val in items.items():
            total += val
        return total
    @task
    def calculate_mean(items):
        total = 0
        for _, val in items.items():
            total += val
        mean = total / len(items)
        return mean
    @task
    def print_result(total, mean):
        print(f'The total amount is {total}')
        print(f'The mean amount is {mean}')
    
    items = item_mapping() 
    total, mean = calculate_sum(items),calculate_mean(items) 
    print_result(total=total, mean=mean)

xcom_with_taskflow_api()