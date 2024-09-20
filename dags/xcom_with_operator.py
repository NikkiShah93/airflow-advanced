import time
import json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nshk'
}
def _item_mapping(**kwargs):
    ti = kwargs['ti']
    items = {
        'i1':230,
        'i2':210,
        'i3':190
    }
    items_string = json.dumps(items)
    ti.xcom_push('items', items_string)
def _calculate_sum(**kwargs):
    ti = kwargs['ti']
    items_string = ti.xcom_pull(task_ids='item_mapping',key='items')
    items = json.loads(items_string)
    total = 0
    for _, val in items.items():
        total += val
    ti.xcom_push('total',total)
def _calculate_mean(**kwargs):
    ti = kwargs['ti']
    items_string = ti.xcom_pull(task_ids='item_mapping', key='items')
    items = json.loads(items_string)
    total = 0
    for _, val in items.items():
        total += val
    mean = total / len(items)
    ti.xcom_push('mean',mean)
def _print_result(**kwargs):
    ti = kwargs['ti']
    total = ti.xcom_pull(task_ids='calculate_sum',key = 'total')
    mean = ti.xcom_pull(task_ids='calculate_mean',key='mean')
    print(f'The total amount is {total}')
    print(f'The mean amount is {mean}')
    
with DAG(
    dag_id='xcom_with_operator',
    description='Cross-communication with operators',
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python','test','xcom','operator']
) as dag:
    item_mapping = PythonOperator(
        task_id='item_mapping',
        python_callable=_item_mapping
    )
    
    calculate_mean = PythonOperator(
        task_id='calculate_mean',
        python_callable=_calculate_mean
    )
    
    calculate_sum = PythonOperator(
        task_id='calculate_sum',
        python_callable=_calculate_sum
    )
    print_result = PythonOperator(
        task_id='print_result',
        python_callable=_print_result
    )

    item_mapping >> [calculate_mean, calculate_sum] >> print_result