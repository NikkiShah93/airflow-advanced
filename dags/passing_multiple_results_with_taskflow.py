from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner':'nshk'
}
@dag(
    dag_id='passing_multiple_results_with_taskflow',
    description='Passing multiple results with taskflow',
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
    @task(
            multiple_outputs=True
    )
    def calculate_sum_and_mean(items):
        total = 0
        for _, val in items.items():
            total += val
        mean = total / len(items)
        return {'total':total, 'mean':mean}
    @task
    def print_result(total, mean):
        print(f'The total amount is {total}')
        print(f'The mean amount is {mean}')
    
    items = item_mapping() 
    total_and_mean = calculate_sum_and_mean(items)
    print_result(
        total_and_mean['total'], 
        total_and_mean['mean']
        )

xcom_with_taskflow_api()