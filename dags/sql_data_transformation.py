import json
from airflow.utils.dates import days_ago
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.decorators import dag,  task
from airflow.models import Variable
args = {
    'url':Variable.get('FILE_URL'),
    'transformation':Variable.get('TRANSFORMATION'),
    'filepath':Variable.get('DATAPATH'),
    'filename':Variable.get('FILENAME'),
    'connection':Variable.get('CONNECTION')
}
default_args = {
    'owner':'nshk'
}
def _read_data(**kwargs):
    import pandas as pd
    url = kwargs['url']
    df = pd.read_csv(url)
    return df.to_json()
@dag(
    dag_id='sql_data_transformation',
    description='Data transformation with SQL operator',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['sqlite','test','python','taskflow','xcom']
)
def sql_data_transformation_api():
    read_data = PythonVirtualenvOperator(
            task_id='read_data',
            python_callable=_read_data,
            requirements=['pandas'],
            system_site_packages=False,
        )
    @task
    def create_table(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_id='read_data')
        
