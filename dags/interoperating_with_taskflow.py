from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

args = {
    'url':Variable.get('FILE_URL'),
    'transformation':Variable.get('TRANSFORMATION'),
    'filepath':Variable.get('DATAPATH'),
    'filename':Variable.get('FILENAME')
}
default_args = {
    'owner':'nshk'
}
@dag(
    dag_id='interoperating_with_taskflow',
    description='Moving data between tasks',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test','python','taskflow']
)
def interoperating_with_taskflow_api():
    @task.virtualenv(
        task_id='read_file',
        requirements=['pandas'],
        op_kwargs=args
    )
    def read_file(**kwargs):
        import pandas as pd
        url = kwargs['url']
        filepath = kwargs['filepath']
        filename = kwargs['filename']
        df = pd.read_csv(url)
        df.to_csv(f'{filepath}/{filename}.csv', index=False)
        return df.to_json()
    @task.branch
    def branch():
        transformation = Variable.get('TRANSFORMATION')
        if transformation == 'filter_two_seaters':
            return 'filter_two_seaters_task'
        elif transformation == 'filter_fwds':
            return 'filter_fwds_task'
    @task.virtualenv(
        task_id='filter_two_seaters_task',
        requirements=['pandas']
    )
    def filter_two_seaters_task(json_data):
        import pandas as pd
        df = pd.read_json(json_data)
        df = df[df['Seats'] == 2]
        return df.to_json()
    json_data = read_file()

interoperating_with_taskflow_api()