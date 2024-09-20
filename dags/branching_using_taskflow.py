from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed
from airflow.models import Variable

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
    dag_id='branching_using_operator',
    description='Branching with operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python', 'test', 'branching','operator']
)
def branching_using_operator_api():
    if not is_venv_installed():
        print('venv should be installed for this DAG!')
    else:
        @task.virtualenv(
            task_id='extract_data',
            requirements=['pandas'],
            system_site_packages=False
        )
        def extract_data(**kwargs):
            import pandas as pd
            url, filepath, filename = kwargs['url'], kwargs['filepath'], kwargs['filename']
            df = pd.read_csv(url)
            df.to_csv(f'{filepath}/{filename}.csv',index=False)
            return df.to_json()
        @task.branch(
            task_id='branch'
        )
        def branch(**kwargs):
            transformation = kwargs['transformation']
            if transformation == 'filter_two_seaters':
                return 'filter_two_seaters_task'
            elif transformation == 'filter_fwds':
                return 'filter_fwds_task'
        @task.virtualenv(
            task_id='filter_two_seaters_task',
            requirements=['pandas'],
            system_site_packages=False,
            multiple_outputs=True
        )
        def filter_two_seaters_task(**kwargs):
            import pandas as pd
            filepath, filename = kwargs['filepath'], kwargs['filename']
            df = pd.read_csv(f'{filepath}/{filename}.csv')
            df = df[df['Seats'] == 2]
            df.to_csv(f'{filepath}/two_seater.csv', index=False)
            # return {'transform_result':df.to_json(),
            #         'transform_filename':'two_seater'}
        @task.virtualenv(
            task_id='filter_fwds_task',
            requirements=['pandas'],
            system_site_packages=False,
            multiple_outputs=True
        )
        def filter_fwds_task(**kwargs):
            import pandas as pd
            filepath, filename = kwargs['filepath'], kwargs['filename']
            df = pd.read_csv(f'{filepath}/{filename}.csv')
            df = df[df['PowerTrain'] == 'FWD']
            df.to_csv(f'{filepath}/fwd.csv', index=False)
            # return {'transform_result':df.to_json(),
            #         'transform_filename':'fwd'}
        extract_data(args) >> branch(args) >> \
            [filter_fwds_task(args), filter_two_seaters_task(args)]

branching_using_operator_api()