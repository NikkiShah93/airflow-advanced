from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import (
    PythonOperator, 
    BranchPythonOperator,
    PythonVirtualenvOperator
)
from airflow.models import Variable

default_args = {
    'owner':'nshk'
}
def _extract_data(url):
    import pandas as pd
    df = pd.read_csv(url)
    return df.to_json()
def _branch(transformation):
    if transformation == 'filter_two_seaters':
        return 'filter_two_seaters_task'
    elif transformation == 'filter_fwds':
        return 'filter_fwds_task'
def _filter_two_seaters_task(ti):
    import pandas as pd
    json_data = ti.xcom_pull(task_ids='extract_data')
    df = pd.read_json(json_data)
    df = df[df['Seats'] == 2]
    ti.xcom_push(key='transform_result',value=df.to_json())
    ti.xcom_push(key='transform_filename',value='two_seaters')
def _filter_fwds_task(ti):
    import pandas as pd
    json_data = ti.xcom_pull(task_ids='extract_data')
    df = pd.read_json(json_data)
    df = df[df['PowerTrain'] == 'FWD']
    ti.xcom_push(key='transform_result',value=df.to_json())
    ti.xcom_push(key='transform_filename',value='fwd')
def _write_csv(json_data, file_name, data_path):
    import pandas as pd
    # data_path = Variable.get('DATAPATH')
    # json_data = ti.xcom_pull(key='transform_result')
    # file_name = ti.xcom_pull(key='transform_filename')
    df = pd.read_json(json_data)
    df.to_csv(f'{data_path}/{file_name}', index=False)

with DAG(
    dag_id='branching_using_operator',
    description='Branching with operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python', 'test', 'branching','operator']
) as dag:
    extract_data = PythonVirtualenvOperator(
        task_id='extract_data',
        python_callable=_extract_data,
        requirements=['pandas'],
        op_kwargs={'url':Variable.get('FILE_URL')}
        )
    branch = BranchPythonOperator(
            task_id='branch',
            python_callable=_branch,
            op_kwargs={'transformation' : Variable.get('TRANSFORMATION')}
        )
    filter_two_seater = PythonVirtualenvOperator(
        task_id='filter_two_seaters_task',
        python_callable=_filter_two_seaters_task,
        requirements=['pandas']
    )
    filter_fwd = PythonVirtualenvOperator(
        task_id='filter_fwds_task',
        python_callable=_filter_fwds_task,
        requirements=['pandas']
    )
    write_csv = PythonVirtualenvOperator(
        task_id='write_csv',
        python_callable=_write_csv,
        requirements=['pandas'],
        trigger_rule='none_failed',
        op_args = [
            "{{ ti.xcom_pull(key='transform_result') }}",
            "{{ ti.xcom_pull(key='transform_filename') }}",
            Variable.get('DATAPATH')],
        system_site_packages=True
        
    )
    # {
    #         'json_data':"{{ ti.xcom_pull(key='transform_result') }}",
    #         'filename':"{{ ti.xcom_pull(key='transform_filename') }}",
    #         'data_path':Variable.get('DATAPATH')
    #     }
    extract_data >> branch >> [filter_two_seater, filter_fwd] >> write_csv