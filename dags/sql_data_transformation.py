import json
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
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
def _create_table(**kwargs):
    connection_id = kwargs['connection']
    sql = """
    CREATE TABLE IF NOT EXISTS car_data(
    id INT PRIMARY KEY,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    body_style TEXT NOT NULL,
    seat INT,
    price INT
    );
    """
    sql_opt = SqliteOperator(
        task_id='sqllite_create_table',
        sqlite_conn_id=connection_id,
        sql=sql
    )
    sql_opt.execute(context=None)
@dag(
    dag_id='sql_data_transformation',
    description='Data transformation with SQL operator',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['sqlite','test','python','taskflow','xcom']
)
def sql_data_transformation_api():
    create_db = BashOperator(
        task_id='create_db',
        bash_command="""sudo apt-get install -y sqlite3 libsqlite3-dev \\
            mkdir db && /usr/bin/sqlite3 /db/car_db.db
            """
    )
    read_data = PythonVirtualenvOperator(
            task_id='read_data',
            python_callable=_read_data,
            requirements=['pandas'],
            system_site_packages=False,
            op_kwargs=args
        )
    create_table = PythonOperator(
            task_id='create_table',
            python_callable=_create_table,
            op_kwargs=args
        )
    

    @task.virtualenv(
            task_id='insert_data',
            requirements=['pandas'],
            system_site_packages=True
    )
    def insert_data(json_data, connection_id):
        import pandas as pd
        from airflow.operators.sqlite_operator import SqliteOperator
        df = pd.read_json(json_data)
        sql = """
            INSERT INTO car_data (brand, model, body_style, seat, price)
            VALUES (?, ?, ?, ?, ?)
        """
        df = df.applymap([lambda x:x.strip() if isinstance(x, str) else x])
        parameters = df.to_dict(orient='records')
        for record in parameters:
            sqlite_opt = SqliteOperator(
                task_id='insert_data',
                sqlite_conn_id=connection_id,
                sql=sql,
                parameters=tuple(record.values())
            )

    create_db >> create_table >> read_data >> insert_data(json_data=read_data.output, 
                                             connection_id=args['connection'])
    # json_data = read_data.output
    # insert_data(json_data=json_data, connection_id=args['connection'])



sql_data_transformation_api()


