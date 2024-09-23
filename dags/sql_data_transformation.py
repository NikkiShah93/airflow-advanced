import json
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.decorators import dag,  task
from airflow.models import Variable
args = {
    'car_url':Variable.get('CAR_FILE_URL'),
    'category_url':Variable.get('CAT_FILE_URL'),
    'transformation':Variable.get('TRANSFORMATION'),
    'filepath':Variable.get('DATAPATH'),
    'filename':Variable.get('FILENAME'),
    'connection':Variable.get('CONNECTION')
}
default_args = {
    'owner':'nshk'
}
def _read_data(url):
    import pandas as pd
    df = pd.read_csv(url)
    return df.to_json()
def _create_car_table(**kwargs):
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
def _create_category_table(**kwargs):
    connection_id = kwargs['connection']
    sql = """
    CREATE TABLE IF NOT EXISTS car_category(
    id INT PRIMARY KEY,
    brand TEXT NOT NULL,
    cetegory TEXT NOT NULL
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
        bash_command="""cd ../&&\\
                        mkdir -p db &&\\
                        cd db &&\\
                        pwd &&\\
                        echo creating the file&&\\
                        touch car_db.db &&\\
                        ls &&\\
                        /usr/bin/sqlite3 /tmp/db/car_db.db &&\\
                        /usr/bin/sqlite3 .database
                        """
    )
    read_car_data = PythonVirtualenvOperator(
            task_id='read_car_data',
            python_callable=_read_data,
            requirements=['pandas'],
            system_site_packages=False,
            op_kwargs={'url':args['car_url']}
        )
    read_category_data = PythonVirtualenvOperator(
            task_id='read_category_data',
            python_callable=_read_data,
            requirements=['pandas'],
            system_site_packages=False,
            op_kwargs={'url':args['category_url']}
        )
    create_car_table = PythonOperator(
            task_id='create_car_table',
            python_callable=_create_car_table,
            op_kwargs=args
        )
    create_category_table = PythonOperator(
            task_id='create_category_table',
            python_callable=_create_category_table,
            op_kwargs=args
        )

    @task.virtualenv(
            task_id='insert_car_data',
            requirements=['pandas'],
            system_site_packages=True
    )
    def insert_car_data(json_data, connection_id):
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
                task_id='insert_car_data',
                sqlite_conn_id=connection_id,
                sql=sql,
                parameters=tuple(record.values())
            )
    @task.virtualenv(
            task_id='insert_category_data',
            requirements=['pandas'],
            system_site_packages=True
    )
    def insert_category_data(json_data, connection_id):
        import pandas as pd
        from airflow.operators.sqlite_operator import SqliteOperator
        df = pd.read_json(json_data)
        sql = """
            INSERT INTO car_category (brand, category)
            VALUES (?, ?)
        """
        df = df.applymap([lambda x:x.strip() if isinstance(x, str) else x])
        parameters = df.to_dict(orient='records')
        for record in parameters:
            sqlite_opt = SqliteOperator(
                task_id='insert_category_data',
                sqlite_conn_id=connection_id,
                sql=sql,
                parameters=tuple(record.values())
            )
    @task
    def join(connection_id):
        sql = """
            CREATE TABLE IF NOT EXISTS joined_car_category AS 
            SELECT c.brand, 
                   c.model, 
                   c.body_style, 
                   c.price,
                   ct.category
            FROM
            car_data as c 
            LEFT JOIN
            car_category ct
            ON c.brand = ct.brand;
        """
        
        sqlite_opt = SqliteOperator(
            task_id='join',
            sqlite_conn_id=connection_id,
            sql=sql
        )
        sqlite_opt.execute(context=None)

    create_db
    # join = join(connection_id=args['connection'])

    # create_db >> [create_car_table, create_category_table] >> read_car_data
    # create_db >> [create_car_table, create_category_table] >> read_category_data
    # read_car_data >> insert_car_data(json_data=read_car_data.output, 
    #                 connection_id=args['connection']) >>\
    #                 join,
    # read_category_data >> insert_category_data(json_data=read_category_data.output, 
    #                 connection_id=args['connection']) >> \
    #                 join
    
        



sql_data_transformation_api()


