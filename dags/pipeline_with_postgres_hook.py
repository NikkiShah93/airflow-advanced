from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner':'nshk'
}
CONN_ID='POSTGRES_ORG'
emp_data = [
    ('Sarah', 27, 1),
    ('Peter', 30, 2),
    ('John', 33, 1),
    ('Kelly', 37, 3),
]
departments = [
    'Engineering',
    'Sales',
    'Marketing'
]

@dag(
    dag_id = 'pipeline_with_postgres_hook',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test', 'python','postgres_hook','pipeline']
)
def pipeline_with_postgres_hook_api():
    # create_db = PostgresOperator(
    #     task_id='create_db',
    #     postgres_conn_id='POSTGRES',
    #     autocommit=True,
    #     sql=""" DROP DATABASE IF EXISTS organization;
    #             CREATE DATABASE organization;"""
    # )
    drop_tables = PostgresOperator(
        task_id='drop_tables',
        postgres_conn_id=CONN_ID,
        autocommit=True,
        sql=""" DROP TABLE IF EXISTS employee;
                DROP TABLE IF EXISTS department;"""
    )
    # @task
    # def create_db():
    #     pg_hook = PostgresHook(
    #         postgres_conn_id='POSTGRES',
    #         options={"autocommit":True}
    #         )
    #     query = "CREATE DATABASE organization;"
    #     pg_hook.run(query)
    @task
    def create_emp_table():
        pg_hook = PostgresHook(
            postgres_conn_id=CONN_ID
        )
        query = """
        CREATE TABLE IF NOT EXISTS employee (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        age INT,
        department_id INT NOT NULL 
        );
        """
        pg_hook.run(query)
    @task
    def create_department_table():
        pg_hook = PostgresHook(
            postgres_conn_id=CONN_ID
        )
        query = """
        CREATE TABLE IF NOT EXISTS department (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
        );
        """
        pg_hook.run(query)
    @task(
            task_id='insert_emp_data'
    )
    def insert_emp_data(emp_data):
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        insert_query = """INSERT INTO employee (name, age, department_id)
        VALUES (%s, %s, %s)"""
        for e in emp_data:
            pg_hook.run(insert_query, parameters=e)
        
    @task(
            task_id='insert_dept_data'
    )
    def insert_dept_data(departments_data):
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        insert_query = """INSERT INTO department (name)
        VALUES (%s)"""
        for d in departments:
            pg_hook.run(insert_query, parameters=(d,))
    @task 
    def insert_emp_dept_data():
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        query = """
        CREATE TABLE IF NOT EXISTS employee_department AS
        SELECT 
            e.id as employee_id,
            e.name as employee_name,
            d.name as department_name
        FROM
            employee AS e 
            LEFT JOIN
            department AS d
            ON e.department_id = d.id
        """
        pg_hook.run(query)
    @task 
    def display_result():
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        query = """SELECT * FROM employee_department"""
        results = pg_hook.get_records(query)
        print(results)
    
    # create_db >> 
    drop_tables >> [create_emp_table(), create_department_table()] >>\
    insert_dept_data(departments) >> insert_emp_data(emp_data) >>\
         insert_emp_dept_data() >> display_result()

pipeline_with_postgres_hook_api()
        
