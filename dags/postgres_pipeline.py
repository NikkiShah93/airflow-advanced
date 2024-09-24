import os
import csv
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

CWD = os.getcwd()
DATA_PATH = Path(f'{CWD.replace('\\','/')}/datasets')
DATA_PATH.mkdir(parents=True, exist_ok=True)
conn_id = Variable.get('PG_CONNECTION')
default_args ={
    'owner':'nshk'
}

def write_to_csv(ti):
    filtered_data = ti.xcom_pull(task_ids='filter')
    with open(f'{CWD.replace('\\','/')}/datasets/filtered_customers.csv', 
              'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Customer_ID', 'Customer_name',
                          'Product', 'Price'])
        for row in filtered_data:
            writer.writerow(row)
@dag(
    dag_id='postgres_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{CWD.replace('\\','/')}/dags/sql_statements',
    tags=['test','postgres','taskflow','pipeline']
)
def postgres_pipeline_api():
    
    drop_tables = PostgresOperator(
        task_id='drop_tables',
        postgres_conn_id=conn_id,
        autocommit=True,
        sql="""DROP TABLE IF EXISTS customer_full_details;
               DROP TABLE IF EXISTS customer_purchase;
               DROP TABLE IF EXISTS customer_purchases;
               DROP TABLE IF EXISTS customers;"""
    )
    create_customer_table = PostgresOperator(
        task_id='create_customer_table',
        postgres_conn_id=conn_id,
        sql='create_customer_table.sql'
    )

    create_customer_purchase_table = PostgresOperator(
        task_id='create_customer_purchase_table',
        postgres_conn_id=conn_id,
        sql='create_customer_purchase.sql'
    )

    insert_customer = PostgresOperator(
        task_id='insert_customer',
        postgres_conn_id=conn_id,
        sql='insert_customers.sql'
    )
    insert_customer_purchases = PostgresOperator(
        task_id='insert_customer_purchases',
        postgres_conn_id=conn_id,
        sql='insert_customer_purchases.sql'
        )
    join = PostgresOperator(
        task_id='join',
        postgres_conn_id=conn_id,
        sql='joins.sql'
    )
    filter = PostgresOperator(
        task_id='filter',
        postgres_conn_id=conn_id,
        sql = """
        SELECT customer_id, 
            customer_name, 
            product, 
            price
        FROM customer_full_details
        WHERE price BETWEEN %(lower_bound)s 
                AND %(upper_bound)s
        """,
        parameters={'lower_bound':5,
                    'upper_bound':10}
    )
    write_csv_file = PythonOperator(
        task_id='write_csv_file',
        python_callable=write_to_csv
    )
    drop_tables >> create_customer_table >> create_customer_purchase_table >>\
          insert_customer >> insert_customer_purchases >>\
          join >> filter >> write_csv_file

postgres_pipeline_api()