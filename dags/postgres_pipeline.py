import os
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

CWD = os.getcwd()
conn_id = Variable.get('PG_CONNECTION')
default_args ={
    'owner':'nshk'
}

@dag(
    dag_id='postgres_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{CWD.replace('\\','/')}/dags/sql_statements',
    tags=['test','postgres','taskflow','pipeline']
)
def postgres_pipeline_api():
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
    create_customer_table >> create_customer_purchase_table >>\
          insert_customer >> insert_customer_purchases

postgres_pipeline_api()