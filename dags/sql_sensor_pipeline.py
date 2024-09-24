import os
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor

CWD = os.getcwd()
CONN_ID = 'PG_CONN_LAPTOPS'
default_args={
    'owner':'nshk'
}
@dag(
    dag_id='sql_sensor_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{CWD.replace('\\','/')}/dags/sql_statements',
    tags=['test','python','postgres','sql_sensor','pipeline']
)
def sql_sensor_pipeline_api():
    # create_laptop_table = PostgresOperator(
    #     task_id='create_laptop_table',
    #     postgres_conn_id=CONN_ID,
    #     sql='create_laptop_table.sql'
    # )
    create_premium_laptop_table = PostgresOperator(
        task_id='create_premium_laptop_table',
        postgres_conn_id=CONN_ID,
        sql='create_premium_laptop_table.sql'
    )
    sql_sensor = SqlSensor(
        task_id='sql_sensor',
        conn_id=CONN_ID,
        sql="SELECT EXISTS(SELECT 1 FROM laptops WHERE price_euros > 500)",
        poke_interval=10,
        timeout=10*10
    )
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id=CONN_ID,
        sql="""INSERT INTO premium_laptops 
        SELECT * FROM laptops 
        WHERE price_euros > 500"""
    )
    remove_data = PostgresOperator(
        task_id='remove_data',
        postgres_conn_id=CONN_ID,
        sql="""DELETE TABLE laptops;"""
    )

    # create_laptop_table >> 
    create_premium_laptop_table >>\
    sql_sensor >> insert_data >> remove_data

sql_sensor_pipeline_api()