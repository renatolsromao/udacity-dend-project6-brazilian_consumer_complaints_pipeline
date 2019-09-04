import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftCustomOperator)

from helpers.cep_queries import cep_queries

s3_key = 'rlsr-dend'
s3_bucket = 'cepaberto'

dag = DAG(
    'cep_dag',
    description='Load CEP data from S3 to Redshift',
    start_date=datetime.datetime(2019, 9, 1),
    catchup=False
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

create_states_table = PostgresOperator(
    task_id='create_states_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=cep_queries['create_states_table']
)

load_states_data = S3ToRedshiftCustomOperator(
    task_id='load_states_data',
    dag=dag,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    s3_key=s3_key,
    s3_bucket=f'{s3_bucket}/states.csv',
    schema='public',
    table='states',
    copy_options=[
        "delimiter ','",
        "csv quote as '\"'"
    ]
)

create_cities_table = PostgresOperator(
    task_id='create_cities_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=cep_queries['create_cities_table']
)

load_cities_table = S3ToRedshiftCustomOperator(
    task_id='load_cities_table',
    dag=dag,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    s3_key=s3_key,
    s3_bucket=f'{s3_bucket}/cities.csv',
    schema='public',
    table='cities',
    copy_options=[
        "delimiter ','",
        "csv quote as '\"'"
    ]
)

create_cep_table = PostgresOperator(
    task_id='create_cep_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=cep_queries['create_cep_table']
)

load_cep_table_data = S3ToRedshiftCustomOperator(
    task_id='load_cep_table_data',
    dag=dag,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    s3_key=s3_key,
    s3_bucket=f'{s3_bucket}/cep/',
    schema='public',
    table='cep',
    copy_options=[
        "delimiter ','",
        "csv quote as '\"'"
    ]
)

create_states_table.set_upstream(start_operator)
create_cities_table.set_upstream(start_operator)

load_states_data.set_upstream(create_states_table)
load_cities_table.set_upstream(create_cities_table)

create_cep_table.set_upstream([load_cities_table, load_states_data])
load_cep_table_data.set_upstream(create_cep_table)
