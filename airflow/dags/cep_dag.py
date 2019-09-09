import datetime

from airflow.utils.helpers import chain

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftCustomOperator)

from helpers.cep_queries import cep_queries

s3_bucket = 'rlsr-dend'
s3_key = 'cepaberto'

dag = DAG(
    'cep_dag',
    description='Load CEP data from S3 to Redshift',
    start_date=datetime.datetime(2019, 9, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
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
    s3_bucket=s3_bucket,
    s3_key=f'{s3_key}/states.csv',
    schema='staging',
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
    s3_bucket=s3_bucket,
    s3_key=f'{s3_key}/cities.csv',
    schema='staging',
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
    s3_bucket=s3_bucket,
    s3_key=f'{s3_key}/cep/',
    schema='staging',
    table='cep',
    copy_options=[
        "delimiter ','",
        "csv quote as '\"'"
    ]
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    [create_states_table, create_cities_table],
    [load_states_data, load_cities_table],
    create_cep_table,
    load_cep_table_data,
    end_operator
)
