import datetime

from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (S3ToRedshiftCustomOperator)
from helpers.brzipcode_queries import brzipcode_queries

s3_bucket = 'rlsr-dend'
s3_key = 'brzipcode'

dag = DAG(
    'brzipcode_dag',
    description='Load Brazilian ZIP code data from S3 to Redshift',
    start_date=datetime.datetime(2019, 9, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

create_brzipcode_table = PostgresOperator(
    task_id='create_brzipcode_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        brzipcode_queries['drop_staging_brzipcode'],
        brzipcode_queries['create_brzipcode_table'],
    ]
)

load_brzipcode_table_data = S3ToRedshiftCustomOperator(
    task_id='load_brzipcode_table_data',
    dag=dag,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    schema='staging',
    table='brzipcode',
    copy_options=[
        "delimiter ','",
        "csv quote as '\"'",
        "IGNOREHEADER AS 1",
        "EMPTYASNULL",
        "NULL AS 'NULL'",
    ]
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    create_brzipcode_table,
    load_brzipcode_table_data,
    end_operator
)
