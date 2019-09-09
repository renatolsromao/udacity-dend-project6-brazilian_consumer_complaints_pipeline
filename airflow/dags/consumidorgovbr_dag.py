import datetime

from airflow.contrib.operators.s3_list_operator import S3ListOperator

from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.sensors import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftCustomOperator, S3ConvertFilesEncodingOperator, S3DeleteFromContextOperator)

from helpers.consumidorgovbr_queries import consumidorgovbr_queries
from helpers.dimensions_queries import dimensions_queries
from helpers.fact_queries import fact_queries

s3_bucket = 'rlsr-dend'
s3_key = 'consumer-complaints/consumidorgovbr'
s3_key_processed = 'consumer-complaints/consumidorgovbr'
table = 'cgb'

dag = DAG(
    'consumidorgovbr_dag',
    description='Load data from consumidor.gov.br complaints from S3 to Redshift.',
    start_date=datetime.datetime(2019, 8, 1),
    schedule_interval=datetime.timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

has_file_to_process = S3KeySensor(
    task_id='has_file_to_process',
    dag=dag,
    bucket_name=s3_bucket,
    bucket_key=f'{s3_key}/*.csv',
    wildcard_match=True,
    aws_conn_id='aws_credentials',
    timeout=31,
    poke_interval=30,
)

convert_file_encoding = S3ConvertFilesEncodingOperator(
    task_id='convert_file_encoding',
    dag=dag,
    aws_conn_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_prefix=s3_key,
    original_encoding='CP1252',
    dest_encoding='UTF-8',
)

create_consumidorgov_stage_table = PostgresOperator(
    task_id='create_consumidorgov_stage_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        consumidorgovbr_queries['drop_stage_table'],
        consumidorgovbr_queries['create_stage_table']
    ]
)

load_consumidorgovbr_stage_data = S3ToRedshiftCustomOperator(
    task_id='load_consumidorgovbr_stage_data',
    dag=dag,
    schema='staging',
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    table=table,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=[
        "DELIMITER AS ';'",
        "DATEFORMAT AS 'DD/MM/YYYY'",
        "IGNOREHEADER AS 1",
        "EMPTYASNULL"
    ]
)

list_s3_processed_s3_files = list_keys = S3ListOperator(
    task_id='list_s3_processed_s3_files',
    dag=dag,
    bucket=s3_bucket,
    prefix=s3_key,
    aws_conn_id='aws_credentials',
)

delete_processed_s3_files = S3DeleteFromContextOperator(
    task_id='delete_proccessed_s3_files',
    dag=dag,
    bucket=s3_bucket,
    context_task_id='list_s3_processed_s3_files',
    aws_conn_id='aws_credentials',
)

load_dm_date_data = PostgresOperator(
    task_id='load_dm_date_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_date'],
        consumidorgovbr_queries['insert_dm_date']
    ]
)

load_dm_region_data = PostgresOperator(
    task_id='load_dm_region_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_region'],
        consumidorgovbr_queries['insert_dm_region']
    ]
)

load_dm_consumer_data = PostgresOperator(
    task_id='load_dm_consumer_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_consumer_profile'],
        consumidorgovbr_queries['insert_dm_consumer_profile']
    ]
)

load_dm_company_data = PostgresOperator(
    task_id='load_dm_company_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_company'],
        consumidorgovbr_queries['insert_dm_company']
    ]
)

load_ft_complaints_data = PostgresOperator(
    task_id='load_ft_complaints_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        fact_queries['create_ft_complaints'],
        consumidorgovbr_queries['insert_ft_complaints']
    ]
)

drop_consumidorgov_stage_table = PostgresOperator(
    task_id='drop_consumidorgov_stage_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        consumidorgovbr_queries['drop_stage_table']
    ]
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    has_file_to_process,
    convert_file_encoding,
    create_consumidorgov_stage_table,
    load_consumidorgovbr_stage_data,
    [load_dm_date_data, load_dm_region_data, load_dm_consumer_data, load_dm_company_data],
    load_ft_complaints_data,
    drop_consumidorgov_stage_table,
    end_operator
)

list_s3_processed_s3_files.set_upstream(load_consumidorgovbr_stage_data)
delete_processed_s3_files.set_upstream(list_s3_processed_s3_files)
