import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.postgres_operator import PostgresOperator

from helpers.consumidorgovbr_queries import consumidorgovbr_queries
from helpers.dimensions_queries import dimensions_queries
from helpers.fact_queries import fact_queries

s3_bucket = 'rlsr-dend'
s3_origin_folder = 'consumer-complaints/consumidor-gov-br'
s3_origin_file = 'sample.csv'
s3_destination_folder = 'consumer-complaints/consumidor-gov-br-utf'
s3_destination_file = 'sample'

dag = DAG(
    'consumidorgovbr_dag',
    description='Load data from consumidor.gov.br complaints from S3 to Redshift.',
    start_date=datetime.datetime(2019, 8, 1),
    catchup=False
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

transform_script = '/usr/local/airflow/plugins/helpers/transform_to_utf.py'
convert_file_to_utf = S3FileTransformOperator(
    task_id='convert_file_to_utf',
    dag=dag,
    source_s3_key=f's3://{s3_bucket}/{s3_origin_folder}/{s3_origin_file}',
    dest_s3_key=f's3://{s3_bucket}/{s3_destination_folder}/{s3_destination_file}',
    source_aws_conn_id='aws_credentials',
    dest_aws_conn_id='aws_credentials',
    transform_script=transform_script,
    replace=True
)

create_consumidorgov_stage_table = PostgresOperator(
    task_id='create_consumidorgov_stage_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        consumidorgovbr_queries['drop_stage_table'].format(s3_destination_file),
        consumidorgovbr_queries['create_stage_table'].format(s3_destination_file)
    ]
)

load_consumidorgovbr_stage_data = S3ToRedshiftTransfer(
    task_id='load_consumidorgovbr_stage_data',
    dag=dag,
    schema='public',
    s3_bucket=s3_bucket,
    s3_key=s3_destination_folder,
    table=s3_destination_file,
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=[
        "DELIMITER AS ';'",
        "DATEFORMAT AS 'DD/MM/YYYY'",
        "IGNOREHEADER AS 1",
        "EMPTYASNULL"
    ]
)

load_dm_date_data = PostgresOperator(
    task_id='load_dm_date_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_date_table'],
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
        dimensions_queries['create_dm_consumer'],
        consumidorgovbr_queries['insert_dm_consumer']
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

convert_file_to_utf.set_upstream(start_operator)
create_consumidorgov_stage_table.set_upstream(convert_file_to_utf)
load_consumidorgovbr_stage_data.set_upstream(create_consumidorgov_stage_table)

load_dm_date_data.set_upstream(load_consumidorgovbr_stage_data)
load_dm_region_data.set_upstream(load_consumidorgovbr_stage_data)
load_dm_consumer_data.set_upstream(load_consumidorgovbr_stage_data)
load_dm_company_data.set_upstream(load_consumidorgovbr_stage_data)

load_ft_complaints_data.set_upstream([
    load_dm_date_data, load_dm_region_data, load_dm_consumer_data, load_dm_company_data
])
