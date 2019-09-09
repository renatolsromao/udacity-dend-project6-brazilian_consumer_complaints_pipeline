import datetime

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.helpers import chain
from airflow.operators.sensors import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (S3ToRedshiftCustomOperator)
from subdags.delete_s3_key_files_subdag import delete_s3_key_files_subdag
from subdags.data_quality_check_subdag import data_quality_check_subdag
from helpers.fact_queries import fact_queries
from helpers.dimensions_queries import dimensions_queries
from helpers.procon_queries import procon_queries

STAGING_TABLE = 'procon'

DAG_NAME = 'procon_dag'
start_date = datetime.datetime(2019, 9, 1)

REDSHIFT_CONN = 'redshift_conn'
AWS_CREDENTIALS = 'aws_credentials'
S3_BUCKET = 'rlsr-dend'
S3_KEY = 'consumer-complaints/procon'

dag = DAG(
    DAG_NAME,
    description='Load data from Procon complaints from S3 to Redshift.',
    start_date=start_date,
    schedule_interval=datetime.timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

has_file_to_process = S3KeySensor(
    task_id='has_file_to_process',
    dag=dag,
    bucket_name=S3_BUCKET,
    bucket_key=f'{S3_KEY}/*.csv',
    wildcard_match=True,
    aws_conn_id=AWS_CREDENTIALS,
    timeout=31,
    poke_interval=30,
    soft_fail=True,
)

create_procon_stage_table = PostgresOperator(
    task_id='create_procon_stage_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        procon_queries['drop_stage_table'],
        procon_queries['create_stage_table']
    ]
)

load_procon_stage_data = S3ToRedshiftCustomOperator(
    task_id='load_procon_stage_data',
    dag=dag,
    aws_conn_id=AWS_CREDENTIALS,
    redshift_conn_id=REDSHIFT_CONN,
    schema='staging',
    table=STAGING_TABLE,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
    copy_options=[
        "DELIMITER AS ';'",
        "DATEFORMAT AS 'DD/MM/YYYY'",
        "IGNOREHEADER AS 1",
        "EMPTYASNULL",
        "NULL AS 'NULL'"
    ]
)

delete_s3_key_files = SubDagOperator(
    task_id='delete_s3_key_files',
    dag=dag,
    subdag=delete_s3_key_files_subdag(
        DAG_NAME, 'delete_s3_key_files', start_date, S3_BUCKET, S3_KEY, AWS_CREDENTIALS)
)

load_dm_date_data = PostgresOperator(
    task_id='load_dm_date_data',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        dimensions_queries['create_dm_date'],
        procon_queries['insert_dm_date']
    ]
)

load_dm_region_data = PostgresOperator(
    task_id='load_dm_region_data',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        dimensions_queries['create_dm_region'],
        procon_queries['insert_dm_region']
    ]
)

load_dm_consumer_data = PostgresOperator(
    task_id='load_dm_consumer_data',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        dimensions_queries['create_dm_consumer_profile'],
        procon_queries['insert_dm_consumer_profile']
    ]
)

load_dm_company_data = PostgresOperator(
    task_id='load_dm_company_data',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        dimensions_queries['create_dm_company'],
        procon_queries['insert_dm_company']
    ]
)

load_ft_complaints_data = PostgresOperator(
    task_id='load_ft_complaints_data',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=[
        fact_queries['create_ft_complaints'],
        procon_queries['insert_ft_complaints']
    ]
)

subdag_task_id = 'data_quality_check'
data_quality_check = SubDagOperator(
    task_id=subdag_task_id,
    dag=dag,
    subdag=data_quality_check_subdag(DAG_NAME, subdag_task_id, start_date, REDSHIFT_CONN, STAGING_TABLE)
)

drop_procon_stage_table = PostgresOperator(
    task_id='drop_procon_stage_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN,
    sql=procon_queries['drop_stage_table']
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    has_file_to_process,
    create_procon_stage_table,
    load_procon_stage_data,
    [load_dm_date_data, load_dm_region_data, load_dm_consumer_data, load_dm_company_data],
    load_ft_complaints_data,
    data_quality_check,
    drop_procon_stage_table,
    end_operator
)

delete_s3_key_files.set_upstream(load_procon_stage_data)
