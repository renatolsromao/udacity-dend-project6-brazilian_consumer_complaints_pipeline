import datetime

from airflow.utils.helpers import chain

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftCustomOperator)

from helpers.dimensions_queries import dimensions_queries
from helpers.fact_queries import fact_queries
from helpers.procon_queries import procon_queries

s3_bucket = 'rlsr-dend'
s3_folder = 'consumer-complaints/procon'
s3_file = 'dadosabertosatendimentofornecedor1trimestre2017'

dag = DAG(
    'procon_dag',
    description='Load data from Procon complaints from S3 to Redshift.',
    start_date=datetime.datetime(2019, 8, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

create_procon_stage_table = PostgresOperator(
    task_id='create_procon_stage_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        procon_queries['drop_stage_table'],
        procon_queries['create_stage_table']
    ]
)

load_procon_stage_data = S3ToRedshiftCustomOperator(
    task_id='load_procon_stage_data',
    dag=dag,
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift_conn',
    schema='staging',
    table='procon',
    s3_bucket=s3_bucket,
    s3_key=f'{s3_folder}/{s3_file}',
    copy_options=[
        "DELIMITER AS ';'",
        "DATEFORMAT AS 'DD/MM/YYYY'",
        "IGNOREHEADER AS 1",
        "EMPTYASNULL",
        "NULL AS 'NULL'"
    ]
)

load_dm_date_data = PostgresOperator(
    task_id='load_dm_date_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_date'],
        procon_queries['insert_dm_date']
    ]
)

load_dm_region_data = PostgresOperator(
    task_id='load_dm_region_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_region'],
        procon_queries['insert_dm_region']
    ]
)

load_dm_consumer_data = PostgresOperator(
    task_id='load_dm_consumer_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_consumer_profile'],
        procon_queries['insert_dm_consumer_profile']
    ]
)

load_dm_company_data = PostgresOperator(
    task_id='load_dm_company_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['create_dm_company'],
        procon_queries['insert_dm_company']
    ]
)

load_ft_complaints_data = PostgresOperator(
    task_id='load_ft_complaints_data',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        fact_queries['create_ft_complaints'],
        procon_queries['insert_ft_complaints']
    ]
)

drop_procon_stage_table = PostgresOperator(
    task_id='drop_procon_stage_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=procon_queries['drop_stage_table']
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    create_procon_stage_table,
    load_procon_stage_data,
    [load_dm_date_data, load_dm_region_data, load_dm_consumer_data, load_dm_company_data],
    load_ft_complaints_data,
    drop_procon_stage_table,
    end_operator
)
