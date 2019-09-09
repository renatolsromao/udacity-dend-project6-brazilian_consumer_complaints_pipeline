import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

from helpers.consumidorgovbr_queries import consumidorgovbr_queries
from helpers.procon_queries import procon_queries
from helpers.fact_queries import fact_queries
from helpers.dimensions_queries import dimensions_queries
from helpers.cep_queries import cep_queries

DAG_NAME = 'cleanup_dag'
start_date = datetime.datetime(2019, 9, 1)

dag = DAG(
    DAG_NAME,
    description='Remove all Redshift tables for consumer complints.',
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

drop_stage_tables = PostgresOperator(
    task_id='drop_stage_tables',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        procon_queries['drop_stage_table'],
        consumidorgovbr_queries['drop_stage_table'],
        cep_queries['drop_staging_cep'],
        cep_queries['drop_staging_cities'],
        cep_queries['drop_staging_states'],
    ]
)

drop_fact_table = PostgresOperator(
    task_id='drop_fact_table',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=fact_queries['drop_ft_complaints']
)

drop_dimension_tables = PostgresOperator(
    task_id='drop_dimension_tables',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=[
        dimensions_queries['drop_dm_date_table'],
        dimensions_queries['drop_dm_region_table'],
        dimensions_queries['drop_dm_consumer_profile_table'],
        dimensions_queries['drop_dm_company_table'],
    ]
)

end_operator = DummyOperator(task_id='finish_execution', dag=dag)

chain(
    start_operator,
    drop_stage_tables,
    drop_fact_table,
    drop_dimension_tables,
    end_operator
)
