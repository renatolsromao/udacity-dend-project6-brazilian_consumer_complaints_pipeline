from airflow import DAG
from airflow.utils.helpers import chain

from airflow.operators import (RedshiftDataQualityOperator, RedshiftCompareResultsOperator)
from helpers.generic_queries import generic_queries


def data_quality_check_subdag(parent_dag_name, child_dag_name, start_date, redshift_conn, staging_table):
    dag = DAG(
        f'{parent_dag_name}.{child_dag_name}',
        description='Check if dimensions tables attend data quality principles.',
        start_date=start_date,
        schedule_interval=None,
        catchup=False,
    )

    dimensions_data_quality_check = RedshiftDataQualityOperator(
        task_id='dimensions_data_quality_check',
        dag=dag,
        redshift_conn_id=redshift_conn,
        rules=[
            {'query': generic_queries['table_size'].format('dm_company'), 'op': lambda x: x > 0},
            {'query': generic_queries['table_size'].format('dm_region'), 'op': lambda x: x > 0},
            {'query': generic_queries['table_size'].format('dm_consumer_profile'), 'op': lambda x: x > 0},
            {'query': generic_queries['table_size'].format('dm_date'), 'op': lambda x: x > 0},
        ],
    )

    fact_data_quality_check = RedshiftCompareResultsOperator(
        task_id='fact_data_quality_check',
        dag=dag,
        redshift_conn_id=redshift_conn,
        query=generic_queries['table_size'].format('ft_complaints'),
        comparison_query=generic_queries['table_size'].format(f'staging.{staging_table}'),
        operator=lambda x, y: x >= y,
    )

    chain(
        dimensions_data_quality_check,
        fact_data_quality_check,
    )

    return dag
