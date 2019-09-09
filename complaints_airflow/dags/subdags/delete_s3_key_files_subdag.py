from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.operators import (S3DeleteFromContextOperator)


def delete_s3_key_files_subdag(parent_dag_name, child_dag_name, start_date, s3_bucket, s3_key, aws_credentials):
    dag = DAG(
        f'{parent_dag_name}.{child_dag_name}',
        description='Delete all S3 files in the provided key.',
        start_date=start_date,
        schedule_interval=None,
        catchup=False,
    )

    list_s3_processed_s3_files = S3ListOperator(
        task_id='list_s3_processed_s3_files',
        dag=dag,
        bucket=s3_bucket,
        prefix=s3_key,
        aws_conn_id=aws_credentials,
    )

    delete_processed_s3_files = S3DeleteFromContextOperator(
        task_id='delete_processed_s3_files',
        dag=dag,
        bucket=s3_bucket,
        context_task_id='list_s3_processed_s3_files',
        aws_conn_id=aws_credentials,
    )

    chain(
        list_s3_processed_s3_files,
        delete_processed_s3_files
    )

    return dag
