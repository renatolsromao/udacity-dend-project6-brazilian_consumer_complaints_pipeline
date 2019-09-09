from typing import Union, List

from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.utils.decorators import apply_defaults


class S3DeleteFromContextOperator(S3DeleteObjectsOperator):
    @apply_defaults
    def __init__(self,
                 context_task_id,
                 bucket,
                 aws_conn_id='aws_default',
                 verify=None,
                 *args, **kwargs):
        super(S3DeleteFromContextOperator, self).__init__(
            keys=[],
            bucket=bucket,
            aws_conn_id=aws_conn_id,
            verify=verify,
            *args, **kwargs)
        self.context_task_id = context_task_id

    def execute(self, context):
        self.keys = context['task_instance'].xcom_pull(task_ids=self.context_task_id)
        super(S3DeleteFromContextOperator, self).execute(context)
