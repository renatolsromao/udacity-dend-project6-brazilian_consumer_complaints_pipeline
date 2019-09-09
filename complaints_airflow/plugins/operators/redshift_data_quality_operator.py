from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftDataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 rules,
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.rules = rules

        super(RedshiftDataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for rule in self.rules:
            query = rule['query']
            op = rule['op']

            records = redshift_conn.get_records(query)
            self.log.info(records)
            if not op(records[0][0]):
                raise ValueError("Data quality check failed.")

