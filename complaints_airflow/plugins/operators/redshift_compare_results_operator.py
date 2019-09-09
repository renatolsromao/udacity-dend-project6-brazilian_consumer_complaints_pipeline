from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftCompareResultsOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 query,
                 comparison_query,
                 operator: staticmethod,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.comparison_query = comparison_query
        self.operator = operator

        super(RedshiftCompareResultsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        result = redshift_conn.get_records(self.query)
        comparison_result = redshift_conn.get_records(self.query)

        if not self.operator(result[0][0], comparison_result[0][0]):
            raise ValueError("Query results diverge.")

