from typing import Union, List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftCustomOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift
    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:
        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            s3_bucket: str,
            s3_key: str,
            table: str,
            schema: str = None,
            redshift_conn_id: str = 'redshift_default',
            aws_conn_id: str = 'aws_default',
            verify: Union[bool, str] = None,
            copy_options: List = None,
            autocommit: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.copy_options = copy_options or []
        self.autocommit = autocommit

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = s3.get_credentials()
        copy_options = '\n\t\t\t'.join(self.copy_options)
        table = f'{self.schema}.{self.table}' if self.schema is not None else self.table

        copy_query = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(table=table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")