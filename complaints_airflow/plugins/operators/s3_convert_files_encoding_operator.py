import sys
from tempfile import NamedTemporaryFile
import subprocess

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ConvertFilesEncodingOperator(BaseOperator):
    """
    Load files that match an S3 prefix into temporary files and apply a bash inconv transformation from the
    original encoding to destination encoding, then replace the file on S3.

    :param aws_conn_id: s3 connection
    :type aws_conn_id: str
    :param s3_bucket: The S3 bucket
    :type s3_bucket: str
    :param s3_prefix: The S3 prefix to match files from, maybe a folder or a single file
    :type s3_prefix: str
    :param s3_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :type s3_verify: bool or str
    :param original_encoding: The encoding of the file to be converted
    :type original_encoding: str
    :param dest_encoding: The encoding to obtain after transform
    :type dest_encoding: str
    """

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_prefix,
            s3_verify=None,
            aws_conn_id='aws_default',
            original_encoding=None,
            dest_encoding=None,
            *args, **kwargs
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_verify = s3_verify
        self.aws_conn_id = aws_conn_id
        self.original_encoding = original_encoding
        self.dest_encoding = dest_encoding
        self.output_encoding = sys.getdefaultencoding()
        super(S3ConvertFilesEncodingOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        source_s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.s3_verify)

        for file_name in source_s3.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix):
            if file_name != self.s3_prefix and file_name != f'{self.s3_prefix}/':
                self.log.info(f'Processing file {file_name}')
                self.convert_file(source_s3, file_name)

    def convert_file(self, source_s3, file_name):
        self.log.info(f"Downloading S3 file {file_name}")
        if not source_s3.check_for_key(file_name, bucket_name=self.s3_bucket):
            raise AirflowException(f"The key {file_name} does not exist")
        source_s3_key_object = source_s3.get_key(file_name, bucket_name=self.s3_bucket)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info(f"Dumping S3 file {file_name} contents to local file {f_source.name}")
            source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            self.log.info("Executing transform bash command.")
            iconv = f'iconv -f {self.original_encoding} -t {self.dest_encoding} {f_source.name}'
            process = subprocess.Popen(iconv.split(), stdout=f_dest, close_fds=True)
            process.wait()

            if process.returncode > 0:
                raise AirflowException(f"Transform failed: {process.returncode}")
            else:
                self.log.info(f"Transform successful. Output temporarily located at {f_dest.name}")

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            source_s3.load_file(filename=f_dest.name, key=file_name, bucket_name=self.s3_bucket, replace=True)

            self.log.info("Upload successful")
