from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"

    operators = [
        operators.S3ToRedshiftCustomOperator,
    ]
