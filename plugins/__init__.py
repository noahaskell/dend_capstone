from __future__ import absolute_import

from airflow.plugins_manager import AirflowPlugin

import operators
# import helpers


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.ProcessSas7bdatOperator
    ]
    # helpers = [
    #    helpers.SqlQueries
    # ]
