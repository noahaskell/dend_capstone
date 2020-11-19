from __future__ import absolute_import

from airflow.plugins_manager import AirflowPlugin

from operators.process_sas7bdat import ProcessSas7bdatOperator
# import helpers


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        ProcessSas7bdatOperator
    ]
    # helpers = [
    #    helpers.SqlQueries
    # ]
