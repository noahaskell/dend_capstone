from __future__ import absolute_import

from airflow.plugins_manager import AirflowPlugin

from operators.process_sas import ProcessSasOperator
# import helpers


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        ProcessSasOperator
    ]
    # helpers = [
    #    helpers.SqlQueries
    # ]
