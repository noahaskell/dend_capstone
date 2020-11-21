# finally got this working by using this approach:
# https://stackoverflow.com/a/57583400/3297752

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
