# finally got this working by using this approach:
# https://stackoverflow.com/a/57583400/3297752

from airflow.plugins_manager import AirflowPlugin

from operators.process_sas import ProcessSasOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers import sql_statements

# from helpers.functions import sas_to_csv


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        ProcessSasOperator,
        StageToRedshiftOperator
    ]
    helpers = [
        sql_statements
    ]
