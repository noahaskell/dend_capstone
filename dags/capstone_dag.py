from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# https://stackoverflow.com/a/58640550/3297752
#  says drop the airflow prefix, but that doesn't seem to work
# leaving off the process_sas seems to work?
from airflow.operators.capstone_plugin import ProcessSasOperator
# from airflow.helpers.functions import sas_to_csv

# add start_date, end_date?
default_args = {
    'owner': 'nhs',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 21),
    'retries': 0,
    'retry_delay': timedelta(seconds=15)
}

dag = DAG(
    'capstone_dag',
    default_args=default_args,
    description='Load and transform i94 data in Redshift with Airflow',
    schedule_interval='@once'
)

start_operator = DummyOperator(
    task_id='start_dag',
    dag=dag
)

process_sas_task = ProcessSasOperator(
    task_id='sas_to_csv',
    dag=dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='nhs-dend-capstone',
    s3_read_key='sas-data',
    s3_write_key='csv-data'
    # rtw_func=sas_to_csv
)

start_operator >> process_sas_task
