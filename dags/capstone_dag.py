from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# https://stackoverflow.com/a/58640550/3297752
#  says drop the airflow prefix, but that doesn't seem to work
# leaving off the process_sas7bdat seems to work?
from airflow.operators.process_sas7bdat import ProcessSas7bdatOperator

# add start_date, end_date?
default_args = {
    'owner': 'nhs',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 26)
}

dag = DAG(
    'capstone_dag',
    default_args=default_args,
    description='Load and transform i94 data in Redshift with Airflow',
    schedule_interval='@daily'
)

start_operator = DummyOperator(
    task_id='start_execution',
    dag=dag
)

process_sas_data = ProcessSas7bdatOperator(
    task_id='sas_to_csv',
    dag=dag,
    s3_bucket='nhs-dend-capstone',
    s3_read_key='sas-data',
    s3_write_key='csv-data'
)

start_operator >> process_sas_data
