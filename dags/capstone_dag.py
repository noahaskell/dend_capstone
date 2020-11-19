from airflow import DAG
from airflow.operators import ProcessSas7bdatOperator


# add start_date, end_date?
default_args = {
    'owner': 'nhs',
    'depends_on_past': False
}

dag = DAG(
    'capstone_dag',
    description='Load and transform i94 data in Redshift with Airflow',
    schedule_interval='@daily'
)

process_sas_data = ProcessSas7bdatOperator(
    task_id='sas_to_csv',
    dag=dag,
    s3_bucket='nhs-dend-capstone',
    s3_read_key='sas-data',
    s3_write_key='csv-data'
)

process_sas_data
