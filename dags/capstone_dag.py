from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

# https://stackoverflow.com/a/58640550/3297752
#  says drop the airflow prefix, but that doesn't seem to work
# leaving off the process_sas seems to work?
from airflow.operators.capstone_plugin import (ProcessSasOperator,
                                               StageToRedshiftOperator)
# from airflow.helpers.functions import sas_to_csv
from helpers import sql_statements

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
    # dag=dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='nhs-dend-capstone',
    s3_read_key='sas-data',
    s3_write_key='csv-data'
    # rtw_func=sas_to_csv
)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=sql_statements.create_tables
)

stage_events_task = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staged_events",
    s3_bucket="nhs-dend-capstone",
    s3_key="csv-data"
)

stage_growth_task = StageToRedshiftOperator(
    task_id="stage_growth",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_growth",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/census_bureau_birth_death_growth_2016.csv"
)

stage_area_task = StageToRedshiftOperator(
    task_id="stage_area",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_area",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/census_bureau_country_area.csv"
)

stage_pop_task = StageToRedshiftOperator(
    task_id="stage_pop",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_pop",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/census_bureau_population_sex_2016.csv"
)

stage_eco_task = StageToRedshiftOperator(
    task_id="stage_eco",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_eco",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/countries_eco_footprint.csv"
)

stage_lang_task = StageToRedshiftOperator(
    task_id="stage_lang",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_lang",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/country_language.csv"
)

stage_happiness_task = StageToRedshiftOperator(
    task_id="stage_happiness",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_happiness",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/happiness_2016"
)

stage_iso_task = StageToRedshiftOperator(
    task_id="stage_iso",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="iso_country_codes",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/iso_country_codes.csv"
)

stage_remit_task = StageToRedshiftOperator(
    task_id="stage_remittance",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="remittance",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/remittance_outflow_2016.csv"
)

stage_gdp_task = StageToRedshiftOperator(
    task_id="stage_gdp",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="state_gdp",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/state_gdp_by_quarter_industry_2016.csv"
)

stage_income_task = StageToRedshiftOperator(
    task_id="stage_income",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="state_income",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/state_income_2016.csv"
)

# start_operator >> process_sas_task
start_operator >> create_tables_task
create_tables_task >> [stage_events_task, stage_growth_task, stage_area_task,
                       stage_pop_task, stage_eco_task, stage_lang_task,
                       stage_happiness_task, stage_iso_task,
                       stage_remit_task, stage_gdp_task, stage_income_task]
