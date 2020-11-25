from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import cross_downstream
from airflow.operators.capstone_plugin import (ProcessSasOperator,
                                               StageToRedshiftOperator,
                                               LoadFactOperator,
                                               LoadDimensionOperator,
                                               DataQualityOperator)
from helpers import sql_statements as sql
from helpers.functions import sas_to_csv

default_args = {
    'owner': 'nhs',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 21),
    'retries': 1,
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
    task_id='read_sas_write_csv',
    dag=dag,
    aws_credentials_id='aws_credentials',
    s3_bucket='nhs-dend-capstone',
    s3_read_key='sas-data',
    s3_write_key='csv-data',
    rtw_func=sas_to_csv
)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=sql.create_tables
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

stage_state_name_task = StageToRedshiftOperator(
    task_id="stage_state_names",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="state_names",
    s3_bucket="nhs-dend-capstone",
    s3_key="supp/state_names_codes.csv"
)

load_fact_task = LoadFactOperator(
    task_id="load_immigration_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=sql.immigration_insert,
    table="immigration_fact",
    append_data=False
)

load_country_dim_task = LoadDimensionOperator(
    task_id="load_country_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=sql.country_insert,
    table="country_dim"
)

load_state_dim_task = LoadDimensionOperator(
    task_id="load_state_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql=sql.state_insert,
    table="state_dim"
)

quality_checks_task = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': sql.dq_sql.format('state', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('year', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('month', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('day', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('cit_country', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('res_country', 'immigration_fact'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('country', 'country_dim'),
         'expected': 0},
        {'check_sql': sql.dq_sql.format('state', 'state_dim'),
         'expected': 0},
    ]
)

stage_list = [stage_events_task, stage_growth_task, stage_area_task,
              stage_pop_task, stage_eco_task, stage_lang_task,
              stage_happiness_task, stage_iso_task, stage_remit_task,
              stage_gdp_task, stage_income_task, stage_state_name_task]

load_list = [load_fact_task, load_country_dim_task, load_state_dim_task]

start_operator >> process_sas_task
process_sas_task >> create_tables_task
create_tables_task >> stage_list
cross_downstream(stage_list, load_list)
load_list >> quality_checks_task
