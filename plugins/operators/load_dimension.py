from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#71BCD0'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f"Clearing data from {self.table}")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f"Loading data into dimension table {self.table}")

        formatted_sql = f"INSERT INTO {self.table} " + self.sql

        self.log.info("sql: " + formatted_sql)
        redshift.run(formatted_sql)
