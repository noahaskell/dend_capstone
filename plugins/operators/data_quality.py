import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#AF09AA'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        null_flags = []
        for check in self.dq_checks:
            sql = check['check_sql']
            expected = check['expected']
            sql_list = sql.split(' ')
            table = sql_list[-1]
            c_idx = sql_list.index('WHEN') + 1
            col = sql_list[c_idx]
            num_null = redshift_hook.get_records(sql)[0][0]
            if num_null > expected:
                null_flags.append(True)
                logging.info(f"Data quality check failed: \
                               {table}:{col} has {num_null} NULLs")
        if any(null_flags):
            raise ValueError("Data quality check failed. \
                              See log info for details.")
        else:
            logging.info("Data quality check passed.")
