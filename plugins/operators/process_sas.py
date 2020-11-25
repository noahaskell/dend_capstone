from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ProcessSasOperator(BaseOperator):

    ui_color = '#A50E04'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_read_key="",
                 s3_write_key="",
                 rtw_func="",
                 *args, **kwargs):

        super(ProcessSasOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_read_key = s3_read_key
        self.s3_write_key = s3_write_key
        self.aws_credentials_id = aws_credentials_id
        self.rtw_func = rtw_func

    def execute(self, context):

        s3_hook = S3Hook(self.aws_credentials_id)

        key_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket,
            prefix=self.s3_read_key
        )

        for key in key_list:
            read_key = key
            filename = key.split('/')[-1].split('.')[0]
            write_key = 'csv-data/' + filename + '.csv'
            self.log.info("processing " + key)
            self.rtw_func(s3_hook, self.s3_bucket,
                          read_key, write_key, self.log)
