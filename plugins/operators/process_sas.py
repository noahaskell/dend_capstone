from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from io import BytesIO, StringIO
import pandas as pd
import datetime


class ProcessSasOperator(BaseOperator):
    # ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_read_key="",
                 s3_write_key="",
                 *args, **kwargs):

        super(ProcessSasOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_read_key = s3_read_key
        self.s3_write_key = s3_write_key
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        def sas_to_date(sasnum, val=None):
            dow_dict = {1: 'Mon', 2: 'Tue', 3: 'Wed',
                        4: 'Thu', 5: 'Fri', 6: 'Sat',
                        7: 'Sun'}
            reference = datetime.datetime(1960, 1, 1)
            diff = datetime.timedelta(days=sasnum)
            date = reference + diff
            if val in ['year', 'month', 'day']:
                return getattr(date, val)
            elif val == 'weekday':
                return dow_dict[date.isoweekday()]
            else:
                return str(date).split(' ')[0]

        s3_hook = S3Hook(self.aws_credentials_id)

        # state abbreviations for mapping, e.g., b'TX' to 'TX'
        self.log.info("reading state data")
        states_obj = s3_hook.get_key("states.csv", bucket_name=self.s3_bucket)
        states = pd.read_csv(states_obj.get()['Body'])
        state_dict = {bytes(x, 'utf-8'): x for x in states['state']}

        # for mapping i94 country codes to country names
        self.log.info("reading country data")
        citres_obj = s3_hook.get_key("i94cit_i94res.csv",
                                     bucket_name=self.s3_bucket)
        ctry = pd.read_csv(citres_obj.get()['Body'])
        ctry_dict = {
            ctry.loc[i, 'code']: ctry.loc[i, 'country'] for i in ctry.index
        }

        # read in sas7bdat file
        self.log.info("reading sas data")
        sas_obj = s3_hook.get_key(self.s3_read_key, bucket_name=self.s3_bucket)
        df = pd.read_sas(BytesIO(sas_obj.get()['Body'].read()),
                         format='sas7bdat')

        self.log.info("processing sas data")
        # map states and countries
        df['state'] = df['i94addr'].map(state_dict)
        df['cit_country'] = df['i94cit'].map(ctry_dict)
        df['res_country'] = df['i94res'].map(ctry_dict)

        # rename i94bir as age
        df.rename(columns={'i94bir': 'age'}, inplace=True)

        # indicator variables for gender, visa type
        df['is_female'] = 1*(df['gender'] == b'F')
        df['visa_business'] = 1*(df['i94visa'] == 1)
        df['visa_pleasure'] = 1*(df['i94visa'] == 2)

        # duration of stay in days
        df['stay_dur'] = df['depdate'] - df['arrdate']

        # group by arrival date, state, country of citizenship & residence
        grp_cols = ['arrdate', 'state', 'cit_country', 'res_country']
        # aggregate: count gender, visa types, totals; get mean age, stay dur
        agg_dict = {'is_female': 'sum',
                    'visa_business': 'sum',
                    'visa_pleasure': 'sum',
                    'age': 'mean',
                    'stay_dur': 'mean',
                    'count': 'sum'}
        dg = df.groupby(grp_cols).agg(agg_dict).reset_index()

        # make year, month, day, weekday, and date (YYYY-MM-DD) fields
        new_cols = ['year', 'month', 'day', 'weekday']
        for c in new_cols:
            dg[c] = dg['arrdate'].apply(sas_to_date, args=(c,))
        dg['date'] = dg['arrdate'].apply(sas_to_date)
        dg.drop('arrdate', axis=1, inplace=True)

        self.log.info("df.shape = " + str(df.shape))
        self.log.info("dg.shape = " + str(dg.shape))
        self.log.info("dg.columns = " + str([x for x in dg.columns]))

        # boto3 related: ERROR - Fileobj must implement read
        # write dg as csv to s3
        self.log.info("writing transformed data as csv")
        csv_buf = StringIO()
        dg.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3_hook.load_string(
            csv_buf.getvalue(),
            key=self.s3_write_key,
            bucket_name=self.s3_bucket
        )
