import pandas as pd
import datetime
from io import BytesIO, StringIO


def sas_to_csv(s3_hook, s3_bucket, s3_read_key,
               s3_write_key, logger):
    """
    Reads sas7bdat file from s3, transforms data, writes as csv to s3

    Parameters
    ----------
    s3_hook : S3Hook instance
        Airflow hook for accessing S3
    s3_bucket : str
        bucket name
    s3_read_key : str
        key for sas7bdat file to read
    s3_write_key : str
        key for csv file to write
    logger : operator logger
        for logging operations
    """
    logger.info("reading state data")
    states_obj = s3_hook.get_key("supp/state_names_codes.csv",
                                 bucket_name=s3_bucket)
    states = pd.read_csv(states_obj.get()['Body'])
    state_dict = {bytes(x, 'utf-8'): x for x in states['code']}

    logger.info("reading country data")
    ctry_obj = s3_hook.get_key("supp/i94cit_i94res.csv",
                               bucket_name=s3_bucket)
    ctry = pd.read_csv(ctry_obj.get()['Body'])
    ctry_dict = {
        ctry.loc[i, 'code']: ctry.loc[i, 'country'] for i in ctry.index
    }

    logger.info("reading sas data : " + s3_read_key)
    sas_obj = s3_hook.get_key(s3_read_key, bucket_name=s3_bucket)
    df = pd.read_sas(BytesIO(sas_obj.get()['Body'].read()),
                     format='sas7bdat')

    logger.info("processing sas data")
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

    logger.info("writing transformed data as csv")
    csv_buf = StringIO()
    dg.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_hook.load_string(
        csv_buf.getvalue(),
        key=s3_write_key,
        bucket_name=s3_bucket,
        replace=True
    )


def sas_to_date(sasnum, val=None):
    "Converts SAS date number to year, month, day, or weekday"
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
