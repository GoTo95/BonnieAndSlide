import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from clickhouse_driver import Client


def get_token(client):
    access_token = client.execute("SELECT token FROM db1.tokens WHERE type = 'access_token'")[0][0]
    return access_token


def main(event, context, ):
    client = Client(host='rc1b-ckq1cemqvctvesog.mdb.yandexcloud.net',
                    settings={'use_numpy': True},
                    user='black_master',
                    password='vjsj338SSnbgv',
                    port=9440,
                    secure=True,
                    verify=True,
                    #                 ca_certs='/etc/ssl/certs/ca-certificates.crt')
                    ca_certs='ca-certificates.crt')

    check_query = '''
        select if(max(created_at) = CAST('1970-01-01 00:00:01', 'datetime'), CAST('2022-06-01 00:00:01', 'datetime'),MAX(created_at)) as max_date
        from db1.amo_calls
    '''

    date_from = str(client.execute(check_query)[0][0])
    token = get_token(client)
    df_out = get_calls(date_from, token, 'call_out')
    df_in = get_calls(date_from, token, 'call_in')

    df = pd.concat([df_out, df_in])

    step = 500
    if len(df) > 0:
        for i in range(0, len(df), step):
            delete_query = '''
                ALTER TABLE db1.amo_calls DELETE WHERE id IN ({})
            '''.format("'" + "','".join(map(str, df.iloc[i:i + step]['id'].values)) + "'")
            client.execute(delete_query)
            client.insert_dataframe('INSERT INTO db1.amo_calls VALUES', df.iloc[i:i + step])

    return {'Rows inserted': len(df)}