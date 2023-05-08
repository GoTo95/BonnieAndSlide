import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os
from clickhouse_driver import Client
import warnings
from urllib import parse

warnings.filterwarnings('ignore')


def get_token(client):
    access_token = client.execute("SELECT token FROM db1.tokens WHERE type = 'access_token'")[0][0]
    return access_token


def get_last_date(client, table, field_name, last_date='2022-01-01 00:00:01'):
    check_query = f'''
        select if(max({field_name}) = CAST('1970-01-01 00:00:01', 'datetime'), CAST('2022-01-01 00:00:01', 'datetime'),MAX({field_name})) as max_date
        from {table}
    '''
    print(check_query)
    return str(client.execute(check_query)[0][0])


def load_data(get_data_func, table_name, update_field_date, last_date):
    local_cert = 'ca-certificates.crt'
    cloud_cert = '/etc/ssl/certs/ca-certificates.crt'
    cert_file_location = local_cert if os.path.exists(local_cert) else cloud_cert

    client = Client(host='rc1b-ckq1cemqvctvesog.mdb.yandexcloud.net',
                    settings={'use_numpy': True},
                    user='black_master',
                    password='vjsj338SSnbgv',
                    port=9440,
                    secure=True,
                    verify=True,
                    ca_certs=cert_file_location)

    date_from = get_last_date(client, table_name, update_field_date, last_date)
    token = get_token(client)
    df = get_data_func(date_from, token)
    step = 500
    if len(df) > 0:
        for i in range(0, len(df), step):
            delete_query = '''
                ALTER TABLE {} DELETE WHERE id IN ({})
            '''.format(table_name, ','.join(map(str, df.iloc[i:i + step]['id'].values)))
            client.execute(delete_query)
            client.insert_dataframe(f'INSERT INTO {table_name} VALUES', df.iloc[i:i + step])

    return {'Rows inserted': len(df)}