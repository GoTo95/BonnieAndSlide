import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from clickhouse_driver import Client
import warnings

warnings.filterwarnings('ignore')


def get_token(client):
    access_token = client.execute("SELECT token FROM db1.tokens WHERE type = 'access_token'")[0][0]
    return access_token


def get_field_val(row, field_name):
    if type(row) == list:
        for field in row:
            if field['field_name'] == field_name:
                return field['values'][0]['value']


def get_users(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/users'
    headers = {
        "Authorization": "Bearer " + token
    }

    params = {
        'page': 1,
        "limit": 250,
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['users'])
            chunk = chunk.replace({np.nan: None})
            df = df.append(chunk)
            page = r.get('_page')

            params['page'] += 1
        else:
            break

    needed_columns = ['id', 'name', 'email']
    df = df[needed_columns]
    df['_insert_date'] = datetime.now()
    return df
