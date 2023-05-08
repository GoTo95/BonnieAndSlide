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


def _get_calls(date_from, token, call_type):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/contacts/notes'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_created_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
    params = {
        'with': 'lead_name',
        'page': 1,
        "filter[created_at][from]": int(last_created_at.timestamp()),
        "filter[note_type]": call_type,
        "limit": 250,
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['notes'])
            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            chunk['updated_at'] = chunk['updated_at'].apply(datetime.fromtimestamp)
            chunk = chunk.replace({np.nan: None})
            df = df.append(chunk)
            page = r.get('_page')

            params['page'] += 1
        else:
            print(f'http status code is {status}')
            break

    df['duration'] = df['params'].apply(lambda x: x['duration'])
    df['call_result'] = df['params'].apply(lambda x: x['call_result'])
    df['call_status'] = df['params'].apply(lambda x: x['call_status'])

    needed_columns = ['id', 'entity_id', 'created_at', 'updated_at', 'responsible_user_id', 'call_status',
                      'call_result', 'duration']

    df = df[needed_columns]
    df = df.rename(columns={'entity_id': 'contact_id'})
    df['type'] = call_type

    return df


def get_all_calls(date_from, token):
    df_out = _get_calls(date_from, token, 'call_out')
    df_in = _get_calls(date_from, token, 'call_in')
    df = pd.concat([df_out, df_in])
    return df
