import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from clickhouse_driver import Client
import warnings
warnings.filterwarnings('ignore')


def get_calls(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/contacts/notes'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_created_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
    params = {
        'with': 'lead_name',
        'page': 1,
        "filter[created_at][from]": int(last_created_at.timestamp()),
        "filter[note_type]": 'call_in,call_out',
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

    if 'params' in df.columns:
        df['duration'] = df['params'].apply(lambda x: x['duration'])
        df['call_result'] = df['params'].apply(lambda x: x['call_result'])
        df['call_status'] = df['params'].apply(lambda x: x['call_status'])

        needed_columns = ['id', 'entity_id', 'created_at', 'updated_at', 'responsible_user_id', 'call_status',
                          'call_result', 'duration', 'note_type']

        df = df[needed_columns]
        df = df.rename(columns={'entity_id': 'contact_id', 'note_type': 'type'})

        return df
    else:
        return None