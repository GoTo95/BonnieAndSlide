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


def get_events(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/events'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_updated_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
    params = {
        'with': 'lead_name',
        'page': 1,
        "filter[entity]": 'lead',
        "filter[created_at][from]": int(last_updated_at.timestamp()),
        "filter[type]": 'lead_status_changed',
        "limit": 250,
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['events'])
            if len(chunk) > 0:
                chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)

                chunk = chunk.replace({np.nan: None})
                df = df.append(chunk)
            params['page'] += 1
        else:
            break

    if len(df) > 0:
        df['lead_status_id_before'] = df['value_before'].str[0].str['lead_status'].str['id'].astype(int)
        df['lead_pipeline_id_before'] = df['value_before'].str[0].str['lead_status'].str['pipeline_id'].astype(int)
        df['lead_status_id_after'] = df['value_after'].str[0].str['lead_status'].str['id'].astype(int)
        df['lead_pipeline_id_after'] = df['value_after'].str[0].str['lead_status'].str['pipeline_id'].astype(int)

        needed_columns = ['id', 'entity_id', 'created_at', 'lead_status_id_before',
                          'lead_pipeline_id_before', 'lead_status_id_after', 'lead_pipeline_id_after']

        df = df[needed_columns]
        df = df.rename(columns={'entity_id': 'lead_id'})

    return df