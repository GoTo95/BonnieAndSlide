import requests
import pandas as pd
import numpy as np
from datetime import datetime
from urllib import parse
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


def to_int(row):
    if str(row).isdigit():
        return int(row)
    else:
        return -1


def get_amo_payments(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/catalogs/9719/elements'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_updated_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')

    params = {
        "filter[updated_at][from]": int(last_updated_at.timestamp()),
        "limit": 250,
    }

    df = pd.DataFrame()
    next_url = _url
    iteration = 0
    while next_url:
        iteration += 1
        resp = requests.get(next_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['elements'])  # все данные

            chunk['lead_id'] = chunk['custom_fields_values'].apply(
                lambda row, field_name='Id сделки': get_field_val(row, field_name)
            )

            chunk['price'] = chunk['custom_fields_values'].apply(
                lambda row, field_name='Сумма': get_field_val(row, field_name)
            )

            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            chunk['updated_at'] = chunk['updated_at'].apply(datetime.fromtimestamp)

            chunk = chunk.replace({np.nan: None})

            df = df.append(chunk)
            next_url = r['_links'].get('next', {}).get('href')
        else:
            break
    if len(df) > 0:
        needed_columns = ['id', 'created_at', 'updated_at', 'lead_id', 'price', 'is_deleted']

        df = df[needed_columns]

        df['id'] = df['id'].apply(to_int)
        df['lead_id'] = df['lead_id'].apply(to_int)
        df['price'] = df['price'].apply(to_int)

        df[['id', 'lead_id', 'price']] = \
            df[['id', 'lead_id', 'price']].replace('', -1).fillna(
                -1).astype(int)

        df[['is_deleted']] = df[['is_deleted']].replace('', False).fillna(False).astype(bool)

    return df