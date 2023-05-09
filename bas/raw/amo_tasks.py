import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from clickhouse_driver import Client
import warnings
from urllib import parse

warnings.filterwarnings('ignore')


def get_token(client):
    access_token = client.execute("SELECT token FROM db1.tokens WHERE type = 'access_token'")[0][0]
    return access_token


def get_field_val(row, field_name):
    if type(row) == list:
        for field in row:
            if field['field_name'] == field_name:
                return field['values'][0]['value']


def loss_reason(value):
    loss_reason = value.get('loss_reason')
    if loss_reason:
        return loss_reason[0]['name']
    else:
        return None


def to_int(row):
    if str(row).isdigit():
        return int(row)
    else:
        return -1


def get_url_values(row, field_name):
    if type(row) == list:
        for field in row:
            if field['field_name'] == field_name:
                url_values = parse.parse_qs(parse.urlsplit(field['values'][0]['value']).query)
                for key in url_values.keys():
                    url_values[key] = url_values[key][0]
                return url_values


def get_tasks(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/tasks'
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
            chunk = pd.DataFrame(r['_embedded']['tasks'])

            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            chunk['updated_at'] = chunk['updated_at'].apply(datetime.fromtimestamp)

            chunk['complete_till'] = chunk[chunk['complete_till'].notna()]['complete_till'].apply(
                datetime.fromtimestamp)

            df = df.append(chunk)
            next_url = r['_links'].get('next', {}).get('href')
        else:
            break
    if len(df) > 0:
        needed_columns = ['id', 'created_at', 'updated_at', 'text', 'duration', 'complete_till', 'is_completed',
                          'responsible_user_id', 'entity_id', 'entity_type']

        df = df[needed_columns]

        for int_column in ['id', 'duration', 'entity_id', 'responsible_user_id']:
            df[int_column] = df[int_column].apply(to_int)

    return df
