import requests
import pandas as pd
import numpy as np
from datetime import datetime
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


def get_webinar(row, field_name):
    if type(row) == list:
        for field in row:
            if field['field_name'] == field_name:
                webinars = []
                for val in field['values']:
                    webinars.append(val['value'])
                return ';'.join(webinars)


def get_contacts(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/contacts'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_created_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
    params = {
        'page': 1,
        "filter[updated_at][from]": int(last_created_at.timestamp()),
        "limit": 250,
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['contacts'])
            chunk = chunk.replace({np.nan: None})
            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            df = df.append(chunk)
            page = r.get('_page')

            params['page'] += 1
        else:
            print(f'http status code is {status}')
            break

    for field in ('utm_campaign', 'utm_source', 'utm_medium', 'utm_term', 'utm_content'):
        df[field] = df['custom_fields_values'].apply(
            lambda row, field_name=field: get_field_val(row, field_name)
        )

    df['webinars'] = df['custom_fields_values'].apply(
        lambda row, field_name='Вебинар': get_webinar(row, field_name)
    )

    needed_columns = ['id', 'created_at', 'updated_at', 'responsible_user_id', 'utm_campaign', 'utm_source',
                      'utm_medium', 'utm_term', 'utm_content', 'webinars']
    df = df[needed_columns]

    return df
