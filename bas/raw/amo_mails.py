import requests
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


def get_token(client):
    access_token = client.execute("SELECT token FROM db1.tokens WHERE type = 'access_token'")[0][0]
    return access_token


def get_mails(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/companies/notes'
    headers = {
        "Authorization": "Bearer " + token
    }

    date_from = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')

    params = {
        'page': 1,
        'filter[note_type]': 'amomail_message',
        "filter[updated_at][from]": int(date_from.timestamp()),
        "limit": 250
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

    needed_columns = ['id', 'entity_id', 'created_at', 'updated_at', 'responsible_user_id']

    df = df[needed_columns]
    df = df.rename(columns={'entity_id': 'company_id', 'responsible_user_id': 'created_by'})

    return df
