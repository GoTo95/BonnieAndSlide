import requests
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


def get_pipelines(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/leads/pipelines'
    headers = {
        "Authorization": "Bearer " + token
    }

    params = {
        'page': 1,
        "limit": 250,
    }

    df = pd.DataFrame()

    resp = requests.get(_url, params=params, headers=headers)
    status = resp.status_code
    if status == 200:
        r = resp.json()
        chunk = pd.DataFrame(r['_embedded']['pipelines'])
        chunk = chunk.replace({np.nan: None})
        df = df.append(chunk)
    else:
        print(f'http status code is {status}')

    df['statuses_id'] = df['_embedded'].apply(lambda row: [r['id'] for r in row['statuses']])
    df['statuses_name'] = df['_embedded'].apply(lambda row: [str(r['name']) for r in row['statuses']])
    df['statuses_sort'] = df['_embedded'].apply(lambda row: [r['sort'] for r in row['statuses']])
    return df