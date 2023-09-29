import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from clickhouse_driver import Client
import warnings

warnings.filterwarnings('ignore')

last_updated_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')

def get_leads(row):
    return [el['id'] for el in row['leads']]


def get_contacts(row):
    return [el['id'] for el in row['contacts']]


def get_companies(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/companies'
    headers = {
        "Authorization": "Bearer " + token
    }

    params = {
        'page': 1,
        "limit": 250,
        'with': 'leads,contacts',
        "filter[updated_at][from]": int(last_updated_at.timestamp())
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['companies'])
            chunk = chunk.replace({np.nan: None})
            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            chunk['updated_at'] = chunk['updated_at'].apply(datetime.fromtimestamp)
            df = df.append(chunk)

            params['page'] += 1
        else:
            break

    df['lead_id'] = df['_embedded'].apply(
        lambda row: get_leads(row)
    )

    df['contact_id'] = df['_embedded'].apply(
        lambda row: get_contacts(row)
    )

    needed_columns = ['id', 'created_at', 'updated_at', 'responsible_user_id', 'name', 'lead_id', 'contact_id']
    df = df[needed_columns]

    df = df.explode('lead_id')
    df = df.explode('contact_id')

    return df