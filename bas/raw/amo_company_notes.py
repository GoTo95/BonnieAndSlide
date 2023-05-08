import requests
import pandas as pd
import numpy as np
from datetime import datetime

def get_company_notes(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/companies/notes'
    headers = {
        "Authorization": "Bearer " + token
    }

    date_from = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')

    params = {
        'page': 1,
        'filter[note_type]': 'amomail_message,call_in,call_out',
        "filter[updated_at][from]": int(date_from.timestamp()),
        "filter[updated_at][to]": int(datetime.now().timestamp()),
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

    needed_columns = ['id', 'entity_id', 'created_at', 'updated_at', 'responsible_user_id', 'note_type']

    df = df[needed_columns]
    df = df.rename(columns={'entity_id': 'company_id', 'responsible_user_id': 'created_by'})

    df = df.astype({'id': 'int32', 'company_id': 'int32', 'created_by': 'int32'})

    return df