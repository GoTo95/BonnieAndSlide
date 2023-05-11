import requests
import pandas as pd
import numpy as np
from datetime import datetime


def get_chat_messages(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/events'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_updated_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
    params = {
        'page': 1,
        "filter[created_at][from]": int(last_updated_at.timestamp()),
        "filter[created_at][to]": int(datetime.now().timestamp()),
        "filter[type]": 'incoming_sms,outgoing_sms,incoming_chat_message,outgoing_chat_message',
        "limit": 100,
    }

    df = pd.DataFrame()

    while True:
        resp = requests.get(_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['events'])
            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)

            chunk = chunk.replace({np.nan: None})
            df = df.append(chunk)

            params['page'] += 1
            if params['page'] % 100 == 0:
                print(params['page'], chunk['created_at'].min())
        else:
            print(f'http status code is {status}')
            break

    needed_columns = ['id', 'entity_id', 'created_at', 'created_by', 'entity_type', 'type']

    df = df[needed_columns]

    return df
