import requests
import pandas as pd
import time
from clickhouse_driver import Client


def get_orders(date_from, token):
    key = 'Jm8OSMY9ju2UtnBW6CHHYvRu8CdGNo4cgmBtNO2skBjyMDaSgF5WoXwlvERhjmms37d1UZ9YY3U7vhmnHvdrbyRXsx7KSxefL6AfNYIHguw4HXppIsfMcrYqvpW0p6gt'
    r_text = 'https://bonnieandslide.getcourse.ru/pl/api/account/deals?key={}&created_at[from]={}'.format(key,
                                                                                                          date_from)
    res = requests.get(r_text).json()

    while res.get('error_code') == 905:
        time.sleep(30)
        res = requests.get(r_text).json()

    export_id = res['info']['export_id']

    time.sleep(30)

    r_text = 'https://bonnieandslide.getcourse.ru/pl/api/account/exports/{}?key={}'.format(export_id, key)
    result = requests.get(r_text).json()
    # Жду, пока подготовится выгрузка и забираю ее
    while result.get('error_code') == 909:
        time.sleep(30)
        result = requests.get(r_text).json()

    df = pd.DataFrame.from_dict(result['info']['items'])
    df = df.iloc[:, [0, 1, 2, 6, 53]]
    df.columns = ['id', 'number', 'user_id', 'created_at', 'tag']
    df['tag'] = df['tag'].apply(lambda x: str(x))

    df[['id', 'number', 'user_id']] = df[['id', 'number', 'user_id']].replace('', -1).astype(int)
    return df
