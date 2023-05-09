import requests
import pandas as pd
import time


def get_payments(date_from, token):
    key = 'Jm8OSMY9ju2UtnBW6CHHYvRu8CdGNo4cgmBtNO2skBjyMDaSgF5WoXwlvERhjmms37d1UZ9YY3U7vhmnHvdrbyRXsx7KSxefL6AfNYIHguw4HXppIsfMcrYqvpW0p6gt'
    r_text = 'https://bonnieandslide.getcourse.ru/pl/api/account/payments?key={}&created_at[from]={}'.format(key,
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
    df.columns = ['id', 'user_name', 'email', 'order_number', 'created_at',
                  'type', 'status', 'amount', 'commission',
                  'received', 'code', 'name']

    for column in ['amount', 'commission', 'received']:
        df[column] = df[column].str.replace(' руб.', '')
        df[column] = df[column].str.replace(' ', '')
    df['order_number'] = df['order_number'].replace('', -1)

    # df = df[df['payment_created_at'] > date_from]
    return df
