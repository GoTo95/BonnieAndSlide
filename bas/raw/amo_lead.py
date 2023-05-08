import requests
import pandas as pd
import numpy as np
from datetime import datetime
from clickhouse_driver import Client
from urllib import parse
from bas.common_functions import get_token, get_last_date


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


def get_tags(row):
    array_tags = []
    for tag in row['tags']:
        array_tags.append(tag['name'])
    return ','.join(array_tags)


def get_data(date_from, token):
    _url = 'https://testbonnieandslide.amocrm.ru/api/v4/leads'
    headers = {
        "Authorization": "Bearer " + token
    }

    last_updated_at = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')

    params = {
        'with': 'loss_reason,contacts',
        "filter[updated_at][from]": int(last_updated_at.timestamp()),
        "limit": 250,
    }

    df = pd.DataFrame()
    next_url = _url
    iteration = 0
    while next_url:
        iteration += 1
        if iteration % 10 == 0:
            print(iteration)
        resp = requests.get(next_url, params=params, headers=headers)
        status = resp.status_code
        if status == 200:
            r = resp.json()
            chunk = pd.DataFrame(r['_embedded']['leads'])

            chunk['referrer'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='referrer': get_field_val(row, field_name)
            )

            chunk['getcourse_deal_id'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='GetCourseId': get_field_val(row, field_name)
            )

            chunk['getcourse_deal_number'] = chunk['custom_fields_values'].apply(
                lambda row, field_name='GetCourse deal_number': get_field_val(row, field_name)
            )

            chunk['assumed_payment_date'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='Дата оплаты': get_field_val(row, field_name)
            )

            chunk['gclientid'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='gclientid': get_field_val(row, field_name)
            )

            chunk['url_values'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='__utm_custom_cookies': get_url_values(row, field_name)
            )

            chunk['web_id'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='web_id': get_field_val(row, field_name)
            )

            chunk['course'] = chunk[chunk['is_deleted'] == False]['custom_fields_values'].apply(
                lambda row, field_name='Курс': get_field_val(row, field_name)
            )

            chunk['tags'] = chunk[chunk['is_deleted'] == False]['_embedded'].apply(
                lambda row: get_tags(row)
            )

            utm_chunk = chunk['url_values'].apply(pd.Series)
            for utm in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content']:
                chunk[utm] = ''
                if utm in utm_chunk.columns:
                    chunk[utm] = utm_chunk[utm]

            chunk['created_at'] = chunk['created_at'].apply(datetime.fromtimestamp)
            chunk['updated_at'] = chunk['updated_at'].apply(datetime.fromtimestamp)

            chunk['closed_at'] = chunk[chunk['closed_at'].notna()]['closed_at'].apply(datetime.fromtimestamp)
            chunk['assumed_payment_date'] = \
                chunk[chunk['assumed_payment_date'].notna()]['assumed_payment_date'].apply(datetime.fromtimestamp)

            chunk['assumed_payment_date'] = chunk[chunk['assumed_payment_date'].notna()][
                                                'assumed_payment_date'] + pd.DateOffset(hours=12)
            chunk['assumed_payment_date'] = pd.to_datetime(
                chunk[chunk['assumed_payment_date'].notna()]['assumed_payment_date']).dt.round('D')

            chunk['closest_task_at'] = chunk[chunk['closest_task_at'].notna()]['closest_task_at'].apply(
                datetime.fromtimestamp
            )
            chunk['lost_reason'] = chunk['_embedded'].apply(loss_reason)
            chunk['contact_id'] = chunk['_embedded']. \
                apply(lambda x: x['contacts'][0]['id'] if len(x['contacts']) > 0 else 0)
            chunk = chunk.replace({np.nan: None})

            df = df.append(chunk)
            next_url = r['_links'].get('next', {}).get('href')
        else:
            print(f'http answer code is {status}')
            break
    if len(df) > 0:
        needed_columns = ['id', 'price', 'created_at', 'gclientid', 'updated_at', 'name', 'pipeline_id', 'status_id',
                          'web_id',
                          'getcourse_deal_id', 'getcourse_deal_number', 'lost_reason', 'contact_id', 'referrer',
                          'responsible_user_id',
                          'assumed_payment_date', 'course', 'tags', 'utm_source', 'utm_medium', 'utm_campaign',
                          'utm_content']

        df = df[needed_columns]

        df['id'] = df['id'].apply(to_int)
        df['getcourse_deal_id'] = df['getcourse_deal_id'].apply(to_int)
        df['getcourse_deal_number'] = df['getcourse_deal_number'].apply(to_int)
        df['pipeline_id'] = df['pipeline_id'].apply(to_int)
        df['status_id'] = df['status_id'].apply(to_int)
        df['contact_id'] = df['contact_id'].apply(to_int)
        df['price'] = df['price'].apply(to_int)

        df[['getcourse_deal_id', 'getcourse_deal_number', 'pipeline_id', 'status_id']] = \
            df[['getcourse_deal_id', 'getcourse_deal_number', 'pipeline_id', 'status_id']].replace('', -1).fillna(
                -1).astype(int)

    return df