import requests
import logging.config
from configs import const
from datetime import date
from zireael import *
from datetime import datetime

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def get_weather_info():
    logger.info('start https request to {}'.format(const.url))
    req = None
    try:
        req = requests.get(url=const.url, params=const.weather_request_params)
        data = req.json()
    except requests.exceptions.RequestException as ex_message:
        logger.error('Ups, api request execute with exception: {}'.format(ex_message))
        return -1
    else:
        if 'error' in data:
            logger.error('Ups, api request execute with exception: {}'.format(data['reason']))
            return -1
        else:
            logger.info('end https request')
    finally:
        if req:
            req.close()
    return data


def put_weather_data_in_db():
    weather_data = get_weather_info()
    data_for_bulk_insert = []
    today = date.today()
    for i in range(len(weather_data['daily']['time'])):
        data_row = [str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), weather_data['daily']['time'][i],
                    weather_data['daily']['temperature_2m_min'][i],
                    weather_data['daily']['temperature_2m_max'][i], const.weather_request_params['latitude'],
                    const.weather_request_params['longitude']]
        data_for_bulk_insert.append(data_row)
    print(data_for_bulk_insert)

    result = db_bulk_insert(table_name=const.sqlite_row_weather_data, data_set=data_for_bulk_insert, db_type='sqlite',
                            pattern='(?,?,?,?,?,?)', db_name=const.sqlite_db_name)
    return result
