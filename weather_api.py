import requests
import logging.config
from configs import const
from datetime import date
from zireael import *
from datetime import datetime

logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')


def get_weather_info(weather_conn):
    logger.info('start https request to {}'.format(const.url))
    req = None
    try:
        req = requests.get(url=const.url, params=weather_conn)
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


def put_weather_data_in_db(db_type, row_table):
    logger.info('start put data about weather in db type {}'.format(db_type))
    success_for_one_location = -1
    script = 'select * from {}'.format(const.sqlite_location_dir)
    resp, locations = db_select(script=script, db_type='sqlite', db_name=const.sqlite_db_name)
    if resp == -1 or locations is None:
        logger.error('failed to get location data')
        logger.error('end put data about weather in db type {}'.format(db_type))
        return -1
    for location in locations:
        logger.info('start get data for {}'.format(location[1]))
        weather_conn = const.weather_request_params
        weather_conn['latitude'] = location[2]
        weather_conn['longitude'] = location[3]
        logger.debug('weather_conn:\n{}'.format(weather_conn))
        weather_data = get_weather_info(weather_conn)
        if weather_data == -1:
            logger.error('failed to get data about {}'.format(location[1]))
        else:
            data_for_bulk_insert = []
            for i in range(len(weather_data['daily']['time'])):
                data_row = [str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), weather_data['daily']['time'][i],
                            weather_data['daily']['temperature_2m_min'][i],
                            weather_data['daily']['temperature_2m_max'][i], location[0]]
                data_for_bulk_insert.append(data_row)

            result = db_bulk_insert(table_name=row_table, data_set=data_for_bulk_insert,
                                    db_type=db_type,
                                    pattern='(?,?,?,?,?)', db_name=const.sqlite_db_name)
            if result == -1:
                logger.error('Failed to insert weather data into the database with type {}'.format(db_type))
            else:
                logger.info('end put data about weather in db type {}'.format(db_type))
                success_for_one_location = 1
    logger.info('end put data about weather in db type {}'.format(db_type))
    return success_for_one_location
