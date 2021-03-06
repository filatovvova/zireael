import requests
import logging.config
from configs import const
from zireael import db_select, db_bulk_insert
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


def put_weather_data_in_db(db_types, row_table):
    logger.info('start put data about weather in db type {}'.format(db_types))
    success_for_one_location = -1
    script = 'select * from {}'.format(const.sqlite_location_dir)
    resp, locations = db_select(script=script, db_type='sqlite', db_name=const.sqlite_db_name)
    if resp == -1 or locations is None:
        logger.error('failed to get location data')
        logger.error('end put data about weather in db type {}'.format(db_types))
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
            for db_type in db_types:
                if db_type == 'sqlite':
                    values_pattern = '(?,?,?,?,?)'
                    fields_name_pattern = 'none'
                if db_type == 'mysql':
                    values_pattern = '%s, %s, %s, %s, %s'
                    fields_name_pattern = '	insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id'
                result = db_bulk_insert(table_name=row_table,
                                        data_set=data_for_bulk_insert,
                                        db_type=db_type,
                                        values_pattern=values_pattern,
                                        fields_name_pattern=fields_name_pattern,
                                        db_name=const.sqlite_db_name)
                if result == -1:
                    logger.error('failed to insert weather data for location {} in db type '.format(location[1],
                                                                                                    db_type))
                else:
                    logger.info('end insert data for {} in db type {}'.format(location[1], db_type))
                    success_for_one_location = 1
            logger.info('end get data for {}'.format(location[1], db_type))
    logger.info('end put data about weather in db type {}'.format(db_types))
    return success_for_one_location
