import yaml
import logging.config
from zireael import *
from configs import const
import requests
from weather_api import *
from datetime import datetime


logging.config.fileConfig('./configs/logger.config')
logger = logging.getLogger('zireaelLogger')

# with open(const.sqlite_conf_path, 'r') as file:
#     db_conf = yaml.safe_load(file)
#
# conn = sqlite3.connect(db_conf['db']['test']['conn_string'])
#
#
# fields = """fill text\n"""
# data_set = [['first'], ['second'], ['third']]
# table_name = 'table_fill_2'
# pattern = '(?)'
# db_type = 'sqlite'
# db_name = 'test'
# where_block = "1 = 1"
# select = '''
# SELECT rwd.ROWID, rwd.*
# FROM row_weather_data rwd
# ;
# '''
#
# # result = db_create_table(table_name='table_fill_2', fields=fields, db_type='sqlite', db_name='test', if_not_exists=True)
# # print(result)
#
# result = db_bulk_insert(table_name=table_name, data_set=data_set, pattern=pattern, db_type=db_type, conn=conn)
# print(result)
#
# result = db_delete(table_name=table_name, where_block=where_block, db_type=db_type, conn=conn)
# print(result)

# resp, result = db_select(select, db_type=db_type, db_name=db_name, conn=conn)
# print(resp)
# if result:
#     for row in result:
#         print(row)

# importing the requests library
import requests

# https://api.open-meteo.com/v1/forecast?latitude=55.7558&longitude=37.6176&daily=temperature_2m_max,temperature_2m_min&timezone=Europe%2FMoscow&past_days=2

# # api-endpoint
# URL = "https://api.open-meteo.com/v1/forecast"
#
# # location given here
# location = "delhi technological university"
#
# # defining a params dict for the parameters to be sent to the API
# PARAMS = {
#     'latitude': 55.7558,
#     'longitude': 37.6176,
#     'daily': {'temperature_2m_max', 'temperature_2m_min'},
#     'timezone': 'Europe/Moscow',
#     'past_days': 2
# }

# sending get request and saving the response as response object
# r = requests.get(url='dsdsd', params=PARAMS)
#
# # extracting data in json format
# data = r.json()

# extracting latitude, longitude and formatted address
# of the first matching location
# latitude = data['results'][0]['geometry']['location']['lat']
# longitude = data['results'][0]['geometry']['location']['lng']
# formatted_address = data['results'][0]['formatted_address']

# printing the output
# print("Latitude:%s\nLongitude:%s\nFormatted Address:%s"
#       % (latitude, longitude, formatted_address))
#
result = put_weather_data_in_db()
print(result)


