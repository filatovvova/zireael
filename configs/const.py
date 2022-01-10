# sqlite
sqlite_conf_path = './configs/sqlite_db.yaml'
sqlite_row_weather_data = 'row_weather_data_daily'
sqlite_location_dir = 'dir_location'
sqlite_db_name = 'test'

# mysql
mysql_conf_path = './configs/mysql_db.yaml'
mysql_row_weather_data = 'row_weather_data_daily'
mysql_file = './files/file.txt'

# weather api
url = "https://api.open-meteo.com/v1/forecast"
weather_request_params = {
    'daily': {'temperature_2m_max', 'temperature_2m_min'},
    'timezone': 'Europe/Moscow',
    'current_weather': False,
    'past_days': 2
}

# data transform
data_transform_path = './configs/data_transform.yaml'
depth_of_calculations_in_days = 30
