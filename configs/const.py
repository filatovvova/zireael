# sqlite
sqlite_conf_path = './configs/sqlite_db.yaml'
sqlite_row_weather_data = 'row_weather_data_daily'
sqlite_db_name = 'test'

# weather api
url = "https://api.open-meteo.com/v1/forecast"
weather_request_params = {
    'latitude': 55.7558,
    'longitude': 37.6176,
    'daily': {'temperature_2m_max', 'temperature_2m_min'},
    'timezone': 'Europe/Moscow',
    'current_weather': False,
    'past_days': 2
}

# data transform
data_transform_path = './configs/data_transform.yaml'
depth_of_calculations_in_days = 30
