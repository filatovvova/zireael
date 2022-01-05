Replace into row_weather_data_daily_deduplicated (insert_date, weather_date, temperature_2m_min, temperature_2m_max, latitude, longitude)
select DATE(insert_date), weather_date, temperature_2m_min, temperature_2m_max, latitude, longitude
FROM row_weather_data_daily
where DATE(insert_date) BETWEEN {0} AND {1}
;