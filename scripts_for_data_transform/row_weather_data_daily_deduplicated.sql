Replace into row_weather_data_daily_deduplicated (insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id)
select insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id
FROM v_row_weather_data_daily_deduplicated
where insert_date BETWEEN {0} AND {1}
