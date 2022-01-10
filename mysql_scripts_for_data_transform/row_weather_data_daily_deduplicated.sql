INSERT into row_weather_data_daily_deduplicated (insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id)
select insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id
FROM v_row_weather_data_daily_deduplicated v
where insert_date BETWEEN {0} and {1}
ON DUPLICATE KEY UPDATE
		temperature_2m_min = v.temperature_2m_min,
		temperature_2m_max = v.temperature_2m_max