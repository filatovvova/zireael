INSERT into daily_weather_with_forecast_range (insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id, forecast_range)
select insert_date, weather_date, temperature_2m_min, temperature_2m_max, location_id, forecast_range
FROM v_daily_weather_with_forecast_range v
where insert_date BETWEEN {0} and {1}
ON DUPLICATE KEY UPDATE
		temperature_2m_min = v.temperature_2m_min,
		temperature_2m_max = v.temperature_2m_max