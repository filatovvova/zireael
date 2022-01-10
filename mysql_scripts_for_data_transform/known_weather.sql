INSERT into known_weather (weather_date, temperature_2m_min, temperature_2m_max, location_id)
select weather_date, temperature_2m_min, temperature_2m_max, location_id
FROM v_known_weather v
where weather_date BETWEEN {0} and {1}
ON DUPLICATE KEY UPDATE
		temperature_2m_min = v.temperature_2m_min,
		temperature_2m_max = v.temperature_2m_max