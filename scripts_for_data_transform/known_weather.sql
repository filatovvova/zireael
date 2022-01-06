Replace into known_weather (weather_date, temperature_2m_min, temperature_2m_max, location_id)
select weather_date, temperature_2m_min, temperature_2m_max, location_id
FROM v_known_weather
where weather_date BETWEEN {0} AND {1}
