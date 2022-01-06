Replace into weather_comparison (weather_date,
								 latitude,
								 longitude,
								 known_temperature_2m_min,
								 known_temperature_2m_max,
								 odf_temperature_2m_min,
								 odf_temperature_2m_max,
						    	 tdf_temperature_2m_min,
								 tdf_temperature_2m_max,
								 fdf_temperature_2m_min,
								 fdf_temperature_2m_max
								)
select  weather_date,
	    latitude,
		longitude,
		known_temperature_2m_min,
		known_temperature_2m_max,
		odf_temperature_2m_min,
		odf_temperature_2m_max,
		tdf_temperature_2m_min,
		tdf_temperature_2m_max,
		fdf_temperature_2m_min,
		fdf_temperature_2m_max
FROM v_weather_comparison
where weather_date BETWEEN {0} AND {1}