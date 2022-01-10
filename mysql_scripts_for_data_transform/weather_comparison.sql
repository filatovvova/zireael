insert into weather_comparison ( weather_date,
								 location_id,
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
	    location_id,
		known_temperature_2m_min,
		known_temperature_2m_max,
		odf_temperature_2m_min,
		odf_temperature_2m_max,
		tdf_temperature_2m_min,
		tdf_temperature_2m_max,
		fdf_temperature_2m_min,
		fdf_temperature_2m_max
FROM v_weather_comparison v
where weather_date BETWEEN {0} and {1}
ON DUPLICATE KEY UPDATE
		known_temperature_2m_min = v.known_temperature_2m_min,
		known_temperature_2m_max = v.known_temperature_2m_max,
		odf_temperature_2m_min   = v.odf_temperature_2m_min,
		odf_temperature_2m_max   = v.odf_temperature_2m_max,
		tdf_temperature_2m_min   = v.tdf_temperature_2m_min,
		tdf_temperature_2m_max   = v.tdf_temperature_2m_max,
		fdf_temperature_2m_min   = v.fdf_temperature_2m_min,
		fdf_temperature_2m_max   = v.fdf_temperature_2m_max