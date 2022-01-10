Replace into dm_inaccuracy_daily_temperature_forecast (
	weather_date,
	name_day_of_week,
	name_of_month,
	season,
	quarter,
	location,
	latitude,
	longitude,
	od_forecast_inaccuracy_t_2m_min,
	od_forecast_inaccuracy_t_2m_max,
	td_forecast_inaccuracy_t_2m_min,
	td_forecast_inaccuracy_t_2m_max,
	fd_forecast_inaccuracy_t_2m_min,
	fd_forecast_inaccuracy_t_2m_max
)
select  weather_date,
		name_day_of_week,
		name_of_month,
		season,
		quarter,
		location,
		latitude,
		longitude,
		od_forecast_inaccuracy_t_2m_min,
		od_forecast_inaccuracy_t_2m_max,
		td_forecast_inaccuracy_t_2m_min,
		td_forecast_inaccuracy_t_2m_max,
		fd_forecast_inaccuracy_t_2m_min,
		fd_forecast_inaccuracy_t_2m_max
FROM v_dm_inaccuracy_daily_temperature_forecast
where weather_date BETWEEN {0} AND {1}