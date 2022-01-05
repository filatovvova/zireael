CREATE table row_weather_data_daily (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	latitude text,
	longitude text
);

CREATE table dir_location(
	location  text,
	latitude  text,
	longitude text
);
INSERT into dir_location (location, latitude, longitude) values ('Moscow', '55.7558', '37.6176');
commit;

CREATE table row_weather_data_daily_deduplicated (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	latitude text,
	longitude text,
	PRIMARY KEY (insert_date, weather_date)
);
