CREATE table if not exists row_weather_data_daily (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	location_id integer
);


CREATE table if not exists dir_location(
	location_id integer,
	location  text,
	latitude  text,
	longitude text,
	primary key (location_id)
);
INSERT into dir_location (location_id, location, latitude, longitude) values (2, 'Moscow', '55.7558', '37.6176');
INSERT into dir_location (location_id, location, latitude, longitude) values (3, 'London', '51.5002', '-0.1262');
commit;


drop view if EXISTS v_row_weather_data_daily_deduplicated;
CREATE VIEW v_row_weather_data_daily_deduplicated as
with ranked_row_data AS (
	SELECT  rwdd.*,
			DENSE_RANK() over (PARTITION by date(insert_date), weather_date, location_id ORDER BY insert_date DESC, rowid DESC) rnk --max(...) keep(dense_rank ...) :(
	FROM row_weather_data_daily rwdd
)
select  date(insert_date) insert_date,
		weather_date,
		temperature_2m_min,
		temperature_2m_max,
		location_id
FROM ranked_row_data
where rnk = 1
;

CREATE table if not exists row_weather_data_daily_deduplicated (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	location_id integer,
	PRIMARY KEY (insert_date, weather_date, location_id)
);


drop view if exists v_daily_weather_with_forecast_range;
CREATE view v_daily_weather_with_forecast_range as
SELECT  r.*
	   ,JULIANDAY(r.weather_date) - JULIANDAY(r.insert_date) forecast_range
FROM row_weather_data_daily_deduplicated r
;


CREATE table if not exists daily_weather_with_forecast_range (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	location_id integer,
	forecast_range integer,
	PRIMARY KEY (insert_date, weather_date, location_id)
);


drop view if exists v_weather_comparison;
create view v_weather_comparison as
with known_weather as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = -1
), forecast_for_one_day as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = 1
), forecast_for_three_day as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = 3
), forecast_for_five_day as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = 5
)
select  kw.weather_date,
		kw.location_id,
		kw.temperature_2m_min known_temperature_2m_min,
		kw.temperature_2m_max known_temperature_2m_max,
		odf.temperature_2m_min odf_temperature_2m_min,
		odf.temperature_2m_max odf_temperature_2m_max,
    	tdf.temperature_2m_min tdf_temperature_2m_min,
		tdf.temperature_2m_max tdf_temperature_2m_max,
		fdf.temperature_2m_min fdf_temperature_2m_min,
		fdf.temperature_2m_max fdf_temperature_2m_max
from known_weather kw
	join forecast_for_one_day   	 odf on odf.weather_date = kw.weather_date and
									   		odf.location_id = kw.location_id
	left join forecast_for_three_day tdf on tdf.weather_date = kw.weather_date and
									   		tdf.location_id = kw.location_id
	left join forecast_for_five_day  fdf on fdf.weather_date = kw.weather_date and
									   		fdf.location_id = kw.location_id
;


create table if not exists weather_comparison(
	weather_date date,
	location_id integer,
	known_temperature_2m_min real,
	known_temperature_2m_max real,
	odf_temperature_2m_min real,
	odf_temperature_2m_max real,
    tdf_temperature_2m_min real,
	tdf_temperature_2m_max real,
	fdf_temperature_2m_min real,
	fdf_temperature_2m_max real,
	PRIMARY KEY (weather_date, location_id)
)