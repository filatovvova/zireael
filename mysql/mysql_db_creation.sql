-- Directory part
-- location
CREATE table if not exists dir_location(
	location_id int,
	location  varchar(255),
	latitude  varchar(255),
	longitude varchar(255),
	primary key (location_id)
);
INSERT into dir_location (location_id, location, latitude, longitude) values (1, 'St. Petersburg', '59.9386', '30.3141');
INSERT into dir_location (location_id, location, latitude, longitude) values (2, 'Moscow', '55.7558', '37.6176');
INSERT into dir_location (location_id, location, latitude, longitude) values (3, 'London', '51.5002', '-0.1262');
commit;

-- custom calendar (To fill it in, export this directory from sqlite)
create table if not exists dir_calendar (
		day 			 	 date,
		day_of_week 	 	 int,
		name_day_of_week 	 varchar(255),
		day_of_month 	 	 int,
		is_last_day_in_month int,
		month 				 int,
		name_of_month 		 varchar(255),
		season 				 varchar(255),
		quarter 			 int,
		year 				 int,
		primary key (day)
);


-- Row part
-- daily weather forecast
CREATE table if not exists row_weather_data_daily (
	id int not null auto_increment,
	insert_date datetime,
	weather_date date,
	temperature_2m_min float(2),
	temperature_2m_max float(2),
	location_id int,
	primary key (id)
);


-- Transform pat
drop view if EXISTS v_row_weather_data_daily_deduplicated;
CREATE VIEW v_row_weather_data_daily_deduplicated as
with ranked_row_data AS (
	SELECT  rwdd.*,
			DENSE_RANK() over (PARTITION by date(insert_date), weather_date, location_id ORDER BY insert_date DESC, id DESC) rnk
	FROM row_weather_data_daily rwdd
)
select  date(insert_date) insert_date,
		date(weather_date) weather_date,
		temperature_2m_min,
		temperature_2m_max,
		location_id
FROM ranked_row_data
where rnk = 1
;

CREATE table if not exists row_weather_data_daily_deduplicated (
	id int not null auto_increment,
	insert_date date,
	weather_date date,
	temperature_2m_min float(2),
	temperature_2m_max float(2),
	location_id int,
	unique (insert_date, weather_date, location_id),
	primary key (id)
);

drop view if exists v_daily_weather_with_forecast_range;
CREATE view v_daily_weather_with_forecast_range as
SELECT  Date(r.insert_date) insert_date,
		Date(r.weather_date) weather_date,
		r.temperature_2m_min,
		r.temperature_2m_max,
		r.location_id,
	    r.weather_date - r.insert_date forecast_range
FROM row_weather_data_daily_deduplicated r
;

CREATE table if not exists daily_weather_with_forecast_range (
	id int not null auto_increment primary key,
	insert_date date,
	weather_date date,
	temperature_2m_min float(2),
	temperature_2m_max float(2),
	location_id int,
	forecast_range int,
	unique key (insert_date, weather_date, location_id)
);

drop view if exists v_known_weather;
create view v_known_weather as
with weather_yesterday as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = -1
), weather_day_before_yesterday as (
	select  weather_date,
		    temperature_2m_min,
		    temperature_2m_max,
		    location_id
	from daily_weather_with_forecast_range
	where forecast_range = -2
), emulate_full_outer as (
	SELECT wy.weather_date,
		   wy.temperature_2m_min,
		   wy.temperature_2m_max,
		   wy.location_id,
		   wby.weather_date weather_date_s,
		   wby.temperature_2m_min temperature_2m_min_s,
		   wby.temperature_2m_max temperature_2m_max_s,
		   wby.location_id location_id_s
	from weather_yesterday wy
		left join weather_day_before_yesterday wby on wy.weather_date = wby.weather_date and wy.location_id = wby.location_id
	union all
	SELECT wy.weather_date,
		   wy.temperature_2m_min,
		   wy.temperature_2m_max,
		   wy.location_id,
		   wby.weather_date weather_date_s,
		   wby.temperature_2m_min temperature_2m_min_s,
		   wby.temperature_2m_max temperature_2m_max_s,
		   wby.location_id location_id_s
	from weather_day_before_yesterday wby
		left join weather_yesterday wy on wy.weather_date = wby.weather_date and wy.location_id = wby.location_id
)
SELECT  DISTINCT
		date(ifnull(weather_date, weather_date_s)) 				weather_date,
		ifnull(temperature_2m_min, temperature_2m_min_s) 	temperature_2m_min,
		ifnull(temperature_2m_max, temperature_2m_max_s) 	temperature_2m_max,
		ifnull(location_id, location_id_s) 					location_id
from  emulate_full_outer
;

CREATE table if not exists known_weather (
	id int not null auto_increment primary key,
	weather_date date,
	temperature_2m_min float(2),
	temperature_2m_max float(2),
	location_id int,
	unique KEY (weather_date, location_id)
);

drop view if exists v_weather_comparison;
create view v_weather_comparison as
with forecast_for_one_day as (
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
select  date(kw.weather_date) weather_date,
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
	left join forecast_for_one_day   odf on odf.weather_date = kw.weather_date and
									   		odf.location_id = kw.location_id
	left join forecast_for_three_day tdf on tdf.weather_date = kw.weather_date and
									   		tdf.location_id = kw.location_id
	left join forecast_for_five_day  fdf on fdf.weather_date = kw.weather_date and
									   		fdf.location_id = kw.location_id
where odf.temperature_2m_min is not null or
	  odf.temperature_2m_max is not null or
	  tdf.temperature_2m_min is not null or
	  tdf.temperature_2m_max is not null or
	  fdf.temperature_2m_min is not null or
	  fdf.temperature_2m_max is not null
;

create table if not exists weather_comparison(
	id int not null auto_increment primary key,
	weather_date date,
	location_id int,
	known_temperature_2m_min float(2),
	known_temperature_2m_max float(2),
	odf_temperature_2m_min float(2),
	odf_temperature_2m_max float(2),
    tdf_temperature_2m_min float(2),
	tdf_temperature_2m_max float(2),
	fdf_temperature_2m_min float(2),
	fdf_temperature_2m_max float(2),
	unique key (weather_date, location_id)
);


-- Data mart part
drop view if exists v_dm_inaccuracy_daily_temperature_forecast;
create view v_dm_inaccuracy_daily_temperature_forecast as
select date(w.weather_date) weather_date,
	   dc.name_day_of_week,
	   dc.name_of_month,
	   dc.season,
	   dc.quarter,
	   dl.location,
	   dl.latitude,
	   dl.longitude,
	   w.odf_temperature_2m_min - w.known_temperature_2m_min 					od_forecast_inaccuracy_t_2m_min,
	   w.odf_temperature_2m_max - w.known_temperature_2m_max 					od_forecast_inaccuracy_t_2m_max,
	   ifnull(w.tdf_temperature_2m_min - w.known_temperature_2m_min, 'unknown') td_forecast_inaccuracy_t_2m_min,
	   ifnull(w.tdf_temperature_2m_max - w.known_temperature_2m_max, 'unknown') td_forecast_inaccuracy_t_2m_max,
	   ifnull(w.fdf_temperature_2m_min - w.known_temperature_2m_min, 'unknown') fd_forecast_inaccuracy_t_2m_min,
	   ifnull(w.fdf_temperature_2m_max - w.known_temperature_2m_max, 'unknown') fd_forecast_inaccuracy_t_2m_max
from weather_comparison w
	left join dir_calendar dc on dc.day  = w.weather_date
	left join dir_location dl on dl.location_id = w.location_id
;

create table if not exists dm_inaccuracy_daily_temperature_forecast(
	weather_date date,
	name_day_of_week varchar(255),
	name_of_month varchar(255),
	season varchar(255),
	quarter int,
	location varchar(255),
	latitude varchar(255),
	longitude varchar(255),
	od_forecast_inaccuracy_t_2m_min float(2),
	od_forecast_inaccuracy_t_2m_max float(2),
	td_forecast_inaccuracy_t_2m_min float(2),
	td_forecast_inaccuracy_t_2m_max float(2),
	fd_forecast_inaccuracy_t_2m_min float(2),
	fd_forecast_inaccuracy_t_2m_max float(2),
	PRIMARY KEY (weather_date, location)
);