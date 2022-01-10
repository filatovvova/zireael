-- Directory part
-- custom calendar
create table if not exists dir_calendar (
		day 			 	 date,
		day_of_week 	 	 integer,
		name_day_of_week 	 text,
		day_of_month 	 	 integer,
		is_last_day_in_month integer,
		month 				 integer,
		name_of_month 		 text,
		season 				 text,
		quarter 			 integer,
		year 				 integer,
		primary key (day)
);

insert into dir_calendar (
		day 			 	 ,
		day_of_week 	 	 ,
		name_day_of_week 	 ,
		day_of_month 	 	 ,
		is_last_day_in_month ,
		month 				 ,
		name_of_month 		 ,
		season 				 ,
		quarter 			 ,
		year
)
with recursive dates(x) as (
		VALUES('1970-01-01')
		UNION all
		select Date(x, '+1 day')
		from dates
		where x < '2037-12-31'
)
select x 										day,
	   cast(strftime('%w',x) as integer) 		day_of_week,
	   CASE cast(strftime('%w',x) as integer)
		   when 1 then 'Monday'
		   when 2 then 'Tuesday'
		   when 3 then 'Wednesday'
		   when 4 then 'Thursday'
		   when 5 then 'Friday'
		   when 6 then 'Saturday'
		   when 0 then 'Sunday'
		   else 'What?'
	   end 										name_day_of_week,
	   cast(strftime('%d',x) as integer) 		day_of_month,
	   CASE
	   	   when Date(x,'start of month', '+1 month', '-1 day') == x then 1
	   	   else 0
	   end										is_last_day_in_month,
	   cast(strftime('%m',x) as integer)		month,
	   CASE cast(strftime('%m',x) as integer)
		   when 1 then 'January'
		   when 2 then 'February'
		   when 3 then 'March'
		   when 4 then 'April'
		   when 5 then 'May'
		   when 6 then 'June'
		   when 7 then 'July'
		   when 8 then 'August'
		   when 9 then 'September'
		   when 10 then 'October'
		   when 11 then 'November'
		   when 12 then 'December'
		   else 'What?'
	   end 										name_of_month,
	   CASE
		   when cast(strftime('%m',x) as integer) in (1,2,12)  then 'winter'
		   when cast(strftime('%m',x) as integer) in (3,4,5)   then 'spring'
		   when cast(strftime('%m',x) as integer) in (6,7,8)   then 'summer'
		   when cast(strftime('%m',x) as integer) in (9,10,11) then 'autumn'
		   else 'What?'
	   end 										season,
	   CASE
		   when cast(strftime('%m',x) as integer) in (1,2,3)    then 1
		   when cast(strftime('%m',x) as integer) in (4,5,6)    then 2
		   when cast(strftime('%m',x) as integer) in (7,8,9)    then 3
		   when cast(strftime('%m',x) as integer) in (10,11,12) then 4
		   else 'What?'
	   end 										quarter,
	   cast(strftime('%Y',x) as integer)		year
FROM dates;
commit;

-- location
CREATE table if not exists dir_location(
	location_id integer,
	location  text,
	latitude  text,
	longitude text,
	primary key (location_id)
);
INSERT into dir_location (location_id, location, latitude, longitude) values (1, 'St. Petersburg', '59.9386', '30.3141');
INSERT into dir_location (location_id, location, latitude, longitude) values (2, 'Moscow', '55.7558', '37.6176');
INSERT into dir_location (location_id, location, latitude, longitude) values (3, 'London', '51.5002', '-0.1262');
commit;


-- Row part
-- daily weather forecast
CREATE table if not exists row_weather_data_daily (
	insert_date date,
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	location_id integer
);


-- Transform pat
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
		ifnull(weather_date, weather_date_s) 				weather_date,
		ifnull(temperature_2m_min, temperature_2m_min_s) 	temperature_2m_min,
		ifnull(temperature_2m_max, temperature_2m_max_s) 	temperature_2m_max,
		ifnull(location_id, location_id_s) 					location_id
from  emulate_full_outer
;

CREATE table if not exists known_weather (
	weather_date date,
	temperature_2m_min real,
	temperature_2m_max real,
	location_id integer,
	PRIMARY KEY (weather_date, location_id)
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

-- Data mart part
drop view if exists v_dm_inaccuracy_daily_temperature_forecast;
create view v_dm_inaccuracy_daily_temperature_forecast as
select w.weather_date,
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
	left join dir_calendar dc on dc."day"  = w.weather_date
	left join dir_location dl on dl.location_id = w.location_id
;

create table if not exists dm_inaccuracy_daily_temperature_forecast(
	weather_date date,
	name_day_of_week text,
	name_of_month text,
	season text,
	quarter integer,
	location text,
	latitude text,
	longitude text,
	od_forecast_inaccuracy_t_2m_min real,
	od_forecast_inaccuracy_t_2m_max real,
	td_forecast_inaccuracy_t_2m_min real,
	td_forecast_inaccuracy_t_2m_max real,
	fd_forecast_inaccuracy_t_2m_min real,
	fd_forecast_inaccuracy_t_2m_max real,
	PRIMARY KEY (weather_date, location)
);