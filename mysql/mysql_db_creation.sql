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