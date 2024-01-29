create extension postgis;
create extension pgrouting;
create extension h3;

create schema scooters;
create schema network;
create schema analysis;


create table scooters.scooters_temp (
    car_id integer,
    location_id integer,
    vehicletype_id integer,
    isdamaged boolean,
    min_fuellevel numeric,
    max_fuellevel numeric,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    pricingtime text,
    geom_rd text
);

create table scooters.scooters as 
select
	car_id,
    location_id,
    vehicletype_id,
    isdamaged,
    min_fuellevel,
    max_fuellevel,
    start_time,
    end_time,
    substring(pricingtime, 4, 2)::int as price_cent,
    st_setsrid(st_geomfromtext(geom_rd), 28992)::geometry as geom
from
	scooters.scooters_temp;
	
select
	*
from
	scooters.scooters;
	
	
	
